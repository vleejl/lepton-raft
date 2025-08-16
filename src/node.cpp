#include "node.h"

#include <array>
#include <cassert>
#include <memory>
#include <system_error>
#include <utility>

#include "asio/awaitable.hpp"
#include "asio/error_code.hpp"
#include "channel.h"
#include "channel_endpoint.h"
#include "describe.h"
#include "expected.h"
#include "leaf_expected.h"
#include "magic_enum.hpp"
#include "raft.pb.h"
#include "raft_error.h"
#include "raw_node.h"
#include "ready.h"
#include "signal_channel_endpoint.h"
#include "spdlog/spdlog.h"
#include "state.h"
#include "tl/expected.hpp"

namespace lepton {

asio::awaitable<void> node::stop() {
  SPDLOG_INFO("ready to send stop signal");
  // Not already stopped, so trigger it
  auto ec = co_await async_select_done([&](auto token) { return stop_chan_.async_send(asio::error_code{}, token); },
                                       done_chan_);
  if (!ec.has_value()) {
    // Node has already been stopped - no need to do anything
    co_return;
  }
  SPDLOG_INFO("Block until the stop has been acknowledged by run()");
  // Block until the stop has been acknowledged by run()
  co_await done_chan_.async_receive();
  co_return;
}

asio::awaitable<void> node::run() {
  signal_channel token_chan(executor_);

  bool is_active_prop_chan = false;
  signal_channel active_prop_chan(executor_);

  signal_channel active_ready_chan(executor_);
  signal_channel_endpoint_handle advance_chan = nullptr;
  signal_channel active_advance_chan(executor_);

  std::array<signal_channel_handle, 4> signal_chan_group = {
      &token_chan,
      &active_prop_chan,
      &active_ready_chan,
      &active_advance_chan,
  };

  std::optional<ready_handle> rd;
  auto& r = raw_node_.raft_;

  auto lead = NONE;

  co_spawn(executor_, listen_propose(token_chan, active_prop_chan, is_active_prop_chan), asio::detached);
  co_spawn(executor_, listen_receive(token_chan), asio::detached);
  co_spawn(executor_, listen_conf_change(token_chan, is_active_prop_chan), asio::detached);
  co_spawn(executor_, listen_tick(token_chan), asio::detached);
  co_spawn(executor_, listen_ready(token_chan, active_ready_chan, advance_chan, active_advance_chan), asio::detached);
  co_spawn(executor_, listen_advance(rd, token_chan, active_advance_chan, advance_chan), asio::detached);
  co_spawn(executor_, listen_status(token_chan), asio::detached);
  co_spawn(executor_, listen_stop(signal_chan_group), asio::detached);

  auto token = stop_source_.get_token();
  while (!stop_source_.stop_requested() && done_chan_.is_open()) {
    auto has_ready = false;
    SPDLOG_INFO("run main loop ......");
    if ((advance_chan == nullptr) && raw_node_.has_ready()) {
      SPDLOG_INFO("run main loop has ready......");
      // Populate a Ready. Note that this Ready is not guaranteed to
      // actually be handled. We will arm readyc, but there's no guarantee
      // that we will actually send on it. It's possible that we will
      // service another channel instead, loop around, and then populate
      // the Ready again. We could instead force the previous Ready to be
      // handled first, but it's generally good to emit larger Readys plus
      // it simplifies testing (by emitting less frequently and more
      // predictably).
      // rd = std::make_shared<ready>(raw_node_.ready_without_accept());
      has_ready = true;
    }
    SPDLOG_INFO("run main loop, continue main loop logic ......");
    if (lead != r.lead()) {
      if (r.has_leader()) {
        if (lead == NONE) {
          SPDLOG_INFO("raft.node: {} elected leader {} at term {}", r.id(), r.lead(), r.term());
        } else {
          SPDLOG_INFO("raft.node: {} changed leader from {} to {} at term {}", r.id(), lead, r.lead(), r.term());
        }
        is_active_prop_chan = true;
      } else {
        SPDLOG_INFO("raft.node: {} lost leader {} at term {}", r.id(), lead, r.term());
        is_active_prop_chan = false;
      }
      lead = r.lead();
    }

    if (is_active_prop_chan) {
      SPDLOG_INFO("ready to send active prop channel");
      // 不能阻塞主循环
      active_prop_chan.try_send(asio::error_code{});
      SPDLOG_INFO("send active prop channel successful");
    }
    SPDLOG_INFO("has_ready: {}, waiting token async_receive", has_ready);
    if (has_ready) {
      active_ready_chan.try_send(asio::error_code{});
    }
    co_await token_chan.async_receive();
  }
  SPDLOG_INFO("receive stop signal and exit run loop");
  co_return;
}

void node::start_run() { co_spawn(executor_, run(), asio::detached); }

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
void node::tick() {
  if (!done_chan_.is_open()) {
    return;
  }

  if (!tick_chan_.raw_channel().try_send(asio::error_code{})) {
    SPDLOG_WARN("{} A tick missed to fire. Node blocks too long!", raw_node_.raft_.id());
  }
}

asio::awaitable<expected<void>> node::campaign() {
  raftpb::message msg;
  msg.set_type(::raftpb::message_type::MSG_HUP);
  auto result = co_await step_impl(std::move(msg));
  co_return result;
}

asio::awaitable<expected<void>> node::propose(std::string&& data) {
  auto result = co_await propose(executor_, std::move(data));
  co_return result;
}

asio::awaitable<expected<void>> node::propose(asio::any_io_executor executor, std::string&& data) {
  raftpb::message msg;
  msg.set_type(raftpb::message_type::MSG_PROP);
  auto entry = msg.add_entries();
  entry->set_data(std::move(data));
  auto result = co_await step_with_wait_impl(executor, std::move(msg));
  co_return result;
}

asio::awaitable<expected<void>> node::step(raftpb::message&& msg) {
  SPDLOG_DEBUG(msg.DebugString());
  if (pb::is_local_msg(msg.type()) && !pb::is_local_msg_target(msg.from())) {
    // Local messages are not handled by step, but by the node's
    // propose method.
    co_return expected<void>{};
  }
  auto result = co_await step_impl(std::move(msg));
  co_return result;
}

asio::awaitable<expected<void>> node::propose_conf_change(const pb::conf_change_var& cc) {
  auto msg_result = leaf_to_expected([&]() -> leaf::result<raftpb::message> {
    BOOST_LEAF_AUTO(m, pb::conf_change_to_message(cc));
    return m;
  });
  if (!msg_result) {
    co_return tl::unexpected{msg_result.error()};
  }
  auto result = co_await step(std::move(*msg_result));
  co_return result;
}

asio::awaitable<expected<ready_handle>> node::async_receive_ready(asio::any_io_executor executor) {
  // ready_channel receive_ready_chan(executor);
  auto receive_ready_chan = std::make_shared<ready_channel>(executor);
  SPDLOG_INFO("prepare to request ready");
  auto result = co_await ready_request_chan_.async_send(std::weak_ptr(receive_ready_chan));
  if (!result) {
    SPDLOG_ERROR("send callback channel failed, error:{}", result.error().message());
    co_return tl::unexpected{result.error()};
  }
  SPDLOG_INFO("request ready callback channel successful and prepare wait callback channel active");
  auto ready = co_await receive_ready_chan->async_receive();
  if (!ready) {
    SPDLOG_ERROR("receive error when try to receive ready, error:{}", ready.error().message());
  } else {
    SPDLOG_INFO("callback channel actived and receive ready content:\n{}", describe_ready(*ready.value()));
  }
  co_return ready;
}

asio::awaitable<void> node::advance() {
  auto ec = co_await advance_chan_.async_send();
  if (!ec.has_value()) {
    SPDLOG_ERROR("Failed to recv non-proposal message: {}", ec.error().message());
  }
  co_return;
}

asio::awaitable<expected<raftpb::conf_state>> node::apply_conf_change(raftpb::conf_change_v2&& cc) {
  if (!stop_chan_.is_open()) {
    co_return raftpb::conf_state{};
  }
  asio::error_code ec;
  co_await conf_chan_.raw_channel().async_send(asio::error_code{}, cc, asio::redirect_error(asio::use_awaitable, ec));
  if (ec != asio::error::operation_aborted && ec) {
    SPDLOG_ERROR(ec.message());
  }
  auto cs = co_await conf_state_chan_.async_receive();
  if (!ec) {
    SPDLOG_ERROR(ec.message());
    co_return tl::unexpected{ec};
  }
  co_return cs;
}

asio::awaitable<expected<lepton::status>> node::status() {
  if (!stop_chan_.is_open()) {
    co_return tl::unexpected{raft_error::STOPPED};
  }
  auto status_chan = std::make_shared<channel_endpoint<lepton::status>>(executor_);
  if (auto result = co_await status_chan_.async_send(std::weak_ptr<channel_endpoint<lepton::status>>(status_chan));
      !result) {
    SPDLOG_ERROR(result.error().message());
    co_return tl::unexpected(result.error());
  }

  auto result = co_await status_chan->async_receive();
  co_return result;
}

asio::awaitable<void> node::report_unreachable(std::uint64_t id) {
  if (!stop_chan_.is_open()) {
    co_return;
  }
  raftpb::message msg;
  msg.set_from(id);
  msg.set_type(raftpb::message_type::MSG_UNREACHABLE);
  auto ec = co_await recv_chan_.async_send(std::move(msg));
  if (!ec) {
    SPDLOG_ERROR(ec.error().message());
  }
  co_return;
}

asio::awaitable<void> node::report_snapshot(std::uint64_t id, snapshot_status status) {
  if (!stop_chan_.is_open()) {
    co_return;
  }
  auto rej = status == snapshot_status::SNAPSHOT_FAILURE;
  raftpb::message msg;
  msg.set_type(raftpb::message_type::MSG_SNAP_STATUS);
  msg.set_from(id);
  msg.set_reject(rej);
  auto ec = co_await recv_chan_.async_send(std::move(msg));
  if (!ec) {
    SPDLOG_ERROR(ec.error().message());
  }
  co_return;
}

asio::awaitable<void> node::transfer_leadership(std::uint64_t lead, std::uint64_t transferee) {
  if (!stop_chan_.is_open()) {
    co_return;
  }
  raftpb::message msg;
  msg.set_type(raftpb::message_type::MSG_TRANSFER_LEADER);
  // manually set 'from' and 'to', so that leader can voluntarily transfers its leadership
  msg.set_from(transferee);
  msg.set_to(lead);
  auto ec = co_await recv_chan_.async_send(std::move(msg));
  if (!ec) {
    SPDLOG_ERROR(ec.error().message());
  }
  co_return;
}

asio::awaitable<expected<void>> node::forget_leader() {
  raftpb::message msg;
  msg.set_type(raftpb::message_type::MSG_FORGET_LEADER);
  auto result = co_await step_impl(std::move(msg));
  co_return result;
}

asio::awaitable<expected<void>> node::read_index(std::string&& data) {
  raftpb::message msg;
  msg.set_type(raftpb::message_type::MSG_PROP);
  auto entry = msg.add_entries();
  entry->set_data(std::move(data));
  auto result = co_await step_impl(std::move(msg));
  co_return result;
}

asio::awaitable<void> node::listen_ready(signal_channel& token_chan, signal_channel& active_ready_chan,
                                         signal_channel_endpoint_handle& advance_chan,
                                         signal_channel& active_advance_chan) {
  while (done_chan_.is_open() && token_chan.is_open() && active_ready_chan.is_open() && ready_request_chan_.is_open()) {
    SPDLOG_INFO("waiting active_ready_chan signal......");
    co_await active_ready_chan.async_receive();
    SPDLOG_INFO("receive active_ready_chan signal");
    // 走到这里表面一定有 ready
    auto result = co_await ready_request_chan_.async_receive();
    if (!result) {
      SPDLOG_ERROR("Failed to receive ready_request_chan, error: {}", result.error().message());
      co_return;
    }
    if (auto chan = result->lock()) {
      auto rd = std::make_shared<ready>(raw_node_.ready_without_accept());
      SPDLOG_INFO("ready to send ready by ready_channel, {}", describe_ready(*rd.get()));
      // TODO(vleejl) 增加超时逻辑兜底
      co_await chan->async_send(rd);
      SPDLOG_INFO("send ready by ready_channel successful, {}", describe_ready(*rd.get()));
      raw_node_.accept_ready(*rd.get());
      if (!raw_node_.async_storage_writes()) {
        advance_chan = &advance_chan_;
        co_await active_advance_chan.async_send(asio::error_code{});
      } else {
        rd.reset();
      }
      SPDLOG_INFO("finish send ready by ready_channel successfully");
    } else {
      SPDLOG_WARN("callback channel has been released, wait next loop");
    }
    co_await token_chan.async_send(asio::error_code{});
  }
  co_return;
}

asio::awaitable<void> node::listen_advance(std::optional<ready_handle>& rd, signal_channel& token_chan,
                                           signal_channel& active_advance_chan,
                                           signal_channel_endpoint_handle& advance_chan) {
  while (done_chan_.is_open() && token_chan.is_open() && advance_chan_.is_open()) {
    if (advance_chan == nullptr) {
      SPDLOG_INFO("waiting active_advance_chan signal......");
      co_await active_advance_chan.async_receive();
      SPDLOG_INFO("receive active_advance_chan signal");
    }
    assert(advance_chan != nullptr);
    SPDLOG_INFO("ready to async_receive advance by advance_chan_");
    auto ec = co_await advance_chan_.async_receive();
    if (!ec) {
      SPDLOG_ERROR("Failed to receive advance, error: {}", ec.error().message());
      co_return;
    }
    SPDLOG_INFO("async_receive advance by advance_chan_ successfully");
    raw_node_.advance();
    rd.reset();
    advance_chan = nullptr;
    co_await token_chan.async_send(asio::error_code{});
  }
  co_return;
}

asio::awaitable<void> node::listen_propose(signal_channel& token_chan, signal_channel& active_prop_chan,
                                           bool& is_active_prop_chan) {
  while (done_chan_.is_open() && token_chan.is_open() && active_prop_chan.is_open() && prop_chan_.is_open()) {
    if (!is_active_prop_chan) {
      SPDLOG_INFO("waiting active_prop_chan signal......");
      co_await active_prop_chan.async_receive();
      SPDLOG_INFO("receive active_prop_chan signal");
    }
    SPDLOG_INFO("ready to listen prop_chan");
    auto msg_result = co_await prop_chan_.async_receive();
    if (!msg_result) {
      SPDLOG_ERROR(msg_result.error().message());
      break;
    }

    auto& msg = msg_result->msg;
    SPDLOG_INFO("receive msg by prop_chan and ready to process, {}", msg.DebugString());
    msg.set_from(raw_node_.raft_.id());
    auto step_result = leaf_to_expected_void([&]() -> leaf::result<void> {
      BOOST_LEAF_CHECK(raw_node_.raft_.step(std::move(msg)));
      return {};
    });
    if (msg_result->err_chan.has_value()) {
      std::error_code ec;
      if (!step_result.has_value()) {
        ec = step_result.error();
      }
      if (auto chan = msg_result->err_chan->lock()) {
        co_await chan->async_send(ec);
      } else {
        SPDLOG_WARN("try lock err_chan callback channel failed");
      }
    }
    SPDLOG_INFO("prop_chan_ send token_chan and wait next loop", msg.DebugString());
    co_await token_chan.async_send(asio::error_code{});
  }
  co_return;
}

asio::awaitable<void> node::listen_receive(signal_channel& token_chan) {
  while (done_chan_.is_open() && token_chan.is_open() && recv_chan_.is_open()) {
    SPDLOG_INFO("begin recv_chan_ async_receive....");
    auto result = co_await recv_chan_.async_receive();
    if (!result) {
      SPDLOG_ERROR(result.error().message());
      break;
    }
    auto& msg = *result;
    SPDLOG_INFO("recv_chan_ async_receive receive msg. {}", msg.DebugString());
    if (pb::is_response_msg(msg.type()) && !pb::is_local_msg_target(msg.from()) &&
        !raw_node_.raft_.has_trk_progress(msg.from())) {
      SPDLOG_INFO("message type: {}, msg from: {}. Filter out response message from unknown From",
                  magic_enum::enum_name(msg.type()), msg.from());
      // Filter out response message from unknown From.
    } else {
      auto _ = raw_node_.raft_.step(std::move(msg));
    }
    SPDLOG_INFO("recv_chan_ send token_chan and wait next loop");
    co_await token_chan.async_send(asio::error_code{});
    SPDLOG_INFO("send token signal successful");
  }
  co_return;
}

asio::awaitable<void> node::listen_conf_change(signal_channel& token_chan, bool& is_active_prop_chan) {
  auto& r = raw_node_.raft_;
  while (done_chan_.is_open() && token_chan.is_open() && conf_chan_.is_open()) {
    auto result = co_await conf_chan_.async_receive();
    if (!result) {
      SPDLOG_ERROR(result.error().message());
      break;
    }
    auto& cc = *result;

    auto ok_before = r.has_trk_progress(r.id());
    auto cs = r.apply_conf_change(std::move(cc));
    // If the node was removed, block incoming proposals. Note that we
    // only do this if the node was in the config before. Nodes may be
    // a member of the group without knowing this (when they're catching
    // up on the log and don't have the latest config) and we don't want
    // to block the proposal channel in that case.
    //
    // NB: propc is reset when the leader changes, which, if we learn
    // about it, sort of implies that we got readded, maybe? This isn't
    // very sound and likely has bugs.
    auto ok_after = r.has_trk_progress(r.id());
    if (ok_before && !ok_after) {
      auto found = false;
      for (const auto& set : {cs.voters(), cs.learners(), cs.voters_outgoing()}) {
        for (const auto& id : set) {
          if (id == r.id()) {
            found = true;
            break;
          }
        }
        if (found) {
          break;
        }
      }
      if (!found) {
        is_active_prop_chan = false;
      }
    }

    auto ec = co_await conf_state_chan_.async_send(std::move(cs));
    if (!ec.has_value()) {
      SPDLOG_ERROR("Failed to send conf state, error: {}", ec.error().message());
    }
    SPDLOG_INFO("conf_chan_ send token_chan and wait next loop");
    co_await token_chan.async_send(asio::error_code{});
  }
  co_return;
}

asio::awaitable<void> node::listen_tick(signal_channel& token_chan) {
  while (done_chan_.is_open() && token_chan.is_open() && tick_chan_.is_open()) {
    asio::error_code ec;
    auto result = co_await tick_chan_.async_receive();
    if (!result) {
      SPDLOG_ERROR(result.error().message());
      break;
    }
    raw_node_.tick();
    SPDLOG_INFO("tick_chan_ send token_chan and wait next loop");
    co_await token_chan.async_send(asio::error_code{});
  }
  co_return;
}

asio::awaitable<void> node::listen_status(signal_channel& token_chan) {
  while (done_chan_.is_open() && token_chan.is_open() && status_chan_.is_open()) {
    asio::error_code ec;
    auto result = co_await status_chan_.async_receive();
    if (!result) {
      SPDLOG_ERROR(result.error().message());
      break;
    }
    if (auto chan = result->lock()) {
      co_await chan->async_send(raw_node_.status());
      SPDLOG_INFO("status_chan send token_chan and wait next loop");
    } else {
      SPDLOG_WARN("try lock status callback channel failed");
    }
    co_await token_chan.async_send(asio::error_code{});
  }
  co_return;
}

asio::awaitable<void> node::listen_stop(std::array<signal_channel_handle, 4>& signal_chan_group) {
  co_await stop_chan_.async_receive();
  SPDLOG_INFO("receive stop signal and stop all channels");
  prop_chan_.close();
  recv_chan_.close();
  conf_chan_.close();
  conf_state_chan_.close();
  ready_chan_.close();
  ready_request_chan_.close();
  advance_chan_.close();
  tick_chan_.close();
  status_chan_.close();
  for (auto& chan : signal_chan_group) {
    assert(chan != nullptr);
    chan->close();
  }
  SPDLOG_INFO("ready to send cancel and done signal");
  co_await done_chan_.async_send(asio::error_code{});
  SPDLOG_INFO("send done signal successful and close done chan");
  done_chan_.close();
  co_return;
}

asio::awaitable<expected<void>> node::handle_non_prop_msg(raftpb::message&& msg) {
  auto debugMsg = msg.DebugString();
  SPDLOG_INFO("ready to send non-proposal message, {}", debugMsg);

  auto ec = co_await recv_chan_.async_send(std::move(msg));
  if (!ec) {
    SPDLOG_ERROR("Failed to recv non-proposal message: {}", ec.error().message());
  }
  SPDLOG_INFO("send non-proposal message {} successful", debugMsg);
  co_return expected<void>{};
}

asio::awaitable<expected<void>> node::step_impl(raftpb::message&& msg) {
  SPDLOG_DEBUG(msg.DebugString());
  const auto msg_type = msg.type();
  if (msg_type != raftpb::message_type::MSG_PROP) {
    auto result = co_await handle_non_prop_msg(std::move(msg));
    co_return result;
  }

  auto ec = co_await prop_chan_.async_send(msg_with_result{.msg = std::move(msg), .err_chan = std::nullopt});
  if (!ec.has_value()) {
    SPDLOG_ERROR("Failed to handle non-proposal message: {}", ec.error().message());
  }
  co_return ec;
}

asio::awaitable<expected<void>> node::step_with_wait_impl(asio::any_io_executor executor, raftpb::message&& msg) {
  SPDLOG_DEBUG(msg.DebugString());
  const auto msg_type = msg.type();
  if (msg_type != raftpb::message_type::MSG_PROP) {
    auto result = co_await handle_non_prop_msg(std::move(msg));
    co_return result;
  }

  if (!done_chan_.is_open()) {
    co_return tl::unexpected(raft_error::STOPPED);
  }

  auto err_chan = std::make_shared<channel_endpoint<std::error_code>>(executor);
  auto ec = co_await prop_chan_.async_send(msg_with_result{.msg = std::move(msg), .err_chan = std::weak_ptr(err_chan)});
  if (!ec.has_value()) {
    SPDLOG_ERROR("Failed to handle non-proposal message: {}", ec.error().message());
    co_return tl::unexpected(ec.error());
  }
  auto result = co_await err_chan->async_receive();
  if (result.has_value()) {
    co_return tl::unexpected{result.value()};
  }
  co_return tl::unexpected{result.error()};
}

node_handle setup_node(asio::any_io_executor executor, lepton::config&& config, std::vector<peer>&& peers) {
  if (peers.empty()) {
    LEPTON_CRITICAL("no peers given; use RestartNode instead");
  }
  auto raw_node_result = leaf::try_handle_some(
      [&]() -> leaf::result<lepton::raw_node> {
        BOOST_LEAF_AUTO(v, new_raw_node(std::move(config)));
        return v;
      },
      [&](const lepton_error& e) -> leaf::result<lepton::raw_node> {
        LEPTON_CRITICAL(e.message);
        return new_error(e);
      });
  assert(raw_node_result);
  auto _ = leaf::try_handle_some(
      [&]() -> leaf::result<void> {
        BOOST_LEAF_CHECK(raw_node_result->bootstrap(std::move(peers)));
        return {};
      },
      [&](const lepton_error& e) -> leaf::result<void> {
        SPDLOG_WARN("error occurred during starting a new node: {}", e.message);
        return {};
      });
  return std::make_unique<node>(executor, std::move(*raw_node_result));
}

node_handle start_node(asio::any_io_executor executor, lepton::config&& config, std::vector<peer>&& peers) {
  auto node_handler = setup_node(executor, std::move(config), std::move(peers));
  node_handler->start_run();
  return node_handler;
}

node_handle restart_node(asio::any_io_executor executor, lepton::config&& config) {
  auto raw_node_result = leaf::try_handle_some(
      [&]() -> leaf::result<lepton::raw_node> {
        BOOST_LEAF_AUTO(v, new_raw_node(std::move(config)));
        return v;
      },
      [&](const lepton_error& e) -> leaf::result<lepton::raw_node> {
        LEPTON_CRITICAL(e.message);
        return new_error(e);
      });
  assert(raw_node_result);

  auto node_handler = std::make_unique<node>(executor, std::move(*raw_node_result));
  node_handler->start_run();
  return node_handler;
}

}  // namespace lepton
