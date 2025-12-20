#include "raft_core/node.h"

#include <atomic>
#include <cassert>
#include <functional>
#include <memory>
#include <system_error>
#include <utility>

#include "asio/awaitable.hpp"
#include "asio/error_code.hpp"
#include "coroutine/channel_endpoint.h"
#include "coroutine/co_spawn_waiter.h"
#include "coroutine/signal_channel_endpoint.h"
#include "error/expected.h"
#include "error/leaf_expected.h"
#include "error/raft_error.h"
#include "raft.pb.h"
#include "raft_core/node_interface.h"
#include "raft_core/raw_node.h"
#include "raft_core/ready.h"
#include "raft_core/tracker/state.h"
#include "spdlog/spdlog.h"
#include "tl/expected.hpp"
namespace lepton::core {

asio::awaitable<void> node::stop() {
  const auto id = raw_node_.raft_.id();
  SPDLOG_INFO("{} ready to send stop signal", id);
  if (stop_source_.stop_requested()) {
    // Node has already been stopped - no need to do anything
    SPDLOG_INFO("{} node has already been stopped, just return", id);
    co_return;
  }
  // Not already stopped, so trigger it
  asio::error_code ec;
  co_await stop_chan_.async_send(asio::error_code{}, asio::redirect_error(asio::use_awaitable, ec));
  if (ec) {
    // Node has already been stopped - no need to do anything
    co_return;
  }
  SPDLOG_INFO("{} Block until the stop has been acknowledged by run()", id);
  // Block until the stop has been acknowledged by run()
  co_await done_chan_.async_receive();
  done_chan_.close();
  if (started_.load(std::memory_order_relaxed)) {
    co_await wait_run_exit_chan_.async_receive();
  }
  SPDLOG_INFO("{} receive done siganl and stop has been acknowledged by run()", id);
  co_return;
}

asio::awaitable<void> node::run() {
  started_.store(true, std::memory_order_relaxed);

  coro::signal_channel_endpoint& token_chan = token_chan_;

  bool is_active_prop_chan = false;
  coro::signal_channel_endpoint& active_prop_chan = active_prop_chan_;

  coro::signal_channel_endpoint& active_ready_chan = active_ready_chan_;
  std::atomic<bool> ready_inflight{false};

  auto& r = raw_node_.raft_;
  auto lead = NONE;

  auto waiter = coro::make_co_spawn_waiter<std::function<asio::awaitable<void>()>>(executor_);
  waiter->add(
      [&]() -> asio::awaitable<void> { co_await listen_propose(token_chan_, active_prop_chan_, is_active_prop_chan); });
  waiter->add([&]() -> asio::awaitable<void> { co_await listen_receive(token_chan); });
  waiter->add([&]() -> asio::awaitable<void> { co_await listen_conf_change(token_chan, is_active_prop_chan); });
  waiter->add([&]() -> asio::awaitable<void> { co_await listen_tick(token_chan); });
  waiter->add([&]() -> asio::awaitable<void> { co_await listen_ready(token_chan, active_ready_chan, ready_inflight); });
  waiter->add([&]() -> asio::awaitable<void> { co_await listen_status(token_chan); });
  waiter->add([&]() -> asio::awaitable<void> { co_await listen_stop(); });

  while (is_running()) {
    SPDLOG_TRACE("run main loop ......");
    if (!ready_inflight.load(std::memory_order_acquire) && raw_node_.has_ready()) {
      SPDLOG_TRACE("run main loop has ready......");
      // Populate a Ready. Note that this Ready is not guaranteed to
      // actually be handled. We will arm readyc, but there's no guarantee
      // that we will actually send on it. It's possible that we will
      // service another channel instead, loop around, and then populate
      // the Ready again. We could instead force the previous Ready to be
      // handled first, but it's generally good to emit larger Readys plus
      // it simplifies testing (by emitting less frequently and more
      // predictably).
      active_ready_chan.try_send();
    }
    SPDLOG_TRACE("run main loop, continue main loop logic ......");
    if (lead != r.lead()) {
      if (r.has_leader()) {
        if (lead == NONE) {
          SPDLOG_TRACE("raft.node: {} elected leader {} at term {}", r.id(), r.lead(), r.term());
        } else {
          SPDLOG_TRACE("raft.node: {} changed leader from {} to {} at term {}", r.id(), lead, r.lead(), r.term());
        }
        is_active_prop_chan = true;
      } else {
        SPDLOG_TRACE("raft.node: {} lost leader {} at term {}", r.id(), lead, r.term());
        is_active_prop_chan = false;
      }
      lead = r.lead();
    }

    if (is_active_prop_chan) {
      SPDLOG_TRACE("ready to send active prop channel");
      // 不能阻塞主循环
      active_prop_chan.try_send();
      SPDLOG_TRACE("send active prop channel successful");
    }
    co_await token_chan.async_receive();
  }
  co_await waiter->wait_all();
  SPDLOG_DEBUG("{} receive stop signal and exit run loop", r.id());
  co_await wait_run_exit_chan_.async_send(asio::error_code{});
  co_return;
}

void node::start_run() { co_spawn(executor_, run(), asio::detached); }

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
void node::tick() {
  if (!is_running()) {
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
  SPDLOG_TRACE(msg.DebugString());
  if (pb::is_local_msg(msg.type()) && !pb::is_local_msg_target(msg.from())) {
    // Local messages are not handled by step, but by the node's
    // propose method.
    co_return ok();
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

asio::awaitable<expected<ready_handle>> node::wait_ready(asio::any_io_executor executor) {
  auto receive_ready_chan = std::make_shared<ready_channel>(executor);
  SPDLOG_TRACE("prepare to request ready");
  auto result = co_await ready_request_chan_.async_send(std::weak_ptr(receive_ready_chan));
  if (!result) {
    SPDLOG_ERROR("send callback channel failed, error:{}", result.error().message());
    co_return tl::unexpected{result.error()};
  }
  SPDLOG_TRACE("request ready callback channel successful and prepare wait callback channel active");
  auto ready = co_await receive_ready_chan->async_receive();
  if (!ready) {
    SPDLOG_ERROR("receive error when try to receive ready, error:{}", ready.error().message());
  } else {
    SPDLOG_TRACE("callback channel actived and receive ready content:\n{}", describe_ready(*ready.value()));
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
  if (!is_running()) {
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

asio::awaitable<expected<lepton::core::status>> node::status() {
  if (!is_running()) {
    co_return tl::unexpected{raft_error::STOPPED};
  }
  auto status_chan = std::make_shared<coro::channel_endpoint<lepton::core::status>>(executor_);
  if (auto result =
          co_await status_chan_.async_send(std::weak_ptr<coro::channel_endpoint<lepton::core::status>>(status_chan));
      !result) {
    SPDLOG_ERROR(result.error().message());
    co_return tl::unexpected(result.error());
  }

  auto result = co_await status_chan->async_receive();
  if (!result) {
    SPDLOG_ERROR(result.error().message());
    co_return tl::unexpected(result.error());
  }
  co_return tl::expected<lepton::core::status, std::error_code>(std::move(result.value()));
}

asio::awaitable<void> node::report_unreachable(std::uint64_t id) {
  if (!is_running()) {
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
  if (!is_running()) {
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
  if (!is_running()) {
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

asio::awaitable<void> node::listen_ready(coro::signal_channel_endpoint& token_chan,
                                         coro::signal_channel_endpoint& active_ready_chan,
                                         std::atomic<bool>& ready_inflight) {
  while (is_running()) {
    SPDLOG_TRACE("waiting active_ready_chan signal......");
    if (auto result = co_await active_ready_chan.async_receive(); !result) {
      SPDLOG_ERROR("Failed to receive active_ready_chan, error: {}", result.error().message());
      break;
    }
    SPDLOG_TRACE("receive active_ready_chan signal");
    // 走到这里表面一定有 ready
    auto result = co_await ready_request_chan_.async_receive();
    if (!result) {
      SPDLOG_ERROR("Failed to receive ready_request_chan, error: {}", result.error().message());
      break;
    }
    if (auto chan = result->lock()) {
      auto rd = std::make_shared<ready>(raw_node_.ready_without_accept());
      SPDLOG_TRACE("ready to send ready by ready_channel, {}", describe_ready(*rd.get()));
      // etcd raft 实现要求，收到 ready以后必须立马 accept_ready
      // 为了避免使用 co_await 导致堆栈被切出去，所以先确认 (accept_ready) 再发送
      raw_node_.accept_ready(*rd.get());
      co_await chan->async_send(rd);
      SPDLOG_TRACE("send ready by ready_channel successful, {}", describe_ready(*rd.get()));
      if (!raw_node_.async_storage_writes()) {
        ready_inflight.store(true, std::memory_order_release);
        co_await token_chan.async_send();
        if (auto ec = co_await advance_chan_.async_receive(); !ec) {
          SPDLOG_ERROR("Failed to receive advance, error: {}", ec.error().message());
          break;
        }
        SPDLOG_TRACE("async_receive advance by advance_chan_ successfully");
        raw_node_.advance();
        ready_inflight.store(false, std::memory_order_release);
        // advance 确认完成后需要立马开始监听 ready_chan，所以不能使用 co_await 避免堆栈被切出去
        token_chan.try_send();
      } else {
        rd.reset();
        co_await token_chan.async_send();
      }
      SPDLOG_TRACE("finish send ready by ready_channel successfully");
    } else {
      SPDLOG_TRACE("callback channel has been released, wait next loop");
      co_await token_chan.async_send();
    }
  }
  SPDLOG_INFO("[listen func]ready_request_chan_ closed, exit listen_ready loop");
  co_return;
}

asio::awaitable<void> node::listen_propose(coro::signal_channel_endpoint& token_chan,
                                           coro::signal_channel_endpoint& active_prop_chan, bool& is_active_prop_chan) {
  while (is_running()) {
    if (!is_active_prop_chan) {
      SPDLOG_TRACE("waiting active_prop_chan signal......");
      co_await active_prop_chan.async_receive();
      SPDLOG_TRACE("receive active_prop_chan signal");
    }
    SPDLOG_TRACE("ready to listen prop_chan");
    auto msg_result = co_await prop_chan_.async_receive();
    if (!msg_result) {
      SPDLOG_ERROR(msg_result.error().message());
      break;
    }

    auto& msg = msg_result->msg;
    SPDLOG_TRACE("receive msg by prop_chan and ready to process, {}", msg.DebugString());
    msg.set_from(raw_node_.raft_.id());
    auto step_result = leaf_to_expected_void([&]() -> leaf::result<void> {
      LEPTON_LEAF_CHECK(raw_node_.raft_.step(std::move(msg)));
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
    SPDLOG_TRACE("prop_chan_ send token_chan and wait next loop", msg.DebugString());
    co_await token_chan.async_send();
  }
  SPDLOG_INFO("[listen func]prop_chan_ closed, exit listen_propose loop");
  co_return;
}

asio::awaitable<void> node::listen_receive(coro::signal_channel_endpoint& token_chan) {
  while (is_running()) {
    SPDLOG_TRACE("begin recv_chan_ async_receive....");
    auto result = co_await recv_chan_.async_receive();
    if (!result) {
      SPDLOG_ERROR(result.error().message());
      break;
    }
    auto& msg = *result;
    SPDLOG_TRACE("recv_chan_ async_receive receive msg. {}", msg.DebugString());
    if (pb::is_response_msg(msg.type()) && !pb::is_local_msg_target(msg.from()) &&
        !raw_node_.raft_.has_trk_progress(msg.from())) {
      SPDLOG_TRACE("message type: {}, msg from: {}. Filter out response message from unknown From",
                   enum_name(msg.type()), msg.from());
      // Filter out response message from unknown From.
    } else {
      auto _ = raw_node_.raft_.step(std::move(msg));
    }
    SPDLOG_TRACE("recv_chan_ send token_chan and wait next loop");
    co_await token_chan.async_send();
    SPDLOG_TRACE("send token signal successful");
  }
  SPDLOG_INFO("[listen func]recv_chan_ closed, exit listen_receive loop");
  co_return;
}

asio::awaitable<void> node::listen_conf_change(coro::signal_channel_endpoint& token_chan, bool& is_active_prop_chan) {
  auto& r = raw_node_.raft_;
  while (is_running()) {
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
    SPDLOG_TRACE("conf_chan_ send token_chan and wait next loop");
    co_await token_chan.async_send();
  }
  SPDLOG_INFO("[listen func]conf_chan_ closed, exit listen_conf_change loop");
  co_return;
}

asio::awaitable<void> node::listen_tick(coro::signal_channel_endpoint& token_chan) {
  while (is_running()) {
    asio::error_code ec;
    auto result = co_await tick_chan_.async_receive();
    if (!result) {
      SPDLOG_ERROR(result.error().message());
      break;
    }
    raw_node_.tick();
    SPDLOG_TRACE("tick_chan_ send token_chan and wait next loop");
    co_await token_chan.async_send();
  }
  SPDLOG_INFO("[listen func]tick_chan_ closed, exit listen_tick loop");
  co_return;
}

asio::awaitable<void> node::listen_status(coro::signal_channel_endpoint& token_chan) {
  while (is_running()) {
    asio::error_code ec;
    auto result = co_await status_chan_.async_receive();
    if (!result) {
      SPDLOG_ERROR(result.error().message());
      break;
    }
    if (auto chan = result->lock()) {
      co_await chan->async_send(raw_node_.status());
      SPDLOG_TRACE("status_chan send token_chan and wait next loop");
    } else {
      SPDLOG_WARN("try lock status callback channel failed");
    }
    co_await token_chan.async_send();
  }
  SPDLOG_INFO("[listen func]status_chan_ closed, exit listen_status loop");
  co_return;
}

asio::awaitable<void> node::listen_stop() {
  co_await stop_chan_.async_receive();
  SPDLOG_INFO("receive stop signal and stop all channels");
  stop_source_.request_stop();
  token_chan_.close();
  active_prop_chan_.close();
  active_ready_chan_.close();
  active_advance_chan_.close();
  prop_chan_.close();
  recv_chan_.close();
  conf_chan_.close();
  conf_state_chan_.close();
  ready_chan_.close();
  ready_request_chan_.close();
  advance_chan_.close();
  tick_chan_.close();
  status_chan_.close();
  SPDLOG_INFO("ready to send cancel and done signal");
  assert(done_chan_.is_open());
  co_await done_chan_.async_send(asio::error_code{});
  SPDLOG_INFO("send done signal successful");
  SPDLOG_INFO("[listen func]close done chan successful and exit listen_stop");
  co_return;
}

asio::awaitable<expected<void>> node::handle_non_prop_msg(raftpb::message&& msg) {
  auto debugMsg = msg.DebugString();
  SPDLOG_TRACE("ready to send non-proposal message, {}", debugMsg);

  auto ec = co_await recv_chan_.async_send(std::move(msg));
  if (!ec) {
    SPDLOG_ERROR("Failed to recv non-proposal message: {}", ec.error().message());
  }
  SPDLOG_TRACE("send non-proposal message {} successful", debugMsg);
  co_return ok();
}

asio::awaitable<expected<void>> node::step_impl(raftpb::message&& msg) {
  SPDLOG_TRACE(msg.DebugString());
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
  SPDLOG_TRACE(msg.DebugString());
  const auto msg_type = msg.type();
  if (msg_type != raftpb::message_type::MSG_PROP) {
    auto result = co_await handle_non_prop_msg(std::move(msg));
    co_return result;
  }

  if (!is_running()) {
    co_return tl::unexpected(raft_error::STOPPED);
  }

  auto err_chan = std::make_shared<coro::channel_endpoint<std::error_code>>(executor);
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

node_handle setup_node(asio::any_io_executor executor, lepton::core::config&& config, std::vector<peer>&& peers) {
  if (peers.empty()) {
    LEPTON_CRITICAL("no peers given; use RestartNode instead");
  }
  auto raw_node_result = leaf::try_handle_some(
      [&]() -> leaf::result<lepton::core::raw_node> {
        BOOST_LEAF_AUTO(v, new_raw_node(std::move(config)));
        return v;
      },
      [&](const lepton_error& e) -> leaf::result<lepton::core::raw_node> {
        LEPTON_CRITICAL(e.message);
        return new_error(e);
      });
  assert(raw_node_result);
  auto _ = leaf::try_handle_some(
      [&]() -> leaf::result<void> {
        LEPTON_LEAF_CHECK(raw_node_result->bootstrap(std::move(peers)));
        return {};
      },
      [&](const lepton_error& e) -> leaf::result<void> {
        SPDLOG_WARN("error occurred during starting a new node: {}", e.message);
        return {};
      });
  return std::make_unique<node>(executor, std::move(*raw_node_result));
}

node_proxy start_node(asio::any_io_executor executor, lepton::core::config&& config, std::vector<peer>&& peers) {
  auto node_handler = setup_node(executor, std::move(config), std::move(peers));
  node_handler->start_run();
  return node_handler;
}

node_proxy restart_node(asio::any_io_executor executor, lepton::core::config&& config) {
  auto raw_node_result = leaf::try_handle_some(
      [&]() -> leaf::result<lepton::core::raw_node> {
        BOOST_LEAF_AUTO(v, new_raw_node(std::move(config)));
        return v;
      },
      [&](const lepton_error& e) -> leaf::result<lepton::core::raw_node> {
        LEPTON_CRITICAL(e.message);
        return new_error(e);
      });
  assert(raw_node_result);

  auto node_handler = std::make_unique<node>(executor, std::move(*raw_node_result));
  node_handler->start_run();
  return node_handler;
}

}  // namespace lepton::core
