#include "node.h"

#include <cassert>
#include <memory>
#include <system_error>
#include <utility>

#include "asio/awaitable.hpp"
#include "asio/error_code.hpp"
#include "channel.h"
#include "expected.h"
#include "leaf_expected.h"
#include "raft.pb.h"
#include "raw_node.h"
#include "spdlog/spdlog.h"
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
  msg_with_result_channel_handle prop_chan = nullptr;
  // ready_channel_handle ready_chan = nullptr;
  signal_channel ready_active_chan(executor_);
  signal_channel advance_active_chan(executor_);
  signal_channel_handle advance_chan = nullptr;

  std::optional<ready> rd;
  auto& r = raw_node_.raft_;

  auto lead = NONE;

  auto token = stop_source_.get_token();
  while (!stop_source_.stop_requested()) {
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
      advance_chan = &advance_chan_;
      rd = raw_node_.ready_without_accept();
      co_spawn(executor_, send_ready(rd, ready_active_chan, advance_active_chan, advance_chan), asio::detached);
    }

    if (lead != r.lead()) {
      if (r.has_leader()) {
        if (lead == NONE) {
          SPDLOG_INFO("raft.node: {} elected leader {} at term {}", r.id(), r.lead(), r.term());
        } else {
          SPDLOG_INFO("raft.node: {} changed leader from {} to {} at term {}", r.id(), lead, r.lead(), r.term());
        }
        prop_chan = &prop_chan_;
      } else {
        SPDLOG_INFO("raft.node: {} lost leader {} at term {}", r.id(), lead, r.term());
        prop_chan = nullptr;
      }
      lead = r.lead();
    }

    if (prop_chan != nullptr) {
      SPDLOG_INFO("ready to listen 5 channels ......");
      auto result = co_await asio::experimental::make_parallel_group(
                        [&](auto token) { return prop_chan->async_receive(token); },
                        [&](auto token) { return recv_chan_.async_receive(token); },
                        [&](auto token) { return conf_chan_.async_receive(token); },
                        [&](auto token) { return tick_chan_.async_receive(token); },
                        [&](auto token) { return ready_active_chan.async_receive(token); },
                        [&](auto token) { return advance_active_chan.async_receive(token); },
                        [&](auto token) { return status_chan_.async_receive(token); },
                        [&](auto token) { return stop_chan_.async_receive(token); })
                        .async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);
      auto [order, prop_chan_ec, prop_chan_result, recv_chan_ec, recv_chan_result, conf_chan_ec, conf_chan_result,
            tick_chan_result, ready_active_chan_ec, advance_active_chan_ec, status_chan_ec, status_chan_result,
            stop_chan_result] = result;
      switch (order[0]) {
        case 0: {
          co_await propose_chan_callback(prop_chan_ec, std::move(prop_chan_result));
          break;
        }
        case 1: {
          receive_chan_callback(recv_chan_ec, std::move(recv_chan_result));
          break;
        }
        case 2: {
          co_await conf_chan_callback(conf_chan_ec, std::move(conf_chan_result), prop_chan);
          break;
        }
        case 3: {
          raw_node_.tick();
          break;
        }
        case 4:
          break;
        case 5:
          break;
        case 6: {
          co_await status_chan_callback(status_chan_ec, std::move(status_chan_result));
          break;
        }
        case 7: {
          co_await stop_chan_callback(stop_chan_result);
          co_return;
        }
      }
    } else {
      SPDLOG_INFO("ready to listen 4 channels ......");
      auto result = co_await asio::experimental::make_parallel_group(
                        [&](auto token) { return recv_chan_.async_receive(token); },
                        [&](auto token) { return conf_chan_.async_receive(token); },
                        [&](auto token) { return tick_chan_.async_receive(token); },
                        [&](auto token) { return ready_active_chan.async_receive(token); },
                        [&](auto token) { return advance_active_chan.async_receive(token); },
                        [&](auto token) { return status_chan_.async_receive(token); },
                        [&](auto token) { return done_chan_.async_receive(token); })
                        .async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);
      auto [order, recv_chan_ec, recv_chan_result, conf_chan_ec, conf_chan_result, tick_chan_result,
            ready_active_chan_ec, advance_active_chan_ec, status_chan_ec, status_chan_result, stop_chan_result] =
          result;
      switch (order[0]) {
        case 0: {
          receive_chan_callback(recv_chan_ec, std::move(recv_chan_result));
          break;
        }
        case 1: {
          co_await conf_chan_callback(conf_chan_ec, std::move(conf_chan_result), prop_chan);
          break;
        }
        case 2: {
          raw_node_.tick();
          break;
        }
        case 3:
          break;
        case 4:
          break;
        case 5: {
          co_await status_chan_callback(status_chan_ec, std::move(status_chan_result));
          break;
        }
        case 6: {
          co_await stop_chan_callback(stop_chan_result);
          co_return;
        }
      }
    }
  }
  co_return;
}

void node::start_run() { co_spawn(executor_, run(), asio::detached); }

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
asio::awaitable<void> node::tick() {
  auto ec = co_await async_select_done([&](auto token) { return tick_chan_.async_send(asio::error_code{}, token); },
                                       done_chan_);
  if (!ec.has_value()) {
    SPDLOG_ERROR("Tick operation aborted: {}", ec.error().message());
  }
  co_return;
}

asio::awaitable<expected<void>> node::campaign() {
  raftpb::message msg;
  msg.set_type(::raftpb::message_type::MSG_HUP);
  auto result = co_await step_impl(std::move(msg));
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

ready_channel& node::ready_handle() { return ready_chan_; }

asio::awaitable<void> node::advance() {
  auto ec = co_await async_select_done([&](auto token) { return advance_chan_.async_send(asio::error_code{}, token); },
                                       done_chan_);
  if (!ec.has_value()) {
    SPDLOG_ERROR("Failed to recv non-proposal message: {}", ec.error().message());
  }
  co_return;
}

asio::awaitable<raftpb::conf_state> node::apply_conf_change(raftpb::conf_change_v2&& cc) {
  if (!stop_chan_.is_open()) {
    co_return raftpb::conf_state{};
  }
  asio::error_code ec;
  co_await conf_chan_.async_send(asio::error_code{}, cc, asio::redirect_error(asio::use_awaitable, ec));
  if (ec != asio::error::operation_aborted && ec) {
    SPDLOG_ERROR(ec.message());
  }
  auto cs = co_await conf_state_chan_.async_receive(asio::redirect_error(asio::use_awaitable, ec));
  if (ec != asio::error::operation_aborted && ec) {
    SPDLOG_ERROR(ec.message());
    co_return raftpb::conf_state{};
  }
  co_return cs;
}

asio::awaitable<lepton::status> node::status() {
  if (!stop_chan_.is_open()) {
    co_return lepton::status{};
  }

  channel<lepton::status> status_chan(executor_);
  asio::error_code ec;
  co_await status_chan_.async_send(asio::error_code{}, status_with_channel{std::ref(status_chan)},
                                   asio::redirect_error(asio::use_awaitable, ec));
  if (ec != asio::error::operation_aborted && ec) {
    SPDLOG_ERROR(ec.message());
    co_return lepton::status{};
  }
  auto result = co_await status_chan.async_receive(asio::redirect_error(asio::use_awaitable, ec));
  if (ec != asio::error::operation_aborted && ec) {
    SPDLOG_ERROR(ec.message());
    co_return lepton::status{};
  }
  co_return result;
}

asio::awaitable<void> node::report_unreachable(std::uint64_t id) {
  if (!stop_chan_.is_open()) {
    co_return;
  }
  raftpb::message msg;
  msg.set_from(id);
  msg.set_type(raftpb::message_type::MSG_UNREACHABLE);
  asio::error_code ec;
  co_await recv_chan_.async_send(asio::error_code{}, std::move(msg), asio::redirect_error(asio::use_awaitable, ec));
  if (ec != asio::error::operation_aborted && ec) {
    SPDLOG_ERROR(ec.message());
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
  asio::error_code ec;
  co_await recv_chan_.async_send(asio::error_code{}, std::move(msg), asio::redirect_error(asio::use_awaitable, ec));
  if (ec != asio::error::operation_aborted && ec) {
    SPDLOG_ERROR(ec.message());
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
  asio::error_code ec;
  co_await recv_chan_.async_send(asio::error_code{}, std::move(msg), asio::redirect_error(asio::use_awaitable, ec));
  if (ec != asio::error::operation_aborted && ec) {
    SPDLOG_ERROR(ec.message());
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

asio::awaitable<void> node::propose_chan_callback(std::error_code callback_ec, msg_with_result&& result) {
  auto& msg = result.msg;
  msg.set_from(raw_node_.raft_.id());
  auto step_result = leaf_to_expected_void([&]() -> leaf::result<void> {
    BOOST_LEAF_CHECK(raw_node_.raft_.step(std::move(msg)));
    return {};
  });
  if (result.ec_chan.has_value()) {
    std::error_code ec;
    if (!step_result.has_value()) {
      ec = step_result.error();
    }
    co_await result.ec_chan->get().async_send(asio::error_code{}, ec, asio::use_awaitable);
    result.ec_chan->get().close();
  }
  co_return;
}

void node::receive_chan_callback(std::error_code _, raftpb::message&& msg) {
  if (pb::is_response_msg(msg.type()) && !pb::is_local_msg_target(msg.from()) &&
      raw_node_.raft_.has_trk_progress(msg.from())) {
    // Filter out response message from unknown From.
  } else {
    auto _ = raw_node_.raft_.step(std::move(msg));
  }
}

asio::awaitable<void> node::conf_chan_callback(std::error_code _, raftpb::conf_change_v2&& cc,
                                               msg_with_result_channel_handle& prop_chan) {
  auto& r = raw_node_.raft_;
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
      prop_chan = nullptr;
    }
  }
  auto ec = co_await async_select_done(
      [&](auto token) { return conf_state_chan_.async_send(asio::error_code{}, std::move(cs), token); }, done_chan_);
  if (!ec.has_value()) {
    SPDLOG_ERROR("Failed to send conf state, error: {}", ec.error().message());
  }
  co_return;
}

asio::awaitable<void> node::send_ready(std::optional<ready>& rd, signal_channel& ready_active_chan,
                                       signal_channel& advance_active_chan, signal_channel_handle& advance_chan) {
  assert(rd.has_value());
  auto ec = co_await async_select_done(
      [&](auto token) { return ready_chan_.async_send(asio::error_code{}, rd->clone(), token); }, done_chan_);
  if (!ec.has_value()) {
    SPDLOG_ERROR("Failed to send ready, error: {}", ec.error().message());
    co_return;
  }
  ready_active_chan.try_send(asio::error_code{});
  raw_node_.accept_ready(*rd);
  if (!raw_node_.async_storage_writes()) {
    ec = co_await async_select_done([&](auto token) { return advance_chan_.async_receive(token); }, done_chan_);
    if (!ec.has_value()) {
      SPDLOG_ERROR("Failed to receive advance, error: {}", ec.error().message());
      co_return;
    }
    raw_node_.advance();
    advance_active_chan.try_send(asio::error_code{});
  }
  rd.reset();
  advance_chan = nullptr;
  co_return;
}

asio::awaitable<void> node::status_chan_callback(std::error_code callback_ec, status_with_channel&& status_chan) {
  asio::error_code ec;
  assert(status_chan.chan.has_value());
  co_await status_chan.chan->get().async_send(asio::error_code{}, raw_node_.status(),
                                              asio::redirect_error(asio::use_awaitable, ec));
}

asio::awaitable<void> node::stop_chan_callback(std::error_code callback_ec) {
  prop_chan_.close();
  recv_chan_.close();
  conf_chan_.close();
  advance_chan_.close();
  tick_chan_.close();
  status_chan_.close();
  SPDLOG_INFO("ready to send done signal");
  co_await done_chan_.async_send(asio::error_code{});
  SPDLOG_INFO("send done signal successful and close done chan");
  done_chan_.close();
}

asio::awaitable<expected<void>> node::handle_non_prop_msg(raftpb::message&& msg) {
  SPDLOG_INFO("ready to send non-proposal message");
  auto ec = co_await async_select_done(
      [&](auto token) { return recv_chan_.async_send(asio::error_code{}, std::move(msg), token); }, done_chan_);
  if (!ec.has_value()) {
    SPDLOG_ERROR("Failed to recv non-proposal message: {}", ec.error().message());
  }
  co_return ec;
}

asio::awaitable<expected<void>> node::step_impl(raftpb::message&& msg) {
  auto msg_type = msg.type();
  if (msg_type != raftpb::message_type::MSG_PROP) {
    auto result = co_await handle_non_prop_msg(std::move(msg));
    co_return result;
  }

  auto ec = co_await async_select_done(
      [&](auto token) {
        return prop_chan_.async_send(asio::error_code{},
                                     msg_with_result{.msg = std::move(msg), .ec_chan = std::nullopt}, token);
      },
      done_chan_);
  if (!ec.has_value()) {
    SPDLOG_ERROR("Failed to handle non-proposal message: {}", ec.error().message());
  }
  co_return ec;
}

asio::awaitable<expected<void>> node::step_with_wait_impl(asio::any_io_executor executor, raftpb::message&& msg) {
  auto msg_type = msg.type();
  if (msg_type != raftpb::message_type::MSG_PROP) {
    auto result = co_await handle_non_prop_msg(std::move(msg));
    co_return result;
  }

  if (!done_chan_.is_open()) {
    co_return tl::unexpected(raft_error::STOPPED);
  }

  channel<std::error_code> ec_chan(executor);
  auto ec = co_await async_select_done(
      [&](auto token) {
        return prop_chan_.async_send(
            asio::error_code{}, msg_with_result{.msg = std::move(msg), .ec_chan = std::optional(std::ref(ec_chan))},
            token);
      },
      done_chan_);
  if (!ec.has_value()) {
    SPDLOG_ERROR("Failed to handle non-proposal message: {}", ec.error().message());
    co_return tl::unexpected(ec.error());
  }

  auto result =
      co_await async_select_done_with_value([&](auto token) { return ec_chan.async_receive(token); }, done_chan_);
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
  auto bootstrap_result = raw_node_result->bootstrap(std::move(peers));
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
