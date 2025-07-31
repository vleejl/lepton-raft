#include "node.h"

#include <system_error>
#include <utility>

#include "expected.h"
#include "leaf_expected.h"
#include "spdlog/spdlog.h"
#include "tl/expected.hpp"

namespace lepton {

void node::stop() {
  std::promise<void> stop_promise;
  auto stop_future = stop_promise.get_future();

  asio::post(executor_, [this, &stop_promise] {
    // 情况1：已停止
    // 重入 stop 函数保护
    if (!done_chan_.is_open()) {
      stop_promise.set_value();
      return;
    }

    // 情况2：成功发送停止信号
    if (stop_chan_.try_send(asio::error_code{})) {
      // 启动异步等待协程
      co_spawn(
          executor_,
          [this, &stop_promise]() -> asio::awaitable<void> {  // 在协程中等待完成信号
            asio::error_code ec;
            co_await done_chan_.async_receive(asio::redirect_error(asio::use_awaitable, ec));

            // 忽略通道关闭错误
            if (ec != asio::experimental::channel_errc::channel_closed) {
              SPDLOG_ERROR("closed done channel with error: {}", ec.message());
            }
            stop_promise.set_value();
          },
          asio::detached);
    }
    // 情况3：发送失败（已停止或停止中）
    else {
      stop_promise.set_value();
    }
  });

  // 阻塞等待停止完成
  stop_future.wait();
}

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
asio::awaitable<void> node::tick() {
  auto ec = co_await async_select_done([&](auto token) { return tick_chan_.async_receive(token); }, done_chan_);
  if (!ec.has_value()) {
    SPDLOG_ERROR("Tick operation aborted: {}", ec.error().message());
  }
  co_return;
}

auto node::campaign() { return raw_node_.campaign(); }

asio::awaitable<expected<void>> node::propose(asio::any_io_executor& executor, std::string&& data) {
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
  co_return leaf_to_expected_void([&]() -> leaf::result<void> {
    BOOST_LEAF_CHECK(raw_node_.step(std::move(*msg_result)));
    return {};
  });
}

auto node::ready_handle() const { return read_only_channel(ready_chan_); }

asio::awaitable<void> node::advance() {
  if (!stop_chan_.is_open()) {
    co_return;
  }
  asio::error_code ec;
  co_await advance_chan_.async_receive(asio::redirect_error(asio::use_awaitable, ec));
  if (ec != asio::error::operation_aborted && ec) {
    SPDLOG_ERROR(ec.message());
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

asio::awaitable<void> node::listen_propose(signal_channel& trigger_chan, signal_channel& active_prop_chan) {
  auto& r = raw_node_.raft_;
  while (done_chan_.is_open()) {
    co_await active_prop_chan.async_receive();
    asio::error_code ec;
    auto msg_result = co_await prop_chan_.async_receive(asio::redirect_error(asio::use_awaitable, ec));
    if (ec == asio::error::operation_aborted) {
      break;
    } else if (ec) {
      SPDLOG_ERROR(ec.message());
      break;
    }
    auto& msg = msg_result.msg;
    msg.set_from(r.id());
    if (msg_result.ec_chan) {
      auto result = leaf_to_expected_void([&]() -> leaf::result<void> {
        BOOST_LEAF_CHECK(r.step(std::move(msg)));
        return {};
      });
      std::error_code ec;
      if (!result) {
        ec = result.error();
      }
      co_await msg_result.ec_chan->get().async_send(asio::error_code{}, ec);
      msg_result.ec_chan->get().close();
    } else {
      auto _ = r.step(std::move(msg));
    }
    co_await trigger_chan.async_send(asio::error_code{});
  }
}

asio::awaitable<void> node::listen_receive(signal_channel& trigger_chan) {
  while (recv_chan_.is_open() && done_chan_.is_open()) {
    asio::error_code ec;
    auto msg = co_await recv_chan_.async_receive(asio::redirect_error(asio::use_awaitable, ec));
    if (ec == asio::error::operation_aborted) {
      break;
    } else if (ec) {
      SPDLOG_ERROR(ec.message());
      break;
    }
    if (pb::is_response_msg(msg.type()) && !pb::is_local_msg_target(msg.from()) &&
        raw_node_.raft_.has_trk_progress(msg.from())) {
      // Filter out response message from unknown From.
    } else {
      auto _ = raw_node_.step(std::move(msg));
    }
    co_await trigger_chan.async_send(asio::error_code{});
  }
}

asio::awaitable<void> node::send_conf_state(raftpb::conf_state&& cs) {
  asio::error_code ec;
  co_await conf_state_chan_.async_send(asio::error_code{}, std::move(cs),
                                       asio::redirect_error(asio::use_awaitable, ec));
  if (ec != asio::error::operation_aborted && ec) {
    SPDLOG_ERROR(ec.message());
  }
  co_return;
}

asio::awaitable<void> node::listen_conf_change(signal_channel& trigger_chan, bool& is_active_prop_chan) {
  auto& r = raw_node_.raft_;
  while (conf_chan_.is_open() && done_chan_.is_open()) {
    asio::error_code ec;
    auto cc = co_await conf_chan_.async_receive(asio::redirect_error(asio::use_awaitable, ec));
    if (ec == asio::error::operation_aborted) {
      break;
    } else if (ec) {
      SPDLOG_ERROR(ec.message());
      break;
    }
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
      if (found) {
        is_active_prop_chan = false;
      }
    }
    // 避免阻塞
    co_spawn(executor_, send_conf_state(std::move(cs)), asio::detached);
    co_await trigger_chan.async_send(asio::error_code{});
  }
  co_return;
}

asio::awaitable<void> node::listen_tick(signal_channel& trigger_chan) {
  while (tick_chan_.is_open() && done_chan_.is_open()) {
    asio::error_code ec;
    co_await tick_chan_.async_receive(asio::redirect_error(asio::use_awaitable, ec));
    if (ec == asio::error::operation_aborted) {
      break;
    } else if (ec) {
      SPDLOG_ERROR(ec.message());
      break;
    }
    raw_node_.tick();
    co_await trigger_chan.async_send(asio::error_code{});
  }
  co_return;
}

asio::awaitable<void> node::send_ready(std::optional<ready>& rd) {
  co_await ready_chan_->async_send(asio::error_code{}, rd->clone());
  raw_node_.accept_ready(*rd);
  if (raw_node_.async_storage_writes()) {
    rd.reset();
  }
  co_return;
}

asio::awaitable<void> node::listen_advance(signal_channel& trigger_chan, std::optional<ready>& rd) {
  while (advance_chan_.is_open() && done_chan_.is_open()) {
    co_await advance_chan_.async_receive();
    raw_node_.advance();
    rd.reset();
    co_await trigger_chan.async_send(asio::error_code{});
  }
  co_return;
}

asio::awaitable<void> node::listen_status(signal_channel& trigger_chan) {
  while (status_chan_.is_open() && done_chan_.is_open()) {
    asio::error_code ec;
    auto status_chan = co_await status_chan_.async_receive(asio::redirect_error(asio::use_awaitable, ec));
    if (ec == asio::error::operation_aborted) {
      break;
    } else if (ec) {
      SPDLOG_ERROR(ec.message());
      break;
    }
    assert(status_chan.chan.has_value());
    co_await status_chan.chan->get().async_send(asio::error_code{}, raw_node_.status(),
                                                asio::redirect_error(asio::use_awaitable, ec));
    co_await trigger_chan.async_send(asio::error_code{});
  }
  co_return;
}

asio::awaitable<void> node::listen_stop() {
  co_await stop_chan_.async_receive();
  done_chan_.close();
  prop_chan_.close();
  recv_chan_.close();
  conf_chan_.close();
  advance_chan_.close();
  tick_chan_.close();
  status_chan_.close();
  co_return;
}

asio::awaitable<void> node::run() {
  signal_channel trigger_chan(executor_);

  bool is_active_prop_chan = false;
  signal_channel active_prop_chan(executor_);

  std::optional<ready> rd;
  auto& r = raw_node_.raft_;

  auto lead = NONE;

  co_spawn(executor_, listen_propose(trigger_chan, active_prop_chan), asio::detached);
  co_spawn(executor_, listen_receive(trigger_chan), asio::detached);
  co_spawn(executor_, listen_conf_change(trigger_chan, is_active_prop_chan), asio::detached);
  co_spawn(executor_, listen_tick(trigger_chan), asio::detached);
  co_spawn(executor_, listen_advance(trigger_chan, rd), asio::detached);
  co_spawn(executor_, listen_status(trigger_chan), asio::detached);
  co_spawn(executor_, listen_stop(), asio::detached);

  while (done_chan_.is_open()) {
    if (raw_node_.has_ready()) {
      // Populate a Ready. Note that this Ready is not guaranteed to
      // actually be handled. We will arm readyc, but there's no guarantee
      // that we will actually send on it. It's possible that we will
      // service another channel instead, loop around, and then populate
      // the Ready again. We could instead force the previous Ready to be
      // handled first, but it's generally good to emit larger Readys plus
      // it simplifies testing (by emitting less frequently and more
      // predictably).
      rd = raw_node_.ready();
      co_await send_ready(rd);
    }

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
      co_await active_prop_chan.async_send(asio::error_code{});
    }

    co_await trigger_chan.async_receive();
  }

  co_return;
}

asio::awaitable<expected<void>> node::handle_non_prop_msg(raftpb::message&& msg) {
  auto ec = co_await async_select_done(
      [&](auto token) { return recv_chan_.async_send(asio::error_code{}, std::move(msg), token); }, done_chan_);
  if (!ec.has_value()) {
    SPDLOG_ERROR("Failed to handle non-proposal message: {}", ec.error().message());
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

asio::awaitable<expected<void>> node::step_with_wait_impl(asio::any_io_executor& executor, raftpb::message&& msg) {
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

}  // namespace lepton
