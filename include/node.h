#ifndef _LEPTON_NODE_
#define _LEPTON_NODE_
#include <asio.hpp>
#include <asio/any_io_executor.hpp>
#include <asio/coroutine.hpp>
#include <asio/detached.hpp>
#include <asio/experimental/channel.hpp>
#include <asio/experimental/co_spawn.hpp>
#include <asio/experimental/parallel_group.hpp>
#include <asio/io_context.hpp>
#include <cassert>
#include <cstdint>
#include <optional>
#include <string>
#include <system_error>

#include "asio/error_code.hpp"
#include "asio/use_awaitable.hpp"
#include "channel.h"
#include "config.h"
#include "node_interface.h"
#include "protobuf.h"
#include "raft.h"
#include "raft.pb.h"
#include "raw_node.h"
#include "ready.h"
#include "spdlog/spdlog.h"
#include "utility_macros.h"

namespace lepton {
struct peer {
  std::uint64_t id;
  std::string context;
};

struct msg_with_result {
  raftpb::message msg;
  std::optional<channel<leaf::result<void>>> ec_chan;
};

using msg_with_result_channel_handle = channel<msg_with_result>*;

// node is the canonical implementation of the Node interface
class node {
  NOT_COPYABLE(node)
 public:
  node(asio::any_io_executor executor, raw_node&& raw_node)
      : executor_(executor),
        prop_chan_(executor),
        recv_chan_(executor),
        conf_chan_(executor),
        conf_state_chan_(executor),
        ready_chan_(executor),
        advance_chan_(executor),
        tick_chan_(executor, 128),
        done_chan_(executor),
        stop_chan_(executor),
        status_chan_(executor),
        raw_node_(std::move(raw_node)) {}

  void stop() {
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

 private:
  asio::awaitable<void> listen_propose(signal_channel& trigger_chan, signal_channel& active_prop_chan) {
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
        co_await msg_result.ec_chan->async_send(asio::error_code{}, r.step(std::move(msg)));
        msg_result.ec_chan->close();
      } else {
        auto _ = r.step(std::move(msg));
      }
      co_await trigger_chan.async_send(asio::error_code{});
    }
  }

  asio::awaitable<void> listen_receive(signal_channel& trigger_chan) {
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

  asio::awaitable<void> send_conf_state(raftpb::conf_state&& cs) {
    asio::error_code ec;
    co_await conf_state_chan_.async_send(asio::error_code{}, std::move(cs),
                                         asio::redirect_error(asio::use_awaitable, ec));
    if (ec != asio::error::operation_aborted && ec) {
      SPDLOG_ERROR(ec.message());
    }
    co_return;
  }

  asio::awaitable<void> listen_conf_change(signal_channel& trigger_chan, bool& is_active_prop_chan) {
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

  asio::awaitable<void> listen_tick(signal_channel& trigger_chan) {
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

  asio::awaitable<void> send_ready(std::optional<ready>& rd) {
    co_await ready_chan_.async_send(asio::error_code{}, rd->clone());
    raw_node_.accept_ready(*rd);
    if (raw_node_.async_storage_writes()) {
      rd.reset();
    }
    co_return;
  }

  asio::awaitable<void> listen_advance(signal_channel& trigger_chan, std::optional<ready>& rd) {
    while (advance_chan_.is_open() && done_chan_.is_open()) {
      co_await advance_chan_.async_receive();
      raw_node_.advance();
      rd.reset();
      co_await trigger_chan.async_send(asio::error_code{});
    }
    co_return;
  }

  asio::awaitable<void> listen_status(signal_channel& trigger_chan) {
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
      co_await status_chan.chan->async_send(asio::error_code{}, raw_node_.status(),
                                            asio::redirect_error(asio::use_awaitable, ec));
      co_await trigger_chan.async_send(asio::error_code{});
    }
    co_return;
  }

  asio::awaitable<void> listen_stop() {
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

  asio::awaitable<void> run() {
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

 private:
  asio::any_io_executor executor_;

  channel<msg_with_result> prop_chan_;
  channel<raftpb::message> recv_chan_;
  channel<raftpb::conf_change_v2> conf_chan_;
  channel<raftpb::conf_state> conf_state_chan_;
  ready_channel ready_chan_;
  signal_channel advance_chan_;
  signal_channel tick_chan_;
  signal_channel done_chan_;
  signal_channel stop_chan_;
  status_channel status_chan_;
  raw_node raw_node_;
};
}  // namespace lepton

#endif  // _LEPTON_NODE_