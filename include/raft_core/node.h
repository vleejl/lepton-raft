#pragma once
#ifndef _LEPTON_NODE_
#define _LEPTON_NODE_
#include <spdlog/spdlog.h>

#include <asio.hpp>
#include <asio/any_io_executor.hpp>
#include <asio/coroutine.hpp>
#include <asio/experimental/channel.hpp>
#include <asio/experimental/co_spawn.hpp>
#include <asio/io_context.hpp>
#include <cassert>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "asio/awaitable.hpp"
#include "basic/utility_macros.h"
#include "coroutine/channel.h"
#include "coroutine/channel_endpoint.h"
#include "coroutine/signal_channel_endpoint.h"
#include "error/expected.h"
#include "raft.pb.h"
#include "raft_core/node_interface.h"
#include "raft_core/raft.h"
#include "raft_core/raw_node.h"
#include "raft_core/ready.h"
#include "raft_core/tracker/state.h"
#include "v4/proxy.h"

namespace lepton::core {

struct msg_with_result {
  raftpb::message msg;
  std::optional<std::weak_ptr<coro::channel_endpoint<std::error_code>>> err_chan;
};

using msg_with_result_channel_handle = coro::channel_endpoint<msg_with_result>*;

// node is the canonical implementation of the Node interface
class node {
  NOT_COPYABLE_NOT_MOVABLE(node)
 public:
  node(asio::any_io_executor executor, raw_node&& raw_node)
      : executor_(executor),
        token_chan_(executor_),
        active_prop_chan_(executor_),
        active_ready_chan_(executor_),
        active_advance_chan_(executor_),
        prop_chan_(executor_),
        recv_chan_(executor_),
        conf_chan_(executor_),
        conf_state_chan_(executor_),
        ready_chan_(executor_),
        ready_request_chan_(executor_),
        advance_chan_(executor_),
        tick_chan_(executor_, 128),
        done_chan_(executor_),
        stop_chan_(executor_),
        wait_run_exit_chan_(executor_),
        status_chan_(executor_),
        raw_node_(std::move(raw_node)) {}

  ~node() { stop_source_.request_stop(); }

  asio::awaitable<void> stop();

  void start_run();

  asio::awaitable<void> run();

  void tick();

  asio::awaitable<expected<void>> campaign();

  asio::awaitable<expected<void>> propose(std::string&& data);

  asio::awaitable<expected<void>> propose(asio::any_io_executor executor, std::string&& data);

  asio::awaitable<expected<void>> step(raftpb::message&& msg);

  asio::awaitable<expected<void>> propose_conf_change(const pb::conf_change_var& cc);

  asio::awaitable<expected<ready_handle>> wait_ready(asio::any_io_executor executor);

  asio::awaitable<void> advance();

  asio::awaitable<expected<raftpb::conf_state>> apply_conf_change(raftpb::conf_change_v2&& cc);

  asio::awaitable<expected<lepton::core::status>> status();

  asio::awaitable<void> report_unreachable(std::uint64_t id);

  asio::awaitable<void> report_snapshot(std::uint64_t id, snapshot_status status);

  asio::awaitable<void> transfer_leadership(std::uint64_t lead, std::uint64_t transferee);

  asio::awaitable<expected<void>> forget_leader();

  asio::awaitable<expected<void>> read_index(std::string&& data);

#ifdef LEPTON_TEST
 public:
#else
 private:
#endif
  asio::awaitable<void> listen_ready(coro::signal_channel_endpoint& token_chan,
                                     coro::signal_channel_endpoint& active_ready_chan,
                                     std::atomic<bool>& ready_inflight);

  asio::awaitable<void> listen_propose(coro::signal_channel_endpoint& token_chan,
                                       coro::signal_channel_endpoint& active_prop_chan, bool& is_active_prop_chan);

  asio::awaitable<void> listen_receive(coro::signal_channel_endpoint& token_chan);

  asio::awaitable<void> listen_conf_change(coro::signal_channel_endpoint& token_chan, bool& is_active_prop_chan);

  asio::awaitable<void> listen_tick(coro::signal_channel_endpoint& token_chan);

  asio::awaitable<void> listen_status(coro::signal_channel_endpoint& token_chan);

  asio::awaitable<void> listen_stop();

  asio::awaitable<expected<void>> handle_non_prop_msg(raftpb::message&& msg);

  asio::awaitable<expected<void>> step_impl(raftpb::message&& msg);

  asio::awaitable<expected<void>> step_with_wait_impl(asio::any_io_executor executor, raftpb::message&& msg);

  bool is_running() const { return !stop_source_.stop_requested(); }

// 为了方便单元测试 修改私有成员函数作用域
#ifdef LEPTON_TEST
 public:
#else
 private:
#endif
  asio::any_io_executor executor_;
  std::stop_source stop_source_;

  coro::signal_channel_endpoint token_chan_;
  coro::signal_channel_endpoint active_prop_chan_;
  coro::signal_channel_endpoint active_ready_chan_;
  coro::signal_channel_endpoint active_advance_chan_;

  coro::channel_endpoint<msg_with_result> prop_chan_;
  coro::channel_endpoint<raftpb::message> recv_chan_;
  coro::channel_endpoint<raftpb::conf_change_v2> conf_chan_;
  coro::channel_endpoint<raftpb::conf_state> conf_state_chan_;
  ready_channel ready_chan_;
  coro::channel_endpoint<std::weak_ptr<ready_channel>> ready_request_chan_;
  coro::signal_channel_endpoint advance_chan_;
  coro::signal_channel_endpoint tick_chan_;
  coro::signal_channel done_chan_;
  coro::signal_channel stop_chan_;
  std::atomic<bool> started_{false};
  coro::signal_channel wait_run_exit_chan_;
  status_channel status_chan_;
  raw_node raw_node_;
};

using node_handle = std::unique_ptr<node>;

node_handle setup_node(asio::any_io_executor executor, lepton::core::config&& config, std::vector<peer>&& peers);

using node_proxy = pro::proxy<node_builder>;

// StartNode returns a new Node given configuration and a list of raft peers.
// It appends a ConfChangeAddNode entry for each given peer to the initial log.
//
// Peers must not be zero length; call RestartNode in that case.
node_proxy start_node(asio::any_io_executor executor, lepton::core::config&& config, std::vector<peer>&& peers);

// RestartNode is similar to StartNode but does not take a list of peers.
// The current membership of the cluster will be restored from the Storage.
// If the caller has an existing state machine, pass in the last log index that
// has been applied to it; otherwise use zero.
node_proxy restart_node(asio::any_io_executor executor, lepton::core::config&& config);
}  // namespace lepton::core

#endif  // _LEPTON_NODE_