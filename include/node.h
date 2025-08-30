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
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "asio/awaitable.hpp"
#include "channel.h"
#include "channel_endpoint.h"
#include "expected.h"
#include "node_interface.h"
#include "raft.h"
#include "raft.pb.h"
#include "raw_node.h"
#include "ready.h"
#include "signal_channel_endpoint.h"
#include "state.h"
#include "utility_macros.h"
#include "v4/proxy.h"

namespace lepton {

struct msg_with_result {
  raftpb::message msg;
  std::optional<std::weak_ptr<channel_endpoint<std::error_code>>> err_chan;
};

using msg_with_result_channel_handle = channel_endpoint<msg_with_result>*;

// node is the canonical implementation of the Node interface
class node {
  NONCOPYABLE_NONMOVABLE(node)
 public:
  node(asio::any_io_executor executor, raw_node&& raw_node)
      : executor_(executor),
        token_chan_(executor),
        active_prop_chan_(executor),
        active_ready_chan_(executor),
        active_advance_chan_(executor),
        prop_chan_(executor),
        recv_chan_(executor),
        conf_chan_(executor),
        conf_state_chan_(executor),
        ready_chan_(executor),
        ready_request_chan_(executor),
        advance_chan_(executor),
        tick_chan_(executor, 128),
        done_chan_(executor),
        stop_chan_(executor),
        status_chan_(executor),
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

  asio::awaitable<expected<lepton::status>> status();

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
  asio::awaitable<void> listen_ready(signal_channel_endpoint& token_chan, signal_channel_endpoint& active_ready_chan,
                                     signal_channel_endpoint_handle& advance_chan,
                                     signal_channel_endpoint& active_advance_chan);

  asio::awaitable<void> listen_advance(std::optional<ready_handle>& rd, signal_channel_endpoint& token_chan,
                                       signal_channel_endpoint& active_advance_chan,
                                       signal_channel_endpoint_handle& advance_chan);

  asio::awaitable<void> listen_propose(signal_channel_endpoint& token_chan, signal_channel_endpoint& active_prop_chan,
                                       bool& is_active_prop_chan);

  asio::awaitable<void> listen_receive(signal_channel_endpoint& token_chan);

  asio::awaitable<void> listen_conf_change(signal_channel_endpoint& token_chan, bool& is_active_prop_chan);

  asio::awaitable<void> listen_tick(signal_channel_endpoint& token_chan);

  asio::awaitable<void> listen_status(signal_channel_endpoint& token_chan);

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

  signal_channel_endpoint token_chan_;
  signal_channel_endpoint active_prop_chan_;
  signal_channel_endpoint active_ready_chan_;
  signal_channel_endpoint active_advance_chan_;

  channel_endpoint<msg_with_result> prop_chan_;
  channel_endpoint<raftpb::message> recv_chan_;
  channel_endpoint<raftpb::conf_change_v2> conf_chan_;
  channel_endpoint<raftpb::conf_state> conf_state_chan_;
  ready_channel ready_chan_;
  channel_endpoint<std::weak_ptr<ready_channel>> ready_request_chan_;
  signal_channel_endpoint advance_chan_;
  signal_channel_endpoint tick_chan_;
  signal_channel done_chan_;
  signal_channel stop_chan_;
  status_channel status_chan_;
  raw_node raw_node_;
};

using node_handle = std::unique_ptr<node>;

node_handle setup_node(asio::any_io_executor executor, lepton::config&& config, std::vector<peer>&& peers);

using node_proxy = pro::proxy<node_builder>;

// StartNode returns a new Node given configuration and a list of raft peers.
// It appends a ConfChangeAddNode entry for each given peer to the initial log.
//
// Peers must not be zero length; call RestartNode in that case.
node_proxy start_node(asio::any_io_executor executor, lepton::config&& config, std::vector<peer>&& peers);

// RestartNode is similar to StartNode but does not take a list of peers.
// The current membership of the cluster will be restored from the Storage.
// If the caller has an existing state machine, pass in the last log index that
// has been applied to it; otherwise use zero.
node_proxy restart_node(asio::any_io_executor executor, lepton::config&& config);
}  // namespace lepton

#endif  // _LEPTON_NODE_