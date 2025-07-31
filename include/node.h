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

#include "asio/awaitable.hpp"
#include "channel.h"
#include "expected.h"
#include "lepton_error.h"
#include "node_interface.h"
#include "raft.h"
#include "raft.pb.h"
#include "raw_node.h"
#include "ready.h"
#include "state.h"
#include "utility_macros.h"

namespace lepton {

struct peer {
  std::uint64_t id;
  std::string context;
};

struct msg_with_result {
  raftpb::message msg;
  std::optional<std::reference_wrapper<channel<std::error_code>>> ec_chan;
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
        ready_chan_(std::make_shared<ready_channel>(executor)),
        advance_chan_(executor),
        tick_chan_(executor, 128),
        done_chan_(executor),
        stop_chan_(executor),
        status_chan_(executor),
        raw_node_(std::move(raw_node)) {}

  void stop();

  asio::awaitable<void> tick();

  auto campaign();

  asio::awaitable<expected<void>> propose(asio::any_io_executor& executor, std::string&& data);

  asio::awaitable<expected<void>> step(raftpb::message&& msg);

  asio::awaitable<expected<void>> propose_conf_change(const pb::conf_change_var& cc);

  auto ready_handle() const;

  asio::awaitable<void> advance();

  asio::awaitable<raftpb::conf_state> apply_conf_change(raftpb::conf_change_v2&& cc);

  asio::awaitable<lepton::status> status();

  asio::awaitable<void> report_unreachable(std::uint64_t id);

  asio::awaitable<void> report_snapshot(std::uint64_t id, snapshot_status status);

  asio::awaitable<void> transfer_leadership(std::uint64_t lead, std::uint64_t transferee);

  asio::awaitable<expected<void>> forget_leader();

  asio::awaitable<expected<void>> read_index(std::string&& data);

 private:
  asio::awaitable<void> listen_propose(signal_channel& trigger_chan, signal_channel& active_prop_chan);

  asio::awaitable<void> listen_receive(signal_channel& trigger_chan);

  asio::awaitable<void> send_conf_state(raftpb::conf_state&& cs);

  asio::awaitable<void> listen_conf_change(signal_channel& trigger_chan, bool& is_active_prop_chan);

  asio::awaitable<void> listen_tick(signal_channel& trigger_chan);

  asio::awaitable<void> send_ready(std::optional<ready>& rd);

  asio::awaitable<void> listen_advance(signal_channel& trigger_chan, std::optional<ready>& rd);

  asio::awaitable<void> listen_status(signal_channel& trigger_chan);

  asio::awaitable<void> listen_stop();

  asio::awaitable<void> run();

  asio::awaitable<expected<void>> handle_non_prop_msg(raftpb::message&& msg);

  asio::awaitable<expected<void>> step_impl(raftpb::message&& msg);

  asio::awaitable<expected<void>> step_with_wait_impl(asio::any_io_executor& executor, raftpb::message&& msg);

// 为了方便单元测试 修改私有成员函数作用域
#ifdef LEPTON_TEST
 public:
#else
 private:
#endif
  asio::any_io_executor executor_;

  channel<msg_with_result> prop_chan_;
  channel<raftpb::message> recv_chan_;
  channel<raftpb::conf_change_v2> conf_chan_;
  channel<raftpb::conf_state> conf_state_chan_;
  std::shared_ptr<ready_channel> ready_chan_;
  signal_channel advance_chan_;
  signal_channel tick_chan_;
  signal_channel done_chan_;
  signal_channel stop_chan_;
  status_channel status_chan_;
  raw_node raw_node_;
};
}  // namespace lepton

#endif  // _LEPTON_NODE_