#pragma once
#ifndef _LEPTON_NODE_H_
#define _LEPTON_NODE_H_
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>
#include <variant>
#include <vector>

#include "asio/awaitable.hpp"
#include "asio/error_code.hpp"
#include "asio/use_awaitable.hpp"
#include "asio/use_future.hpp"
#include "async_mutex.h"
#include "basic/enum_name.h"
#include "basic/spdlog_logger.h"
#include "coroutine/channel.h"
#include "coroutine/channel_endpoint.h"
#include "coroutine/co_spawn_waiter.h"
#include "coroutine/signal_channel_endpoint.h"
#include "raft.pb.h"
#include "raft_core/config.h"
#include "raft_core/describe.h"
#include "raft_core/memory_storage.h"
#include "raft_core/node.h"
#include "raft_core/node_interface.h"
#include "raft_core/pb/protobuf.h"
#include "raft_core/pb/types.h"
#include "raft_core/ready.h"
#include "raft_network.h"
#include "spdlog/spdlog.h"
namespace rafttest {

enum class event_type { tick, ready, recv, stop, pause };

struct event {
  event_type type;
  std::variant<std::monostate, lepton::core::ready_handle, raftpb::message, bool> payload;
};

struct node_adapter;
std::unique_ptr<node_adapter> start_node(asio::any_io_executor executor, std::uint64_t id,
                                         std::unique_ptr<iface> &&iface);

struct node_adapter {
  node_adapter(asio::any_io_executor executor, lepton::core::node_proxy &&node_handle, std::uint64_t id,
               std::unique_ptr<iface> &&iface, std::unique_ptr<lepton::core::memory_storage> &&storage)
      : executor_(executor),
        event_chan_(executor_),
        node_handle_(std::move(node_handle)),
        id_(id),
        iface_(std::move(iface)),
        done_chan_(executor_),
        stop_chan_(executor_),
        wait_run_exit_chan_(executor_),
        pause_chan_(executor_),
        storage_(std::move(storage)) {
    iface_->connect();
  }

  void start() { co_spawn(executor_, start_impl(), asio::detached); }

  // stop stops the node. stop a stopped node might panic.
  // All in memory state of node is discarded.
  // All stable MUST be unchanged.
  asio::awaitable<void> stop() {
    if (stop_source_.stop_requested()) {
      // Node has already been stopped - no need to do anything
      LOG_INFO("{} node has already been stopped, just return", id_);
      co_return;
    }
    iface_->disconnect();
    iface_->close();
    LOG_INFO("{} ready to send stop signal", id_);
    co_await stop_chan_.async_send(asio::error_code{});
    LOG_INFO("{} Block until the stop has been acknowledged by run()", id_);
    // Block until the stop has been acknowledged by run()
    co_await done_chan_.async_receive();
    done_chan_.close();
    co_await wait_run_exit_chan_.async_receive();
    LOG_INFO("{} receive stop siganl and stop has been acknowledged by run()", id_);
    stop_chan_.close();
  }

  // restart restarts the node. restart a started node
  // blocks and might affect the future stop operation.
  static asio::awaitable<std::unique_ptr<node_adapter>> restart_node(std::unique_ptr<node_adapter> &&node,
                                                                     raft_network *nt) {
    co_await node->stop();
    // 确保所有操作在移动前完成
    co_await asio::post(asio::bind_executor(node->executor_, asio::use_awaitable));
    auto id = node->id_;
    auto executor = node->executor_;
    auto storage = std::move(node->storage_);
    LOG_INFO("node {} is stopped and ready to restart", id);
    lepton::core::config c;
    c.id = id;
    c.election_tick = 10;
    c.heartbeat_tick = 1;
    c.storage = storage.get();
    c.max_size_per_msg = 1024 * 1024;
    c.max_committed_size_per_ready = c.max_size_per_msg;
    c.max_inflight_msgs = 256;
    c.max_inflight_bytes = lepton::core::NO_LIMIT;
    c.max_uncommitted_entries_size = 1 << 30;
    c.logger = std::make_shared<lepton::spdlog_logger>();
    auto rn = lepton::core::restart_node(executor, std::move(c));
    std::unique_ptr<iface> iface = std::make_unique<rafttest::node_network>(id, nt);
    auto n = std::make_unique<node_adapter>(executor, std::move(rn), id, std::move(iface), std::move(storage));
    n->start();
    co_return n;
  }

  // pause pauses the node.
  // The paused node buffers the received messages and replies
  // all of them when it resumes.
  asio::awaitable<void> pause() {
    paused_.store(true, std::memory_order_relaxed);
    co_return;
  }

  // resume resumes the paused node.
  asio::awaitable<void> resume() {
    paused_.store(false, std::memory_order_relaxed);
    // flush buffered messages
    for (auto &m : paused_recv_ms_buffer_) {
      co_await event_chan_.async_send(event{event_type::recv, std::move(m)});
    }
    paused_recv_ms_buffer_.Clear();
    co_return;
  }

  void tick() { node_handle_->tick(); }

  asio::awaitable<lepton::expected<void>> campaign() { co_return co_await node_handle_->campaign(); }

  asio::awaitable<lepton::expected<void>> propose(std::string &&data) {
    co_return co_await node_handle_->propose(std::move(data));
  }

  asio::awaitable<lepton::expected<void>> propose(asio::any_io_executor executor, std::string &&data) {
    co_return co_await node_handle_->propose(std::move(data));
  }

  asio::awaitable<lepton::expected<void>> step(raftpb::message &&msg) {
    co_return co_await node_handle_->step(std::move(msg));
  }

  asio::awaitable<lepton::expected<void>> propose_conf_change(const lepton::core::pb::conf_change_var &cc) {
    co_return co_await node_handle_->propose_conf_change(cc);
  }

  asio::awaitable<lepton::expected<lepton::core::ready_handle>> wait_ready(asio::any_io_executor executor) {
    co_return co_await node_handle_->wait_ready(executor);
  }

  asio::awaitable<void> advance() {
    co_await node_handle_->advance();
    co_return;
  }

  asio::awaitable<lepton::expected<raftpb::conf_state>> apply_conf_change(raftpb::conf_change_v2 &&cc) {
    co_return co_await node_handle_->apply_conf_change(std::move(cc));
  }

  asio::awaitable<lepton::expected<lepton::core::status>> status() { co_return co_await node_handle_->status(); }

  asio::awaitable<void> report_unreachable(std::uint64_t id) {
    co_await node_handle_->report_unreachable(id);
    co_return;
  }

  asio::awaitable<void> report_snapshot(std::uint64_t id, lepton::core::snapshot_status status) {
    co_await node_handle_->report_snapshot(id, status);
    co_return;
  }

  asio::awaitable<void> transfer_leadership(std::uint64_t lead, std::uint64_t transferee) {
    co_await node_handle_->transfer_leadership(lead, transferee);
    co_return;
  }

  asio::awaitable<lepton::expected<void>> read_index(std::string &&data) {
    co_return co_await node_handle_->read_index(std::move(data));
  }

  ~node_adapter() { stop_source_.request_stop(); }

 private:
  bool is_running() const { return !stop_source_.stop_requested(); }

  asio::awaitable<void> start_impl() {
    auto send_msg = [](asio::any_io_executor executor, std::weak_ptr<rafttest::iface> iface_handle, std::size_t id,
                       raftpb::message msg, int wait_ms) -> asio::awaitable<void> {
      asio::steady_timer timer(executor, asio::chrono::milliseconds(wait_ms));
      co_await timer.async_wait(asio::use_awaitable);
      LOG_TRACE("node {} sending msg: {}", id, msg.DebugString());
      if (auto iface = iface_handle.lock()) {
        iface->send(msg);
      }
      co_return;
    };
    auto id = id_;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, 9);  // [0, 9]的均匀分布

    auto waiter = lepton::coro::make_co_spawn_waiter<std::function<asio::awaitable<void>()>>(executor_);
    waiter->add([&]() -> asio::awaitable<void> { co_await listen_tick(); });
    waiter->add([&]() -> asio::awaitable<void> { co_await listen_ready(); });
    waiter->add([&]() -> asio::awaitable<void> { co_await listen_recv(); });
    waiter->add([&]() -> asio::awaitable<void> { co_await listen_pause(); });
    waiter->add([&]() -> asio::awaitable<void> { co_await listen_stop(); });
    while (is_running()) {
      LOG_TRACE("node {} waiting for events", id_);
      auto ev_result = co_await event_chan_.async_receive();
      if (!ev_result) {
        LOG_TRACE("node {} event channel closed, exiting", id_);
        break;
      }
      auto ev = std::move(ev_result.value());
      LOG_TRACE("node {} received event: {}", id_, magic_enum::enum_name(ev.type));
      switch (ev.type) {
        case event_type::tick:
          node_handle_->tick();
          break;
        case event_type::ready: {
          auto rd_handle = std::get<lepton::core::ready_handle>(ev.payload);
          auto &rd = *rd_handle.get();
          LOG_TRACE(lepton::core::describe_ready(rd, nullptr));
          if (!lepton::core::pb::is_empty_hard_state(rd.hard_state)) {
            std::lock_guard<std::mutex> lock(mu);
            state = rd.hard_state;
            storage_->set_hard_state(std::move(state));
          }
          storage_->append(std::move(rd.entries));
          asio::steady_timer timer(executor_, std::chrono::milliseconds(1));
          co_await timer.async_wait();

          // simulate async send, more like real world...
          for (auto &msg : rd.messages) {
            int wait_ms = dist(gen);
            co_spawn(executor_, send_msg(executor_, iface_, id_, std::move(msg), wait_ms), asio::detached);
          }
          co_await node_handle_->advance();
          break;
        }
        case event_type::recv:
          co_spawn(executor_, node_handle_->step(std::move(std::get<raftpb::message>(ev.payload))), asio::detached);
          break;
        case event_type::stop:
          co_return;
        case event_type::pause: {
          break;
        }
      }
    }
    co_await waiter->wait_all();
    LOG_INFO("node {} all listeners exited", id);
    co_await wait_run_exit_chan_.async_send(asio::error_code{});
    co_return;
  }

  asio::awaitable<void> listen_tick() {
    auto interval = std::chrono::milliseconds(5);
    asio::steady_timer timer(executor_, interval);
    while (is_running()) {
      timer.expires_after(interval);
      co_await timer.async_wait();
      if (paused_.load(std::memory_order_relaxed)) {
        continue;
      }
      co_await event_chan_.async_send(event{.type = event_type::tick, .payload = {}});
    }
    LOG_INFO("node {} tick listener exited", id_);
    co_return;
  }

  asio::awaitable<void> listen_ready() {
    while (is_running()) {
      auto rd_handle_result = co_await node_handle_->wait_ready(executor_);
      if (!rd_handle_result) {
        LOG_ERROR("node {} wait_ready failed: {}", id_, rd_handle_result.error().message());
        break;
      }
      assert(rd_handle_result);
      auto rd_handle = rd_handle_result.value();
      co_await event_chan_.async_send(event{.type = event_type::ready, .payload = {rd_handle}});
    }
    LOG_INFO("node {} ready listener exited", id_);
    co_return;
  }

  asio::awaitable<void> listen_recv() {
    while (is_running()) {
      auto recv_chan_handle = iface_->recv();
      assert(recv_chan_handle != nullptr);
      auto msg = co_await recv_chan_handle->async_receive();
      if (!msg) {
        LOG_INFO("node {} iface recv channel closed, exiting", id_);
        break;
      }
      if (paused_.load(std::memory_order_relaxed)) {
        paused_recv_ms_buffer_.Add(std::move(msg.value()));
      } else {
        co_await event_chan_.async_send(event{event_type::recv, std::move(msg.value())});
      }
    }
    LOG_INFO("node {} recv listener exited", id_);
    co_return;
  }

  asio::awaitable<void> listen_stop() {
    co_await stop_chan_.async_receive();
    stop_source_.request_stop();
    event_chan_.close();
    pause_chan_.close();
    co_await node_handle_->stop();
    assert(done_chan_.is_open());
    co_await done_chan_.async_send(asio::error_code{});
    co_await asio::post(asio::bind_executor(executor_, asio::use_awaitable));
    LOG_INFO("node {} all chaneels has closed, and has send stop_chan response", id_);
    co_return;
  }

  asio::awaitable<void> listen_pause() {
    while (is_running()) {
      auto result = co_await pause_chan_.async_receive();
      if (!result) {
        LOG_INFO("node {} pause channel closed, exiting", id_);
        break;
      }
      co_await event_chan_.async_send(event{event_type::pause, result.value()});
    }
    LOG_INFO("node {} pause listener exited", id_);
    co_return;
  }

 public:
  asio::any_io_executor executor_;
  std::stop_source stop_source_;
  lepton::coro::channel_endpoint<event> event_chan_;

  lepton::core::node_proxy node_handle_;
  std::uint64_t id_;
  std::shared_ptr<rafttest::iface> iface_;
  lepton::coro::signal_channel done_chan_;
  lepton::coro::signal_channel stop_chan_;
  lepton::coro::signal_channel wait_run_exit_chan_;
  lepton::coro::channel_endpoint<bool> pause_chan_;
  std::atomic<bool> paused_{false};
  lepton::core::pb::repeated_message paused_recv_ms_buffer_;

  // stable
  std::unique_ptr<lepton::core::memory_storage> storage_;

  std::mutex mu;
  raftpb::hard_state state;
};

inline std::unique_ptr<node_adapter> start_node(asio::any_io_executor executor, std::uint64_t id,
                                                std::vector<lepton::core::peer> &&peers,
                                                std::unique_ptr<iface> &&iface) {
  auto mm_storage_ptr = std::make_unique<lepton::core::memory_storage>();
  lepton::core::config c;
  c.id = id;
  c.election_tick = 10;
  c.heartbeat_tick = 1;
  c.storage = mm_storage_ptr.get();
  c.max_size_per_msg = 1024 * 1024;
  c.max_committed_size_per_ready = c.max_size_per_msg;
  c.max_inflight_msgs = 256;
  c.max_inflight_bytes = lepton::core::NO_LIMIT;
  c.max_uncommitted_entries_size = 1 << 30;
  c.logger = std::make_shared<lepton::spdlog_logger>();
  auto rn = lepton::core::start_node(executor, std::move(c), std::move(peers));
  auto n = std::make_unique<node_adapter>(executor, std::move(rn), id, std::move(iface), std::move(mm_storage_ptr));
  n->start();
  return n;
}

}  // namespace rafttest

#endif  // _LEPTON_NODE_H_
