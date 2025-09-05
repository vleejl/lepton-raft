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
#include "channel.h"
#include "channel_endpoint.h"
#include "co_spawn_waiter.h"
#include "config.h"
#include "describe.h"
#include "memory_storage.h"
#include "node.h"
#include "node_interface.h"
#include "protobuf.h"
#include "raft.pb.h"
#include "raft_network.h"
#include "ready.h"
#include "signal_channel_endpoint.h"
#include "spdlog/spdlog.h"
#include "types.h"
namespace rafttest {

enum class event_type { tick, ready, recv, stop, pause };

struct event {
  event_type type;
  std::variant<std::monostate, lepton::ready_handle, raftpb::message, bool> payload;
};

struct node_adapter {
  node_adapter(asio::any_io_executor executor, lepton::node_proxy &&node_handle, std::uint64_t id,
               std::unique_ptr<iface> &&iface, std::unique_ptr<lepton::memory_storage> &&storage)
      : executor_(executor),
        event_chan_(executor),
        node_handle_(std::move(node_handle)),
        id_(id),
        iface_(std::move(iface)),
        stop_chan_(executor),
        pause_chan_(executor),
        storage_(std::move(storage)) {}

  void start() { co_spawn(executor_, start_impl(), asio::detached); }

  // stop stops the node. stop a stopped node might panic.
  // All in memory state of node is discarded.
  // All stable MUST be unchanged.
  asio::awaitable<void> stop() {
    if (stop_source_.stop_requested()) {
      // Node has already been stopped - no need to do anything
      SPDLOG_INFO("{} node has already been stopped, just return", id_);
      co_return;
    }
    iface_->disconnect();
    iface_->close();
    SPDLOG_INFO("{} ready to send stop signal", id_);
    co_await stop_chan_.async_send(asio::error_code{});
    SPDLOG_INFO("{} Block until the stop has been acknowledged by run()", id_);
    // wait for the shutdown
    co_await stop_chan_.async_receive();
    SPDLOG_INFO("{} receive stop siganl and stop has been acknowledged by run()", id_);
    stop_chan_.close();
  }

  // restart restarts the node. restart a started node
  // blocks and might affect the future stop operation.
  asio::awaitable<void> restart() {
    // wait for the shutdown
    co_await stop_chan_.async_receive();
    lepton::config c;
    c.id = id_;
    c.election_tick = 10;
    c.heartbeat_tick = 1;
    c.storage = storage_.get();
    c.max_size_per_msg = 1024 * 1024;
    c.max_inflight_msgs = 256;
    c.max_uncommitted_entries_size = 1 << 30;
    node_handle_ = lepton::restart_node(executor_, std::move(c));
    start();
    iface_->connect();
  }

  // pause pauses the node.
  // The paused node buffers the received messages and replies
  // all of them when it resumes.
  asio::awaitable<void> pause() { co_await pause_chan_.async_send(true); }

  // resume resumes the paused node.
  asio::awaitable<void> resumes() { co_await pause_chan_.async_send(false); }

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

  asio::awaitable<lepton::expected<void>> propose_conf_change(const lepton::pb::conf_change_var &cc) {
    co_return co_await node_handle_->propose_conf_change(cc);
  }

  asio::awaitable<lepton::expected<lepton::ready_handle>> wait_ready(asio::any_io_executor executor) {
    co_return co_await node_handle_->wait_ready(executor);
  }

  asio::awaitable<void> advance() {
    co_await node_handle_->advance();
    co_return;
  }

  asio::awaitable<lepton::expected<raftpb::conf_state>> apply_conf_change(raftpb::conf_change_v2 &&cc) {
    co_return co_await node_handle_->apply_conf_change(std::move(cc));
  }

  asio::awaitable<lepton::expected<lepton::status>> status() { co_return co_await node_handle_->status(); }

  asio::awaitable<void> report_unreachable(std::uint64_t id) {
    co_await node_handle_->report_unreachable(id);
    co_return;
  }

  asio::awaitable<void> report_snapshot(std::uint64_t id, lepton::snapshot_status status) {
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
      SPDLOG_DEBUG("node {} sending msg: {}", id, msg.DebugString());
      if (auto iface = iface_handle.lock()) {
        iface->send(msg);
      }
      co_return;
    };
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, 9);  // [0, 9]的均匀分布

    auto waiter = lepton::make_co_spawn_waiter<std::function<asio::awaitable<void>()>>(executor_);
    waiter->add([&]() -> asio::awaitable<void> { co_await listen_tick(); });
    waiter->add([&]() -> asio::awaitable<void> { co_await listen_ready(); });
    waiter->add([&]() -> asio::awaitable<void> { co_await listen_recv(); });
    waiter->add([&]() -> asio::awaitable<void> { co_await listen_pause(); });
    co_spawn(executor_, listen_stop(), asio::detached);
    while (is_running()) {
      SPDLOG_TRACE("node {} waiting for events", id_);
      auto ev_result = co_await event_chan_.async_receive();
      if (!ev_result) {
        SPDLOG_INFO("node {} event channel closed, exiting", id_);
        break;
      }
      auto ev = std::move(ev_result.value());
      switch (ev.type) {
        case event_type::tick:
          node_handle_->tick();
          break;
        case event_type::ready: {
          auto rd_handle = std::get<lepton::ready_handle>(ev.payload);
          auto &rd = *rd_handle.get();
          SPDLOG_INFO(lepton::describe_ready(rd));
          if (!lepton::pb::is_empty_hard_state(rd.hard_state)) {
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
          bool p = std::get<bool>(ev.payload);
          if (!p) break;  // pause=false 直接忽略
          lepton::pb::repeated_message recvms;
          while (is_running()) {
            auto ev2_result = co_await event_chan_.async_receive();
            if (!ev2_result) {
              SPDLOG_INFO("node {} event channel closed, exiting", id_);
              co_return;
            }
            auto ev2 = std::move(ev2_result.value());
            if (ev2.type == event_type::recv) {
              recvms.Add(std::move(std::get<raftpb::message>(ev2.payload)));
            } else if (ev2.type == event_type::pause) {
              bool p2 = std::get<bool>(ev2.payload);
              if (!p2) {
                break;  // 退出 pause
              }
              // 如果再次 true，就继续收
            } else {
              // Go 实现是只 select {recv,pause}，所以这里我们丢掉其他事件
            }
          }
          for (auto &m : recvms) {
            co_await node_handle_->step(std::move(m));
          }
          break;
        }
      }
    }
    co_await waiter->wait_all();
    SPDLOG_INFO("node {} all listeners exited", id_);
    co_return;
  }

  asio::awaitable<void> listen_tick() {
    auto interval = std::chrono::milliseconds(5);
    asio::steady_timer timer(executor_, interval);
    while (is_running()) {
      timer.expires_after(interval);
      co_await timer.async_wait();
      co_await event_chan_.async_send(event{.type = event_type::tick, .payload = {}});
    }
    SPDLOG_INFO("node {} tick listener exited", id_);
    co_return;
  }

  asio::awaitable<void> listen_ready() {
    while (is_running()) {
      auto rd_handle_result = co_await node_handle_->wait_ready(executor_);
      if (!rd_handle_result) {
        SPDLOG_ERROR("node {} wait_ready failed: {}", id_, rd_handle_result.error().message());
        co_return;
      }
      assert(rd_handle_result);
      auto rd_handle = rd_handle_result.value();
      co_await event_chan_.async_send(event{.type = event_type::ready, .payload = {rd_handle}});
    }
    SPDLOG_INFO("node {} ready listener exited", id_);
    co_return;
  }

  asio::awaitable<void> listen_recv() {
    while (is_running()) {
      auto recv_chan_handle = iface_->recv();
      assert(recv_chan_handle != nullptr);
      auto msg = co_await recv_chan_handle->async_receive();
      if (!msg) {
        SPDLOG_INFO("node {} iface recv channel closed, exiting", id_);
        break;
      }
      co_await event_chan_.async_send(event{event_type::recv, std::move(msg.value())});
    }
    SPDLOG_INFO("node {} recv listener exited", id_);
    co_return;
  }

  asio::awaitable<void> listen_stop() {
    co_await stop_chan_.async_receive();
    stop_source_.request_stop();
    event_chan_.close();
    pause_chan_.close();
    co_await node_handle_->stop();
    co_await stop_chan_.async_send(asio::error_code{});
    co_return;
  }

  asio::awaitable<void> listen_pause() {
    while (is_running()) {
      auto result = co_await pause_chan_.async_receive();
      if (!result) {
        SPDLOG_INFO("node {} pause channel closed, exiting", id_);
        break;
      }
      co_await event_chan_.async_send(event{event_type::pause, result.value()});
    }
    SPDLOG_INFO("node {} pause listener exited", id_);
    co_return;
  }

 public:
  asio::any_io_executor executor_;
  std::stop_source stop_source_;
  lepton::channel_endpoint<event> event_chan_;

  lepton::node_proxy node_handle_;
  std::uint64_t id_;
  std::shared_ptr<rafttest::iface> iface_;
  lepton::signal_channel stop_chan_;
  lepton::channel_endpoint<bool> pause_chan_;

  // stable
  std::unique_ptr<lepton::memory_storage> storage_;

  std::mutex mu;
  raftpb::hard_state state;
};

inline std::unique_ptr<node_adapter> start_node(asio::any_io_executor executor, std::uint64_t id,
                                                std::vector<lepton::peer> &&peers, std::unique_ptr<iface> &&iface) {
  auto mm_storage_ptr = std::make_unique<lepton::memory_storage>();
  lepton::config c;
  c.id = id;
  c.election_tick = 10;
  c.heartbeat_tick = 1;
  c.storage = mm_storage_ptr.get();
  c.max_size_per_msg = 1024 * 1024;
  c.max_committed_size_per_ready = c.max_size_per_msg;
  c.max_inflight_msgs = 256;
  c.max_inflight_bytes = lepton::NO_LIMIT;
  c.max_uncommitted_entries_size = 1 << 30;
  auto rn = lepton::start_node(executor, std::move(c), std::move(peers));
  auto n = std::make_unique<node_adapter>(executor, std::move(rn), id, std::move(iface), std::move(mm_storage_ptr));
  n->start();
  return n;
}

}  // namespace rafttest

#endif  // _LEPTON_NODE_H_
