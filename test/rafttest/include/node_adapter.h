#ifndef _LEPTON_NODE_H_
#define _LEPTON_NODE_H_
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "asio/awaitable.hpp"
#include "asio/error_code.hpp"
#include "asio/use_awaitable.hpp"
#include "asio/use_future.hpp"
#include "async_mutex.h"
#include "channel.h"
#include "config.h"
#include "memory_storage.h"
#include "node.h"
#include "node_interface.h"
#include "protobuf.h"
#include "raft.pb.h"
#include "raft_network.h"
#include "spdlog/spdlog.h"
#include "types.h"
namespace rafttest {

struct node_adapter {
  node_adapter(asio::any_io_executor executor, lepton::node_proxy &&node_handle, std::uint64_t id,
               std::unique_ptr<iface> &&iface, std::unique_ptr<lepton::memory_storage> &&storage)
      : executor_(executor),
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
    iface_->disconnect();
    co_await stop_chan_.async_send(asio::error_code{});
    // wait for the shutdown
    co_await stop_chan_.async_receive();
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
  asio::awaitable<void> pause() { co_await pause_chan_.async_send(asio::error_code{}, true); }

  // resume resumes the paused node.
  asio::awaitable<void> resumes() { co_await pause_chan_.async_send(asio::error_code{}, false); }

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
  asio::awaitable<void> start_impl() {
    lepton::signal_channel token_chan(executor_);
    async_mutex m(executor_);
    // co_spawn(executor_, listen_tick(token_chan), asio::detached);
    // co_spawn(executor_, listen_ready(token_chan), asio::detached);
    // co_spawn(executor_, listen_recv(token_chan, m), asio::detached);
    co_spawn(executor_, listen_stop(token_chan), asio::detached);
    co_spawn(executor_, listen_pause(token_chan, m), asio::detached);
    while (!stop_source_.stop_requested() && stop_chan_.is_open() && token_chan.is_open()) {
      SPDLOG_TRACE("node {} waiting for events", id_);
      co_await token_chan.async_receive();
    }
    co_return;
  }

  asio::awaitable<void> listen_tick(lepton::signal_channel &token_chan) {
    auto interval = std::chrono::milliseconds(5);
    asio::steady_timer timer(executor_, interval);
    timer.expires_after(interval);
    while (stop_chan_.is_open() && token_chan.is_open()) {
      co_await timer.async_wait();
      node_handle_->tick();
      co_await token_chan.async_send(asio::error_code{});
      timer.expires_after(interval);
    }
    co_return;
  }

  asio::awaitable<void> listen_ready(lepton::signal_channel &token_chan) {
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
    while (stop_chan_.is_open() && token_chan.is_open()) {
      auto rd_handle_result = co_await node_handle_->wait_ready(executor_);
      if (!rd_handle_result) {
        SPDLOG_ERROR("node {} wait_ready failed: {}", id_, rd_handle_result.error().message());
        co_return;
      }
      assert(rd_handle_result);
      auto rd_handle = rd_handle_result.value();
      auto &rd = *rd_handle.get();
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
        co_spawn(executor_, send_msg(executor_, iface_, id_, msg, wait_ms), asio::detached);
      }
      co_await node_handle_->advance();
      std::error_code ec;
      co_await token_chan.async_send(asio::error_code{}, asio::redirect_error(asio::use_awaitable, ec));
      if (ec) {
        SPDLOG_DEBUG("Failed to send token: {}", ec.message());
        break;
      }
    }
    co_return;
  }

  asio::awaitable<void> listen_recv(lepton::signal_channel &token_chan, async_mutex &m) {
    auto async_step_func = [this](raftpb::message &&msg) -> asio::awaitable<void> {
      co_await node_handle_->step(std::move(msg));
      co_return;
    };
    while (stop_chan_.is_open() && token_chan.is_open()) {
      co_await m.async_lock(asio::use_awaitable);
      auto msg = co_await iface_->recv()->async_receive();
      m.unlock();
      co_spawn(executor_, async_step_func(std::move(msg)), asio::detached);
      co_await token_chan.async_send(asio::error_code{});
    }
    co_return;
  }

  asio::awaitable<void> listen_stop(lepton::signal_channel &token_chan) {
    co_await stop_chan_.async_receive();
    token_chan.close();
    pause_chan_.close();
    co_await node_handle_->stop();
    co_await stop_chan_.async_send(asio::error_code{});
    co_return;
  }

  asio::awaitable<void> listen_pause(lepton::signal_channel &token_chan, async_mutex &m) {
    auto recv_func = [this](bool &pause, lepton::pb::repeated_message &recvms) -> asio::awaitable<void> {
      while (pause) {
        auto msg = co_await iface_->recv()->async_receive();
        recvms.Add(std::move(msg));
      }
      co_return;
    };
    auto listen_pause = [this](bool &pause) -> asio::awaitable<void> {
      while (pause) {
        pause = co_await pause_chan_.async_receive();
      }
      co_return;
    };
    while (!stop_source_.stop_requested() && stop_chan_.is_open() && token_chan.is_open()) {
      lepton::pb::repeated_message recvms;
      auto p = co_await pause_chan_.async_receive();
      co_await m.async_lock(asio::use_awaitable);
      co_spawn(executor_, recv_func(p, recvms), asio::detached);
      co_spawn(executor_, listen_pause(p), asio::use_future);
      m.unlock();
      // step all pending messages
      for (auto &msg : recvms) {
        co_await node_handle_->step(std::move(msg));
      }
    }
    co_return;
  }

 public:
  asio::any_io_executor executor_;
  std::stop_source stop_source_;

  lepton::node_proxy node_handle_;
  std::uint64_t id_;
  std::shared_ptr<rafttest::iface> iface_;
  lepton::signal_channel stop_chan_;
  lepton::channel<bool> pause_chan_;

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
