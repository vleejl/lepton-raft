#pragma once

#include <asio/error_code.hpp>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>

#include "channel.h"
#include "raft.pb.h"
#include "spdlog/spdlog.h"

namespace rafttest {

using channel_msg = lepton::channel<raftpb::message>;

using channel_msg_handle = lepton::channel<raftpb::message>*;

// 基础网络接口 (等价于 Go iface)
struct iface {
  virtual void send(const raftpb::message& m) = 0;
  virtual channel_msg_handle recv() = 0;
  virtual void disconnect() = 0;
  virtual void close() = 0;
  virtual void connect() = 0;
  virtual ~iface() = default;
};

struct conn {
  uint64_t from;
  uint64_t to;

  bool operator<(const conn& other) const { return std::tie(from, to) < std::tie(other.from, other.to); }
};

struct delay {
  std::chrono::milliseconds d{};
  double rate = 0.0;
};

// 模拟网络
class raft_network {
 public:
  explicit raft_network(asio::any_io_executor executor, const std::vector<uint64_t>& nodes) : rng_(1) {
    for (auto n : nodes) {
      recv_queues_[n] = std::make_unique<channel_msg>(executor, 1024);
      disconnected_[n] = false;
    }
  }

  void send(const raftpb::message& m) {
    channel_msg_handle to = nullptr;
    bool disconnected = false;
    double drop = 0.0;
    struct delay dl;

    {
      std::unique_lock lk(mu_);
      auto it = recv_queues_.find(m.to());
      if (it != recv_queues_.end()) {
        to = it->second.get();
      }
      disconnected = disconnected_[m.to()];
      drop = dropmap_[{m.from(), m.to()}];
      dl = delaymap_[{m.from(), m.to()}];
    }

    if (!to || disconnected) return;

    // 模拟丢包
    if (drop != 0.0 && dist_(rng_) < drop) return;

    // 模拟延迟
    if (dl.d.count() != 0 && dist_(rng_) < dl.rate) {
      auto rd = std::uniform_int_distribution<int64_t>(0, dl.d.count())(rng_);
      std::this_thread::sleep_for(std::chrono::milliseconds(rd));
    }

    // protobuf 拷贝 (避免数据竞争)
    std::string buf;
    if (!m.SerializeToString(&buf)) {
      throw std::runtime_error("failed to serialize message");
    }

    raftpb::message cm;
    if (!cm.ParseFromString(buf)) {
      throw std::runtime_error("failed to parse message");
    }
    auto debug_str = cm.DebugString();
    if (auto result = to->try_send(asio::error_code{}, std::move(cm)); !result) {
      SPDLOG_DEBUG("send msg:{} failed", debug_str);
    } else {
      SPDLOG_INFO("send msg:{} successful", debug_str);
    }
  }

  channel_msg_handle recv_from(uint64_t from) {
    std::unique_lock lk(mu_);
    if (disconnected_[from]) {
      return nullptr;
    }
    auto it = recv_queues_.find(from);
    if (it != recv_queues_.end()) {
      return it->second.get();
    }
    assert(false);
    return nullptr;
  }

  void close(uint64_t from) {
    std::unique_lock lk(mu_);
    auto it = recv_queues_.find(from);
    if (it != recv_queues_.end()) {
      it->second.get()->close();
    }
  }

  void drop(uint64_t from, uint64_t to, double rate) {
    std::unique_lock lk(mu_);
    dropmap_[{from, to}] = rate;
  }

  void delay(uint64_t from, uint64_t to, std::chrono::milliseconds d, double rate) {
    std::unique_lock lk(mu_);
    delaymap_[{from, to}] = {d, rate};
  }

  void disconnect(uint64_t id) {
    std::unique_lock lk(mu_);
    disconnected_[id] = true;
  }

  void connect(uint64_t id) {
    std::unique_lock lk(mu_);
    disconnected_[id] = false;
  }

 private:
  std::mutex mu_;
  std::map<uint64_t, std::unique_ptr<channel_msg>> recv_queues_;
  std::map<uint64_t, bool> disconnected_;
  std::map<conn, double> dropmap_;
  std::map<conn, struct delay> delaymap_;

  std::mt19937 rng_;
  std::uniform_real_distribution<double> dist_{0.0, 1.0};
};

// 节点视角
class node_network : public iface {
 public:
  node_network(uint64_t id, raft_network* net) : id_(id), net_(net) {}

  void connect() override { net_->connect(id_); }
  void disconnect() override { net_->disconnect(id_); }
  void close() override { net_->close(id_); }
  void send(const raftpb::message& m) override { net_->send(m); }
  channel_msg_handle recv() override { return net_->recv_from(id_); }

 private:
  uint64_t id_;
  raft_network* net_;
};

}  // namespace rafttest
