#include <gtest/gtest.h>

#include <asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/io_context.hpp>
#include <asio/use_awaitable.hpp>
#include <atomic>
#include <chrono>
#include <iostream>
using asio::awaitable;
using asio::co_spawn;
using asio::detached;
using asio::io_context;
using asio::steady_timer;
using asio::use_awaitable;

struct RaftMessage {
  std::string content;
};

class RaftNode {
 public:
  RaftNode(io_context &io_context) : io_context_(io_context), stop_flag_(false), timer_(io_context) {}

  // 启动协程事件循环
  void start() {
    co_spawn(io_context_, [this] { return event_loop(); }, detached);
  }

  void stop() { stop_flag_ = true; }

  awaitable<void> propose(const RaftMessage &message) {
    std::cout << "Proposing message: " << message.content << std::endl;
    co_return;
  }

  awaitable<void> propose_conf_change(const std::string &config) {
    std::cout << "Proposing config change: " << config << std::endl;
    co_return;
  }

  awaitable<void> handle_ready() {
    std::cout << "Handling Raft ready state." << std::endl;
    co_return;
  }

 private:
  awaitable<void> event_loop() {
    while (!stop_flag_) {
      // 每100ms执行一次 Raft tick 操作
      timer_.expires_after(std::chrono::milliseconds(100));  // 设置定时器
      co_await timer_.async_wait(use_awaitable);             // 等待定时器到期

      std::cout << "RaftNode ticking..." << std::endl;

      // 在协程中处理 Raft 提案和配置更改
      RaftMessage message{"Proposal 1"};
      co_await propose(message);

      co_await propose_conf_change("Add node A");

      // 处理 Raft 条目
      co_await handle_ready();
    }
  }

 private:
  io_context &io_context_;
  std::atomic<bool> stop_flag_;
  steady_timer timer_;  // 定时器对象
};

// TEST(test_event_driven, asio_io_context) {
//   io_context io_context;
//   RaftNode raft_node(io_context);

//   raft_node.start();

//   // 运行事件循环，协程会在这里执行
//   io_context.run();
// }