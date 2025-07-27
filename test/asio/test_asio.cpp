#include <gtest/gtest.h>

#include <asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/io_context.hpp>
#include <asio/use_awaitable.hpp>
#include <atomic>
#include <chrono>
#include <iostream>
#include <system_error>

#include "asio/detached.hpp"
#include "asio/error_code.hpp"
#include "asio/use_future.hpp"
#include "channel.h"
#include "expected.h"
#include "raft.pb.h"
#include "raft_error.h"
#include "spdlog/spdlog.h"
using asio::awaitable;
using asio::co_spawn;
using asio::detached;
using asio::io_context;
using asio::steady_timer;
using asio::use_awaitable;
using asio::experimental::make_parallel_group;
;

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

TEST(asio_test_suit, asio_io_context) {
  io_context io;
  lepton::channel<int> proc_chan(io.get_executor());
  bool has_finish_async = false;
  auto mock_run = [&]() -> asio::awaitable<lepton::expected<void>> {
    asio::error_code ec;
    SPDLOG_INFO("ready send async send result info");
    co_await proc_chan.async_send(asio::error_code{}, 10, asio::redirect_error(asio::use_awaitable, ec));
    SPDLOG_INFO("finish async send result info {}", ec.value());
    has_finish_async = true;
    CO_CHECK_EXPECTED(ec);
    co_return lepton::expected<void>{};
  };
  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<lepton::expected<void>> {
        auto result = co_await mock_run();
        co_return result;
      },
      asio::detached);

  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        SPDLOG_INFO("event loop has started");
        co_return;
      },
      asio::detached);

  // 在下一轮事件循环中 stop()（此时 step() 应已挂起）
  asio::post(io, [&]() {
    SPDLOG_INFO("posting io.stop()");
    io.stop();
  });

  // 运行事件循环，协程会在这里执行
  SPDLOG_INFO("ready to start io_context");
  io.run();
  ASSERT_FALSE(has_finish_async);
}

TEST(asio_test_suit, asio_io_context1) {
  io_context io;
  lepton::channel<int> proc_chan(io.get_executor());

  // 用于模拟异步逻辑
  auto mock_run = [&proc_chan]() -> awaitable<lepton::expected<void>> {
    asio::error_code ec;
    SPDLOG_INFO("ready send async send result info");
    co_await proc_chan.async_send(asio::error_code{}, 10, redirect_error(use_awaitable, ec));
    SPDLOG_INFO("finish async send result info {}", ec.value());
    co_return ec ? tl::unexpected(ec) : lepton::expected<void>{};
  };

  // 启动发送协程
  co_spawn(
      io,
      [&]() -> awaitable<lepton::expected<void>> {
        auto result = co_await mock_run();
        co_return result;
      },
      detached);

  // 启动接收协程，确保 send 可以完成
  co_spawn(
      io,
      [&]() -> awaitable<void> {
        int val = co_await proc_chan.async_receive(use_awaitable);
        SPDLOG_INFO("recv {}", val);
        // 等所有操作完成后手动停止事件循环
        io.stop();
        co_return;
      },
      detached);

  // 再启动一个协程做日志记录
  co_spawn(
      io,
      [&]() -> awaitable<void> {
        SPDLOG_INFO("event loop has started");
        co_return;
      },
      detached);

  SPDLOG_INFO("ready to start io_context");
  io.run();  // run 到 stop 被调用才会返回
}

TEST(asio_test_suit, asio_io_context2) {
  io_context io;

  // channel容量1，int类型数据
  auto excutor = io.get_executor();
  lepton::channel<int> chan(excutor, 1);

  co_spawn(
      io,
      [&]() -> awaitable<void> {
        SPDLOG_INFO("Starting async_send");
        // 用 redirect_error 捕获错误码
        auto result = chan.try_send(asio::error_code{}, 42);
        SPDLOG_INFO("finished try send async_send finished, result = {}", result);
        co_return;
      },
      detached);

  // 立即停止io_context，触发取消
  asio::post(io, [&]() {
    SPDLOG_INFO("Calling io.stop()");
    io.stop();
  });

  SPDLOG_INFO("Starting io.run()");
  io.run();

  SPDLOG_INFO("io.run() returned");
}

// 等效操作函数
awaitable<asio::error_code> async_select(lepton::channel<raftpb::message> &recvc, raftpb::message m,
                                         asio::steady_timer &ctx_done, lepton::signal_channel &done_chan) {
  // 同时发起三个异步操作
  auto [order, ec1, ec2, ec3] = co_await asio::experimental::make_parallel_group(
                                    // 尝试发送消息
                                    [&](auto token) {
                                      std::cout << "Attempting to send message..." << std::endl;
                                      return recvc.async_send(asio::error_code{}, m, token);
                                      std::cout << "Message sent successfully." << std::endl;
                                    },
                                    // 等待上下文取消
                                    [&](auto token) { return ctx_done.async_wait(token); },
                                    // 等待节点停止
                                    [&](auto token) { return done_chan.async_receive(token); })
                                    .async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);

  // 根据第一个完成的操作返回结果
  switch (order[0]) {  // 查看哪个操作先完成
    case 0:            // 发送成功
      co_return asio::error_code{};
    case 1:  // 上下文取消
      co_return asio::error::operation_aborted;
    case 2:  // 节点停止
      co_return asio::error::connection_aborted;
    default:
      co_return asio::error::operation_aborted;
  }
}

TEST(asio_test_suit, asio_io_context3) {
  // 初始化组件
  asio::io_context io;
  lepton::channel<raftpb::message> recvc(io.get_executor());  // 带缓冲的通道
  lepton::signal_channel done_chan(io.get_executor());        // 用于取消的信号通道
  asio::steady_timer ctx_done(io);

  // 触发取消（测试用）
  ctx_done.expires_after(std::chrono::milliseconds(100));  // 100ms后超时

  // 执行选择操作

  co_spawn(
      io,
      [&]() -> awaitable<void> {
        auto ec = co_await async_select(recvc, raftpb::message{}, ctx_done, done_chan);
        if (!ec) {
          std::cout << "发送成功\n";
        } else if (ec == asio::error::operation_aborted) {
          std::cout << "上下文取消\n";
        } else if (ec == asio::error::connection_aborted) {
          std::cout << "节点停止\n";
        }
        co_return;
      },
      asio::detached);

  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        SPDLOG_INFO("event loop has started");
        co_return;
      },
      asio::detached);

  // 在下一轮事件循环中 stop()（此时 step() 应已挂起）
  asio::post(io, [&]() {
    SPDLOG_INFO("posting done");
    done_chan.try_send(asio::error_code{});  // 触发取消
  });

  io.run();
}