#include <gtest/gtest.h>

#include <asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/io_context.hpp>
#include <asio/use_awaitable.hpp>
#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <system_error>

#include "asio/cancellation_signal.hpp"
#include "asio/detached.hpp"
#include "asio/error_code.hpp"
#include "asio/use_future.hpp"
#include "channel.h"
#include "channel_endpoint.h"
#include "expected.h"
#include "raft.pb.h"
#include "raft_error.h"
#include "signal_channel_endpoint.h"
#include "spdlog/spdlog.h"
#include "storage_error.h"
#include "tl/expected.hpp"
using asio::awaitable;
using asio::co_spawn;
using asio::detached;
using asio::io_context;
using asio::steady_timer;
using asio::use_awaitable;
using asio::experimental::make_parallel_group;

struct RaftMessage {
  std::string content;
};

class RaftNode {
 public:
  RaftNode(io_context& io_context) : io_context_(io_context), stop_flag_(false), timer_(io_context) {}

  // 启动协程事件循环
  void start() {
    co_spawn(io_context_, [this] { return event_loop(); }, detached);
  }

  void stop() { stop_flag_ = true; }

  awaitable<void> propose(const RaftMessage& message) {
    std::cout << "Proposing message: " << message.content << std::endl;
    co_return;
  }

  awaitable<void> propose_conf_change(const std::string& config) {
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
  io_context& io_context_;
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
awaitable<lepton::expected<void>> async_select(lepton::channel<raftpb::message>& recvc, raftpb::message m,
                                               lepton::signal_channel& done_chan) {
  // 同时发起异步操作
  auto [order, ec1, ec2] = co_await asio::experimental::make_parallel_group(
                               // 尝试发送消息
                               [&](auto token) {
                                 std::cout << "Attempting to send message..." << std::endl;
                                 return recvc.async_send(asio::error_code{}, m, token);
                                 std::cout << "Message sent successfully." << std::endl;
                               },
                               // 等待节点停止
                               [&](auto token) { return done_chan.async_receive(token); })
                               .async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);

  // 根据第一个完成的操作返回结果
  switch (order[0]) {  // 查看哪个操作先完成
    case 0:            // 发送成功
      co_return lepton::expected<void>{};
    case 1:  // 节点停止
      co_return tl::unexpected{lepton::raft_error::STOPPED};
    default:
      co_return tl::unexpected{lepton::raft_error::UNKNOWN_ERROR};
  }
}

TEST(asio_test_suit, async_select_done_chan) {
  // 初始化组件
  asio::io_context io;
  lepton::channel<raftpb::message> recvc(io.get_executor());  // 带缓冲的通道
  lepton::signal_channel done_chan(io.get_executor());        // 用于取消的信号通道
  asio::steady_timer ctx_done(io);

  // 触发取消（测试用）
  // ctx_done.expires_after(std::chrono::milliseconds(100));  // 100ms后超时

  // 执行选择操作

  co_spawn(
      io,
      [&]() -> awaitable<void> {
        auto ec = co_await async_select(recvc, raftpb::message{}, done_chan);
        if (ec.has_value()) {
          std::cout << "发送成功\n";
        } else if (ec.error() == lepton::make_error_code(lepton::raft_error::STOPPED)) {
          std::cout << "上下文取消\n";
        } else if (ec.error() == lepton::make_error_code(lepton::raft_error::UNKNOWN_ERROR)) {
          std::cout << "未知错误\n";
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

TEST(asio_test_suit, async_select_done) {
  // 初始化组件
  asio::io_context io;
  lepton::channel<raftpb::message> recvc(io.get_executor());  // 带缓冲的通道
  lepton::signal_channel done_chan(io.get_executor());        // 用于取消的信号通道
  asio::steady_timer ctx_done(io);

  // 执行选择操作
  co_spawn(
      io,
      [&]() -> awaitable<void> {
        auto ec = co_await lepton::async_select_done(
            [&](auto token) {
              std::cout << "Attempting to send message..." << std::endl;
              return recvc.async_send(asio::error_code{}, raftpb::message{}, token);
              std::cout << "Message sent successfully." << std::endl;
            },
            done_chan);
        if (ec.has_value()) {
          std::cout << "发送成功\n";
        } else if (ec.error() == lepton::make_error_code(lepton::raft_error::STOPPED)) {
          std::cout << "上下文取消\n";
        } else if (ec.error() == lepton::make_error_code(lepton::raft_error::UNKNOWN_ERROR)) {
          std::cout << "未知错误\n";
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

TEST(asio_test_suit, async_select_any_expected_async_send) {
  // 初始化组件
  asio::io_context io;
  lepton::channel<raftpb::message> recvc(io.get_executor());  // 带缓冲的通道
  lepton::signal_channel done_chan(io.get_executor());        // 用于取消的信号通道

  // 执行选择操作
  co_spawn(
      io,
      [&]() -> awaitable<void> {
        auto ec = co_await lepton::async_select_any_expected(
            [&](auto token) {
              std::cout << "Attempting to send message..." << std::endl;
              return recvc.async_send(asio::error_code{}, raftpb::message{}, token);
              std::cout << "Message sent successfully." << std::endl;
            },
            [&](auto token) { return done_chan.async_receive(token); });
        if (ec.has_value()) {
          std::cout << "发送成功, idx: " << ec.value() << std::endl;
        } else if (ec.error() == lepton::make_error_code(lepton::raft_error::STOPPED)) {
          std::cout << "上下文取消\n";
        } else if (ec.error() == lepton::make_error_code(lepton::raft_error::UNKNOWN_ERROR)) {
          std::cout << "未知错误\n";
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

TEST(asio_test_suit, async_select_done_with_raft_message_type) {
  // 初始化组件
  asio::io_context io;
  lepton::channel<raftpb::message> recvc(io.get_executor());  // 带缓冲的通道
  lepton::signal_channel done_chan(io.get_executor());        // 用于取消的信号通道

  // 执行选择操作
  co_spawn(
      io,
      [&]() -> awaitable<void> {
        auto msg = co_await lepton::async_select_done_with_value(
            [&](auto token) {
              std::cout << "Attempting to recv message..." << std::endl;
              return recvc.async_receive(token);
              std::cout << "Message recv successfully." << std::endl;
            },
            done_chan);
        if (msg.has_value()) {
          std::cout << "发送成功 " << msg.value().DebugString() << std::endl;
        } else if (msg.error() == lepton::make_error_code(lepton::raft_error::STOPPED)) {
          std::cout << "上下文取消\n";
        } else if (msg.error() == lepton::make_error_code(lepton::raft_error::UNKNOWN_ERROR)) {
          std::cout << "未知错误\n";
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
    SPDLOG_INFO("ready to send msg heartbeat");
    raftpb::message msg;
    msg.set_type(raftpb::message_type::MSG_HEARTBEAT);
    recvc.try_send(asio::error_code{}, std::move(msg));
    SPDLOG_INFO("has ready send msg heartbeat");
  });

  io.run();
}

TEST(asio_test_suit, async_select_done_with_expected_error_type) {
  // 初始化组件
  asio::io_context io;
  lepton::channel<std::error_code> recvc(io.get_executor());  // 带缓冲的通道
  lepton::signal_channel done_chan(io.get_executor());        // 用于取消的信号通道

  // 执行选择操作
  co_spawn(
      io,
      [&]() -> awaitable<void> {
        auto msg = co_await lepton::async_select_done_with_value(
            [&](auto token) {
              std::cout << "Attempting to recv message..." << std::endl;
              return recvc.async_receive(token);
              std::cout << "Message recv successfully." << std::endl;
            },
            done_chan);
        if (msg.has_value()) {
          std::cout << "发送成功 " << msg->value() << std::endl;
        } else if (msg.error() == lepton::make_error_code(lepton::raft_error::STOPPED)) {
          std::cout << "上下文取消\n";
        } else if (msg.error() == lepton::make_error_code(lepton::raft_error::UNKNOWN_ERROR)) {
          std::cout << "未知错误\n";
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
    SPDLOG_INFO("ready to send mock error info");
    recvc.try_send(asio::error_code{}, lepton::storage_error::COMPACTED);
    SPDLOG_INFO("has ready send mock error info");
  });

  io.run();
}

TEST(asio_test_suit, close_channel) {
  // 初始化组件
  asio::io_context io;
  lepton::channel<std::error_code> recvc1(io.get_executor());  // 带缓冲的通道
  lepton::channel<std::error_code> recvc2(io.get_executor());  // 带缓冲的通道
  lepton::signal_channel done_chan(io.get_executor());         // 用于取消的信号通道

  // 执行选择操作
  co_spawn(
      io,
      [&]() -> awaitable<void> {
        auto msg = co_await lepton::async_select_done_with_value(
            [&](auto token) {
              std::cout << "Attempting to recv message..." << std::endl;
              return recvc1.async_receive(token);
              std::cout << "Message recv successfully." << std::endl;
            },
            done_chan);
        if (msg) {
          std::cout << "发送成功 " << msg->value() << std::endl;
        } else if (msg.error() == lepton::raft_error::STOPPED) {
          std::cout << "上下文取消\n";
        } else if (msg.error() == lepton::make_error_code(lepton::raft_error::UNKNOWN_ERROR)) {
          std::cout << "未知错误\n";
        }
        co_return;
      },
      asio::detached);

  // 执行选择操作
  co_spawn(
      io,
      [&]() -> awaitable<void> {
        auto msg = co_await lepton::async_select_done_with_value(
            [&](auto token) {
              std::cout << "Attempting to recv message..." << std::endl;
              return recvc2.async_receive(token);
              std::cout << "Message recv successfully." << std::endl;
            },
            done_chan);
        if (msg) {
          std::cout << "发送成功 " << msg->value() << std::endl;
        } else if (msg.error() == lepton::make_error_code(lepton::raft_error::STOPPED)) {
          std::cout << "上下文取消\n";
        } else if (msg.error() == lepton::make_error_code(lepton::raft_error::UNKNOWN_ERROR)) {
          std::cout << "未知错误\n";
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

  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        SPDLOG_INFO("ready to send mock error info");
        done_chan.close();
        SPDLOG_INFO("has ready send mock error info");
        co_return;
      },
      asio::use_future);
  io.run();
}

TEST(asio_test_suit, cancellation_signal) {
  asio::io_context io;
  asio::cancellation_signal cancel_sig;

  // 创建两个容量为0的channel（确保发送操作会阻塞）
  lepton::channel_endpoint<std::string> ch1(io.get_executor());
  lepton::channel_endpoint<std::string> ch2(io.get_executor());

  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        auto msg = co_await ch1.async_receive();
        if (msg.has_value()) {
          std::cout << "ch1 发送成功 " << msg.value() << std::endl;
        } else if (msg.error() == lepton::make_error_code(lepton::raft_error::STOPPED)) {
          std::cout << "ch1 上下文取消\n";
        } else {
          std::cout << "ch1 未知错误\n";
        }
        co_return;
      },
      asio::detached);

  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        auto msg = co_await ch2.async_send("value");
        if (msg.has_value()) {
          std::cout << "ch2 发送成功 " << std::endl;
        } else if (msg.error() == lepton::make_error_code(lepton::raft_error::STOPPED)) {
          std::cout << "ch2 上下文取消\n";
        } else {
          std::cout << "ch2 未知错误\n";
        }
        co_return;
      },
      asio::detached);

  // 启动协程设置取消定时器
  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        // 等待1秒后取消操作
        asio::steady_timer timer(io, std::chrono::seconds(1));
        co_await timer.async_wait(asio::use_awaitable);
        SPDLOG_INFO("wait timeout and ready cancel channel....");
        ch1.close();
        ch2.close();
        SPDLOG_INFO("cancel channel successful");
        co_return;
      },
      asio::detached);

  // 运行IO上下文
  io.run();
}

// asio make_parallel_group 与 channel
// 结合一起使用时，如果其中一个channel收到消息时，会取消其他channel；导致并行发送的消息会丢失
TEST(asio_test_suit, parallel_send_msg) {
  asio::io_context io;
  lepton::channel<std::string> recvc1(io.get_executor());  // 带缓冲的通道
  lepton::channel<std::string> recvc2(io.get_executor());  // 带缓冲的通道

  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        auto group = asio::experimental::make_parallel_group([&](auto token) { return recvc1.async_receive(token); },
                                                             [&](auto token) { return recvc2.async_receive(token); });
        for (auto times = 0; times < 2; ++times) {
          SPDLOG_INFO("running test main loop times: {}", times);
          auto [order, ec1, result1, ec2, result2] =
              co_await group.async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);

          switch (order[0]) {
            case 0:
              SPDLOG_INFO("receive msg from channel 1, {}", result1);
              break;
            case 1:
              SPDLOG_INFO("receive msg from channel 2, {}", result2);
              break;
          }
        }
        co_return;
      },
      asio::detached);

  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        SPDLOG_INFO("ready to send msg by channel 1");
        co_await recvc1.async_send(asio::error_code{}, "测试 channel 1");
        SPDLOG_INFO("send msg by channel 1 successful");
        co_return;
      },
      asio::detached);

  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        SPDLOG_INFO("ready to send msg by channel 2");
        co_await recvc2.async_send(asio::error_code{}, "测试 channel 2");
        SPDLOG_INFO("send msg by channel 2 successful");
        co_return;
      },
      asio::detached);

  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        // 等待1秒后取消操作
        asio::steady_timer timer(io, std::chrono::seconds(1));
        co_await timer.async_wait(asio::use_awaitable);
        SPDLOG_INFO("wait timeout and ready cancel channel....");
        recvc1.close();
        recvc2.close();
        SPDLOG_INFO("cancel channel successful");
        co_return;
      },
      asio::detached);

  io.run();
}

asio::awaitable<void> parallel_process(lepton::channel<std::string>& recvc, lepton::signal_channel& token_chan) {
  while (recvc.is_open() && token_chan.is_open()) {
    auto msg = co_await recvc.async_receive();
    SPDLOG_INFO("receive msg from channel, {}", msg);
    co_await token_chan.async_send(asio::error_code{});
  }
}

TEST(asio_test_suit, lepton_parallel_send_msg) {
  asio::io_context io;
  lepton::channel<std::string> recvc1(io.get_executor());
  lepton::channel<std::string> recvc2(io.get_executor());
  lepton::signal_channel done_chan(io.get_executor());
  lepton::signal_channel token_chan(io.get_executor());

  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        asio::co_spawn(io.get_executor(), parallel_process(recvc1, token_chan), asio::detached);
        asio::co_spawn(io.get_executor(), parallel_process(recvc2, token_chan), asio::detached);
        while (done_chan.is_open()) {
          co_await token_chan.async_receive();
        }
        co_return;
      },
      asio::detached);

  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        SPDLOG_INFO("ready to send msg by channel 1");
        co_await recvc1.async_send(asio::error_code{}, "测试 channel 1");
        SPDLOG_INFO("send msg by channel 1 successful");
        co_return;
      },
      asio::detached);

  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        SPDLOG_INFO("ready to send msg by channel 2");
        co_await recvc2.async_send(asio::error_code{}, "测试 channel 2");
        SPDLOG_INFO("send msg by channel 2 successful");
        co_return;
      },
      asio::detached);

  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        // 等待1秒后取消操作
        asio::steady_timer timer(io, std::chrono::seconds(1));
        co_await timer.async_wait(asio::use_awaitable);
        SPDLOG_INFO("wait timeout and ready cancel channel....");
        recvc1.close();
        recvc2.close();
        done_chan.close();
        token_chan.close();
        SPDLOG_INFO("cancel channel successful");
        co_return;
      },
      asio::detached);

  io.run();
}