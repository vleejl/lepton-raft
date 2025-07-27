#include <gtest/gtest.h>
#include <proxy.h>
#include <raft.pb.h>
#include <spdlog/spdlog.h>

#include <cmath>
#include <cstddef>
#include <cstdio>
#include <optional>
#include <set>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "asio/detached.hpp"
#include "asio/error_code.hpp"
#include "asio/use_awaitable.hpp"
#include "channel.h"
#include "conf_change.h"
#include "config.h"
#include "describe.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "joint.h"
#include "leaf.hpp"
#include "lepton_error.h"
#include "magic_enum.hpp"
#include "majority.h"
#include "memory_storage.h"
#include "node.h"
#include "protobuf.h"
#include "raft.h"
#include "raft_error.h"
#include "raw_node.h"
#include "read_only.h"
#include "state.h"
#include "storage.h"
#include "test_diff.h"
#include "test_raft_protobuf.h"
#include "test_raft_utils.h"
#include "test_utility_data.h"
#include "tracker.h"
#include "types.h"
using namespace lepton;

class node_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

using asio::steady_timer;

template <typename T>
asio::awaitable<leaf::result<T>> receive_with_timeout(channel<void(asio::error_code, T)>& ch,
                                                      std::chrono::steady_clock::duration timeout) {
  auto exec = co_await asio::this_coro::executor;
  steady_timer timer(exec);
  timer.expires_after(timeout);

  std::optional<T> result;
  asio::error_code final_ec;
  bool done = false;

  co_await (ch.async_receive(
                [&](asio::error_code ec, T value) {
                  if (!done) {
                    final_ec = ec;
                    if (!ec) {
                      result = std::move(value);
                    }
                    done = true;
                  }
                },
                asio::use_awaitable) ||
            timer.async_wait(
                [&](asio::error_code ec) {
                  if (!done) {
                    final_ec = asio::error::timed_out;
                    done = true;
                  }
                },
                asio::use_awaitable));
  if (final_ec) {
    co_return result;
  }
  co_return leaf::new_error(final_ec);
}
// TestNodeStep ensures that node.Step sends msgProp to propc chan
// and other kinds of messages to recvc chan.
TEST_F(node_test_suit, test_node_step) {
  for (std::size_t i = 0; i < all_raftpb_message_types.size(); ++i) {
    asio::io_context io;
    auto msg_type = all_raftpb_message_types[i];
    raftpb::message msg;
    msg.set_type(msg_type);

    auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
    // auto& mm_storage = *mm_storage_ptr;
    pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
    auto raw_node_result = lepton::new_raw_node(new_test_config(1, 3, 1, std::move(storage_proxy)));
    ASSERT_TRUE(raw_node_result);
    auto& raw_node = *raw_node_result;
    lepton::node n(io.get_executor(), std::move(raw_node));
    // Proposal goes to proc chan. Others go to recvc chan.
    auto fut = asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
          auto result = co_await n.step(std::move(msg));
          EXPECT_TRUE(result) << "msg type: " << magic_enum::enum_name(msg_type);
          std::cout << "step finished, " << "msg type: " << magic_enum::enum_name(msg_type) << std::endl;
          co_return;
        },
        asio::use_future);

    auto expected_fut = asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
          if (msg_type == raftpb::message_type::MSG_PROP) {
            // Propose message should be sent to propc chan
            auto msg_result = n.prop_chan_.try_receive([&](asio::error_code _, msg_with_result msg_result) {
              EXPECT_EQ(msg_type, msg_result.msg.type()) << "msg type: " << magic_enum::enum_name(msg_type);
            });
            EXPECT_TRUE(msg_result) << "msg type: " << magic_enum::enum_name(msg_type);
          } else {
            auto msg_result = n.recv_chan_.try_receive([&](asio::error_code _, raftpb::message msg) {
              EXPECT_EQ(msg_type, msg.type()) << "msg type: " << magic_enum::enum_name(msg_type);
            });
            if (lepton::pb::is_local_msg(msg_type)) {
              EXPECT_FALSE(msg_result) << "msg type: " << magic_enum::enum_name(msg_type);
            } else {
              EXPECT_TRUE(msg_result) << "msg type: " << magic_enum::enum_name(msg_type);  // 非本地消息应该收到
            }
          }
          co_return;
        },
        asio::use_future);
    io.run();
    fut.get();
    expected_fut.get();
  }
}

// TestNodeStepUnblock should Cancel and Stop should unblock Step()
TEST_F(node_test_suit, test_node_step_unblock) {
  // 两个子测试
  struct test_case {
    std::function<void(lepton::node& n, asio::io_context& io)> unblock;
    std::error_code expected_error;
  };

  std::vector<test_case> tests;
  // tests.push_back(test_case{[&](lepton::node& n, asio::io_context& io) {  // 用一个协程作为 "run 已启动" 的标记
  //                             asio::co_spawn(
  //                                 io,
  //                                 [&]() -> asio::awaitable<void> {
  //                                   SPDLOG_INFO("event loop has started");
  //                                   co_return;
  //                                 },
  //                                 asio::detached);

  //                             // 在下一轮事件循环中 stop()（此时 step() 应已挂起）
  //                             asio::post(io, [&]() {
  //                               SPDLOG_INFO("close done channel");
  //                               n.done_chan_.close();
  //                             });
  //                           },
  //                           lepton::make_error_code(raft_error::STOPPED)});
  tests.push_back(test_case{[&](lepton::node& _, asio::io_context& io) {
                              // 用一个协程作为 "run 已启动" 的标记
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
                            },
                            lepton::make_error_code(raft_error::STOPPED)});

  for (size_t i = 0; i < tests.size(); ++i) {
    asio::io_context io;

    auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
    pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
    auto raw_node_result = lepton::new_raw_node(new_test_config(1, 3, 1, std::move(storage_proxy)));
    ASSERT_TRUE(raw_node_result);
    auto& raw_node = *raw_node_result;

    // 创建 node（未运行主 loop）
    lepton::node n(io.get_executor(), std::move(raw_node));

    const auto& tt = tests[i];

    channel<expected<void>> err_chan(io.get_executor());

    // 启动协程调用 step（它应该阻塞）
    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
          raftpb::message msg;
          msg.set_type(raftpb::message_type::MSG_PROP);
          auto step_result = co_await n.step(std::move(msg));
          SPDLOG_INFO("step result has received");
          EXPECT_FALSE(step_result);
          EXPECT_EQ(tt.expected_error, step_result.error());
          co_await err_chan.async_send(asio::error_code{}, step_result, asio::use_awaitable);
          SPDLOG_INFO("finish async send error code info");
          co_return;
        },
        asio::detached);

    // 触发 unblock 操作
    tt.unblock(n, io);

    auto expected_fut = asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
          auto step_result = co_await err_chan.async_receive(asio::use_awaitable);
          EXPECT_EQ(tt.expected_error, step_result.error());
          n.stop_chan_.try_receive([&](asio::error_code ec) { EXPECT_EQ(asio::error_code{}, ec); });
          co_return;
        },
        asio::use_future);

    io.run();
    expected_fut.get();
  }
}
