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
#include <utility>
#include <vector>

#include "conf_change.h"
#include "config.h"
#include "describe.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "joint.h"
#include "lepton_error.h"
#include "majority.h"
#include "memory_storage.h"
#include "node.h"
#include "protobuf.h"
#include "raft.h"
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
          EXPECT_TRUE(result);
          co_return;
        },
        asio::use_future);

    auto expected_fut = asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
          if (msg_type == raftpb::message_type::MSG_PROP) {
            // Propose message should be sent to propc chan
            auto msg_result = n.prop_chan_.try_receive(
                [&](asio::error_code _, msg_with_result msg_result) { EXPECT_EQ(msg_type, msg_result.msg.type()); });
            EXPECT_EQ(msg_result, raftpb::message_type::MSG_PROP);
          } else {
            auto msg_result = n.recv_chan_.try_receive(
                [&](asio::error_code _, raftpb::message msg) { EXPECT_EQ(msg_type, msg.type()); });
            if (lepton::pb::is_local_msg(msg_type)) {
              EXPECT_FALSE(msg_result);
            } else {
              EXPECT_TRUE(msg_result);  // 非本地消息应该收到
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

TEST_F(node_test_suit, test_node_step1) {
  using namespace std::chrono_literals;

  for (std::size_t i = 0; i < all_raftpb_message_types.size(); ++i) {
    asio::io_context io;
    auto msg_type = all_raftpb_message_types[i];
    raftpb::message msg;
    msg.set_type(msg_type);

    auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
    pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
    auto raw_node_result = lepton::new_raw_node(new_test_config(1, 3, 1, std::move(storage_proxy)));
    ASSERT_TRUE(raw_node_result);
    auto& raw_node = *raw_node_result;

    lepton::node n(io.get_executor(), std::move(raw_node));

    // step 协程
    auto fut = asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
          auto result = co_await n.step(std::move(msg));
          EXPECT_TRUE(result);
          co_return;
        },
        asio::use_future);

    std::optional<raftpb::message_type> received_type;

    // 接收 channel 消息协程
    auto receive_fut = asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
          if (msg_type == raftpb::message_type::MSG_PROP) {
            auto result = co_await n.prop_chan_.async_receive(asio::use_awaitable);
            received_type = result.msg.type();
          } else if (!lepton::pb::is_local_msg(msg_type)) {
            auto result = co_await n.recv_chan_.async_receive(asio::use_awaitable);
            received_type = result.type();
          }
          co_return;
        },
        asio::use_future);

    io.restart();
    io.run();

    fut.get();
    auto status = receive_fut.wait_for(50ms);  // 最多等待 50ms 防挂起

    if (msg_type == raftpb::message_type::MSG_PROP) {
      ASSERT_EQ(status, std::future_status::ready);
      ASSERT_TRUE(received_type.has_value());
      EXPECT_EQ(*received_type, raftpb::message_type::MSG_PROP);
    } else if (lepton::pb::is_local_msg(msg_type)) {
      EXPECT_EQ(status, std::future_status::timeout);  // 应该没收到
    } else {
      ASSERT_EQ(status, std::future_status::ready);
      ASSERT_TRUE(received_type.has_value());
      EXPECT_EQ(*received_type, msg_type);
    }
  }
}
