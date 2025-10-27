#include <gtest/gtest.h>
#include <proxy.h>
#include <raft.pb.h>

#include <cmath>
#include <cstdio>
#include <vector>

#include "raft_network.h"
#include "test_diff.h"
#include "test_raft_utils.h"

using namespace lepton;

class raft_network_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

TEST_F(raft_network_test_suit, test_network_drop) {
  asio::io_context io_context;
  // drop around 10% messages
  auto sent = 1000;
  auto droprate = 0.1;
  auto nt = rafttest::raft_network{io_context.get_executor(), {1, 2}};
  nt.drop(1, 2, droprate);
  for (auto i = 0; i < sent; ++i) {
    nt.send(new_pb_message(1, 2, raftpb::message_type::MSG_APP));
  }

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto c = nt.recv_from(2);
        EXPECT_NE(c, nullptr);
        auto done = false;
        auto received = 0;
        while (!done) {
          if (!c->raw_channel().try_receive([&](asio::error_code _, raftpb::message msg) { received++; })) {
            done = true;
          }
        }

        auto drop = sent - received;
        EXPECT_LE(drop, static_cast<int>((droprate + 0.1) * static_cast<double>(sent)));
        EXPECT_GE(drop, static_cast<int>((droprate - 0.1) * static_cast<double>(sent)));
        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(raft_network_test_suit, test_network_delay) {
  asio::io_context io_context;
  auto sent = 1000;
  std::chrono::milliseconds delay{1};
  auto delayrate = 0.1;
  auto nt = rafttest::raft_network{io_context.get_executor(), {1, 2}};

  nt.delay(1, 2, delay, delayrate);
  std::chrono::nanoseconds total{0};
  for (auto i = 0; i < sent; ++i) {
    auto start = std::chrono::high_resolution_clock::now();
    nt.send(new_pb_message(1, 2, raftpb::message_type::MSG_APP));
    auto end = std::chrono::high_resolution_clock::now();
    total += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
  }

  // 计算逻辑
  double multiplier = static_cast<double>(sent) * delayrate / 2.0;

  // 结果计算（两种等效方式）
  // 方式1：直接计算纳秒
  auto w = std::chrono::duration_cast<std::chrono::nanoseconds>(multiplier * delay);
  ASSERT_GE(total, w);
}