#include <gtest/gtest.h>
#include <proxy.h>
#include <raft.pb.h>

#include <cmath>
#include <cstddef>
#include <cstdio>
#include <string>
#include <vector>

#include "fmt/base.h"
#include "fmt/format.h"
#include "raft.h"
#include "storage.h"
#include "test_diff.h"
#include "test_raft_utils.h"
using namespace lepton;
using namespace lepton::core;

class raft_flow_control_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

// TestMsgAppFlowControlFull ensures:
// 1. msgApp can fill the sending window until full
// 2. when the window is full, no more msgApp can be sent.
TEST_F(raft_flow_control_test_suit, test_msg_app_flow_control_full) {
  auto r = new_test_raft(1, 5, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2})}})));
  r.become_candidate();
  r.become_leader();

  auto &pr2 = r.trk_.progress_map_mutable_view().mutable_view().at(2);
  // force the progress to be in replicate state
  pr2.become_replicate();
  // fill in the inflights window
  for (std::size_t i = 0; i < r.trk_.max_inflight(); ++i) {
    r.step(new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "somedata"));
    auto ms = r.read_messages();
    ASSERT_EQ(1, ms.size());
    ASSERT_EQ(raftpb::message_type::MSG_APP, ms[0].type()) << "msg type should be MSG_APP";
  }

  // ensure 1
  ASSERT_TRUE(pr2.is_paused());

  // ensure 2
  for (std::size_t i = 0; i < 10; ++i) {
    r.step(new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "somedata"));
    auto ms = r.read_messages();
    ASSERT_EQ(0, ms.size());
  }
}

// TestMsgAppFlowControlMoveForward ensures msgAppResp can move
// forward the sending window correctly:
// 1. valid msgAppResp.index moves the windows to pass all smaller or equal index.
// 2. out-of-dated msgAppResp has no effect on the sliding window.
TEST_F(raft_flow_control_test_suit, test_msg_app_flow_control_move_forward) {
  auto r = new_test_raft(1, 5, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2})}})));
  r.become_candidate();
  r.become_leader();

  auto &pr2 = r.trk_.progress_map_mutable_view().mutable_view().at(2);
  // force the progress to be in replicate state
  pr2.become_replicate();

  // fill in the inflights window
  for (std::size_t i = 0; i < r.trk_.max_inflight(); ++i) {
    r.step(new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "somedata"));
    auto ms = r.read_messages();
    ASSERT_EQ(1, ms.size());
    ASSERT_EQ(raftpb::message_type::MSG_APP, ms[0].type()) << "msg type should be MSG_APP";
  }

  // 1 is noop, 2 is the first proposal we just sent.
  // so we start with 2.
  for (std::size_t tt = 2; tt < r.trk_.max_inflight(); ++tt) {
    // move forward the window
    auto msg = new_pb_message(2, 1, raftpb::message_type::MSG_APP_RESP);
    msg.set_index(tt);
    r.step(raftpb::message{msg});
    r.read_messages();
    ASSERT_FALSE(pr2.is_paused()) << fmt::format("pr2 should not be paused at tt: {}", tt);

    // fill in the inflights window again
    r.step(new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "somedata"));
    auto ms = r.read_messages();
    ASSERT_EQ(1, ms.size());
    ASSERT_EQ(raftpb::message_type::MSG_APP, ms[0].type()) << "msg type should be MSG_APP";

    // ensure 1
    ASSERT_TRUE(pr2.is_paused());

    // ensure 2
    for (std::size_t i = 0; i < tt; ++i) {
      msg.set_index(i);
      r.step(raftpb::message{msg});
      ASSERT_TRUE(pr2.is_paused());
    }
  }
}

// TestMsgAppFlowControlRecvHeartbeat ensures a heartbeat response
// frees one slot if the window is full.
TEST_F(raft_flow_control_test_suit, test_msg_app_flow_control_recv_heartbeat) {
  auto r = new_test_raft(1, 5, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2})}})));
  r.become_candidate();
  r.become_leader();

  auto &pr2 = r.trk_.progress_map_mutable_view().mutable_view().at(2);
  // force the progress to be in replicate state
  pr2.become_replicate();

  // fill in the inflights window
  for (std::size_t i = 0; i < r.trk_.max_inflight(); ++i) {
    r.step(new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "somedata"));
    auto ms = r.read_messages();
    ASSERT_EQ(1, ms.size());
    ASSERT_EQ(raftpb::message_type::MSG_APP, ms[0].type()) << "msg type should be MSG_APP";
  }

  for (std::size_t tt = 1; tt < 5; ++tt) {
    // recv tt msgHeartbeatResp and expect one free slot
    for (std::size_t i = 0; i < tt; ++i) {
      ASSERT_TRUE(pr2.is_paused());
      // Unpauses the progress, sends an empty MsgApp, and pauses it again.
      r.step(new_pb_message(2, 1, raftpb::message_type::MSG_HEARTBEAT_RESP));
      auto ms = r.read_messages();
      ASSERT_EQ(1, ms.size()) << fmt::format("#{}.{}", tt, i);
      ASSERT_EQ(raftpb::message_type::MSG_APP, ms[0].type()) << "msg type should be MSG_APP";
      ASSERT_TRUE(ms[0].entries().empty()) << "msg app should be empty";
    }

    // No more appends are sent if there are no heartbeats.
    for (std::size_t i = 0; i < 10; ++i) {
      ASSERT_TRUE(pr2.is_paused());
      r.step(new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "somedata"));
      auto ms = r.read_messages();
      ASSERT_TRUE(ms.empty());
    }

    // clear all pending messages.
    r.step(new_pb_message(2, 1, raftpb::message_type::MSG_HEARTBEAT_RESP));
    r.read_messages();
  }
}