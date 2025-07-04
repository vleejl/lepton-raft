#include <gtest/gtest.h>
#include <proxy.h>
#include <raft.pb.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdio>
#include <functional>
#include <memory>
#include <source_location>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/types/span.h"
#include "conf_change.h"
#include "config.h"
#include "fmt/base.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "lepton_error.h"
#include "magic_enum.hpp"
#include "memory_storage.h"
#include "protobuf.h"
#include "raft.h"
#include "raft_log.h"
#include "spdlog/spdlog.h"
#include "state.h"
#include "storage.h"
#include "test_diff.h"
#include "test_raft_protobuf.h"
#include "test_raft_utils.h"
#include "test_utility_data.h"
#include "types.h"
using namespace lepton;

class raft_snap_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

static raftpb::snapshot init_test_raft_snap() {
  raftpb::snapshot snap;
  snap.mutable_metadata()->set_index(11);
  snap.mutable_metadata()->set_term(11);
  snap.mutable_metadata()->mutable_conf_state()->add_voters(1);
  snap.mutable_metadata()->mutable_conf_state()->add_voters(2);
  return snap;
}

TEST_F(raft_snap_test_suit, test_sending_snapshot_set_pending_snapshot) {
  auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1})}})));
  sm.restore(init_test_raft_snap());

  sm.become_candidate();
  sm.become_leader();

  // force set the next of node 2, so that
  // node 2 needs a snapshot
  sm.trk_.progress_map_mutable_view().mutable_view().at(2).set_next(sm.raft_log_handle_.first_index());

  auto msg = new_pb_message(2, 1, raftpb::message_type::MSG_APP_RESP);
  msg.set_index(sm.trk_.progress_map_view().view().at(2).next() - 1);
  msg.set_reject(true);
  sm.step(raftpb::message{msg});
  ASSERT_EQ(11, sm.trk_.progress_map_view().view().at(2).pending_snapshot()) << "pending snapshot should be set to 11";
}

TEST_F(raft_snap_test_suit, test_pending_snapshot_pause_replication) {
  auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2})}})));
  sm.restore(init_test_raft_snap());

  sm.become_candidate();
  sm.become_leader();

  sm.trk_.progress_map_mutable_view().mutable_view().at(2).become_snapshot(11);
  sm.step(new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "somedata"));
  auto ms = sm.read_messages();
  ASSERT_EQ(0, ms.size());
}

// 验证 Raft 算法中的 Leader 节点在​​处理 Follower
// 节点快照（Snapshot）传输失败时的状态恢复和重试机制​​
TEST_F(raft_snap_test_suit, test_snapshot_failure) {
  auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2})}})));
  sm.restore(init_test_raft_snap());

  sm.become_candidate();
  sm.become_leader();

  sm.trk_.progress_map_mutable_view().mutable_view().at(2).set_next(1);
  sm.trk_.progress_map_mutable_view().mutable_view().at(2).become_snapshot(11);

  auto msg = new_pb_message(2, 1, raftpb::message_type::MSG_SNAP_STATUS);
  msg.set_reject(true);
  //   确保 Leader 在收到快照失败响应后，​​立即清除 Follower 的 PendingSnapshot 标记​
  // ​（重置 pending_snapshot 为 0）
  sm.step(raftpb::message{msg});
  ASSERT_EQ(0, sm.trk_.progress_map_view().view().at(2).pending_snapshot());
  ASSERT_EQ(1, sm.trk_.progress_map_view().view().at(2).next());
  ASSERT_TRUE(sm.trk_.progress_map_view().view().at(2).msg_app_flow_paused());
}

TEST_F(raft_snap_test_suit, test_snapshot_succeed) {
  auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2})}})));
  sm.restore(init_test_raft_snap());

  sm.become_candidate();
  sm.become_leader();

  sm.trk_.progress_map_mutable_view().mutable_view().at(2).set_next(1);
  sm.trk_.progress_map_mutable_view().mutable_view().at(2).become_snapshot(11);

  auto msg = new_pb_message(2, 1, raftpb::message_type::MSG_SNAP_STATUS);
  sm.step(raftpb::message{msg});
  ASSERT_EQ(0, sm.trk_.progress_map_view().view().at(2).pending_snapshot());
  ASSERT_EQ(12, sm.trk_.progress_map_view().view().at(2).next());
  ASSERT_TRUE(sm.trk_.progress_map_view().view().at(2).msg_app_flow_paused());
}

TEST_F(raft_snap_test_suit, test_snapshot_abort) {
  auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2})}})));
  sm.restore(init_test_raft_snap());

  sm.become_candidate();
  sm.become_leader();

  sm.trk_.progress_map_mutable_view().mutable_view().at(2).set_next(1);
  sm.trk_.progress_map_mutable_view().mutable_view().at(2).become_snapshot(11);

  // A successful msgAppResp that has a higher/equal index than the
  // pending snapshot should abort the pending snapshot.
  auto msg = new_pb_message(2, 1, raftpb::message_type::MSG_APP_RESP);
  msg.set_index(11);
  sm.step(raftpb::message{msg});
  ASSERT_EQ(0, sm.trk_.progress_map_view().view().at(2).pending_snapshot());
  // The follower entered StateReplicate and the leader send an append
  // and optimistically updated the progress (so we see 13 instead of 12).
  // There is something to append because the leader appended an empty entry
  // to the log at index 12 when it assumed leadership.
  ASSERT_EQ(13, sm.trk_.progress_map_view().view().at(2).next());
  ASSERT_EQ(1, sm.trk_.progress_map_view().view().at(2).ref_inflights().count());
}