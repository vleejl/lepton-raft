#include <gtest/gtest.h>
#include <proxy.h>
#include <raft.pb.h>

#include <cstddef>
#include <memory>
#include <vector>

#include "config.h"
#include "error.h"
#include "memory_storage.h"
#include "raft.h"
#include "types.h"
using namespace lepton;
using memory_storage_ptr = std::unique_ptr<lepton::memory_storage>;

PRO_DEF_MEM_DISPATCH(state_machine_step, step);

PRO_DEF_MEM_DISPATCH(state_machine_read_messages, read_messages);

PRO_DEF_MEM_DISPATCH(state_machine_advance_messages_after_append, advance_messages_after_append);

// clang-format off
struct state_machine_builer : pro::facade_builder 
  ::add_convention<state_machine_step, leaf::result<void>()> 
  ::add_convention<state_machine_read_messages, lepton::pb::repeated_message()>
  ::add_convention<state_machine_advance_messages_after_append, void()>
  ::add_view<storage_builer>
  ::build{};
// clang-format on

class raft_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

static lepton::pb::repeated_entry next_ents(raft &r, memory_storage &s) {
  lepton::pb::repeated_entry ents;
  auto next_unstable_ents = r.raft_log_handle_.next_unstable_ents();
  for (const auto &entry : next_unstable_ents) {
    ents.Add()->CopyFrom(*entry);
  }
  // Append unstable entries.
  s.append(std::move(ents));
  r.raft_log_handle_.stable_to(r.raft_log_handle_.last_entry_id());

  // Run post-append steps.
  r.advance_messages_after_append();

  // Return committed entries.
  auto next_committed_ents = r.raft_log_handle_.next_committed_ents(true);
  r.raft_log_handle_.applied_to(r.raft_log_handle_.committed(), 0);
  return next_committed_ents;
}

static void must_append_entry(raft &r, lepton::pb::repeated_entry &&ents) {
  if (!r.append_entry(std::move(ents))) {
    LEPTON_CRITICAL("entry unexpectedly dropped");
  }
}

using test_memory_storage_options = std::function<void(lepton::memory_storage &)>;

static test_memory_storage_options with_peers(lepton::pb::repeated_peers &&peers) {
  // / 将右值 peers 移动构造到堆内存，并用 shared_ptr 管理 auto data =
  auto data = std::make_shared<lepton::pb::repeated_peers>(std::move(peers));

  // 返回的 lambda 按值捕获 shared_ptr（安全）
  return [data](lepton::memory_storage &ms) {
    auto *conf_state = ms.snapshot_ref().mutable_metadata()->mutable_conf_state();

    // 安全操作：data 的生命周期与 lambda 绑定
    conf_state->mutable_voters()->Swap(data.get());
  };
}

static test_memory_storage_options with_peers(std::vector<std::uint64_t> &&peers) {
  lepton::pb::repeated_peers repeated_peers;
  for (auto id : peers) {
    repeated_peers.Add(id);
  }
  return with_peers(std::move(repeated_peers));
}

static test_memory_storage_options with_learners(lepton::pb::repeated_peers &&learners) {
  return [&](lepton::memory_storage &ms) -> void {
    ms.snapshot_ref().mutable_metadata()->mutable_conf_state()->mutable_learners()->Swap(&learners);
  };
}

static memory_storage_ptr new_memory_storage(std::vector<test_memory_storage_options> &&options) {
  auto ms_ptr = std::make_unique<lepton::memory_storage>();
  auto &ms = *ms_ptr;
  for (auto &option : options) {
    option(ms);
  }
  return ms_ptr;
}

static lepton::config new_test_config(std::uint64_t id, int election_tick, int heartbeat_tick,
                                      pro::proxy_view<storage_builer> storage) {
  return lepton::config{id, election_tick, heartbeat_tick, storage, lepton::NO_LIMIT, 256};
}

static lepton::raft new_test_raft(std::uint64_t id, int election_tick, int heartbeat_tick,
                                  pro::proxy_view<storage_builer> storage) {
  auto r = new_raft(new_test_config(id, election_tick, heartbeat_tick, storage));
  assert(r);
  return std::move(r.value());
}

static raftpb::message new_pb_message(std::uint64_t from, std::uint64_t to, raftpb::message_type type) {
  raftpb::message msg;
  msg.set_from(from);
  msg.set_to(to);
  msg.set_type(type);
  return msg;
}

TEST_F(raft_test_suit, progress_leader) {
  auto ms = new_memory_storage({with_peers({1, 2})});
  pro::proxy_view<storage_builer> storage{ms.get()};
  auto r = new_test_raft(1, 5, 1, storage);
  r.become_candidate();
  r.become_leader();
  r.trk_.progress_map_mutable_view().mutable_view().at(2).become_replicate();

  // Send proposals to r1. The first 5 entries should be queued in the unstable log.
  raftpb::message prop_msg = new_pb_message(1, 1, raftpb::message_type::MSG_PROP);
  auto entry = prop_msg.add_entries();
  entry->set_data("foo");
  for (std::size_t i = 0; i < 5; ++i) {
    raftpb::message new_prop_msg{prop_msg};
    r.step(std::move(new_prop_msg));
  }
  ASSERT_EQ(0, r.trk_.progress_map_mutable_view().mutable_view().at(1).match());

  auto ents = r.raft_log_handle_.next_unstable_ents();
  ASSERT_EQ(6, ents.size());
  ASSERT_TRUE(ents[0]->data().empty());
  ASSERT_EQ("foo", ents[5]->data());

  r.advance_messages_after_append();
  ASSERT_EQ(6, r.trk_.progress_map_mutable_view().mutable_view().at(1).match());
  ASSERT_EQ(7, r.trk_.progress_map_mutable_view().mutable_view().at(1).next());
}

// TestProgressResumeByHeartbeatResp ensures raft.heartbeat reset progress.paused by heartbeat response.
TEST_F(raft_test_suit, progress_resume_by_heartbeat_resp) {
  auto ms = new_memory_storage({with_peers({1, 2})});
  pro::proxy_view<storage_builer> storage{ms.get()};
  auto r = new_test_raft(1, 5, 1, storage);
  r.become_candidate();
  r.become_leader();
  r.trk_.progress_map_mutable_view().mutable_view().at(2).set_msg_app_flow_paused(true);

  // Send proposals to r1. The first 5 entries should be queued in the unstable log.
  r.step(new_pb_message(1, 1, raftpb::MSG_BEAT));
  ASSERT_TRUE(r.trk_.progress_map_mutable_view().mutable_view().at(2).msg_app_flow_paused());

  r.trk_.progress_map_mutable_view().mutable_view().at(2).become_replicate();
  ASSERT_FALSE(r.trk_.progress_map_mutable_view().mutable_view().at(2).msg_app_flow_paused());
  r.trk_.progress_map_mutable_view().mutable_view().at(2).set_msg_app_flow_paused(true);
  r.step(new_pb_message(2, 1, raftpb::MSG_HEARTBEAT_RESP));
  ASSERT_FALSE(r.trk_.progress_map_mutable_view().mutable_view().at(2).msg_app_flow_paused());
}

TEST_F(raft_test_suit, progress_paused) {
  auto ms = new_memory_storage({with_peers({1, 2})});
  pro::proxy_view<storage_builer> storage{ms.get()};
  auto r = new_test_raft(1, 5, 1, storage);
  r.become_candidate();
  r.become_leader();
  auto prop_msg = new_pb_message(1, 1, raftpb::MSG_PROP);
  prop_msg.add_entries()->set_data("somedata");
  r.step(raftpb::message(prop_msg));
  r.step(raftpb::message(prop_msg));
  r.step(raftpb::message(prop_msg));

  auto msgs = r.read_messages();
  ASSERT_EQ(1, msgs.size());
}

TEST_F(raft_test_suit, progress_flow_control) {
  auto ms = new_memory_storage({with_peers({1, 2})});
  pro::proxy_view<storage_builer> storage{ms.get()};
  auto cfg = new_test_config(1, 5, 1, storage);
  cfg.max_inflight_msgs = 3;
  cfg.max_size_per_msg = 2048;
  cfg.max_inflight_bytes = 9000;  // A little over MaxInflightMsgs * MaxSizePerMsg.
  auto r_result = new_raft(std::move(cfg));
  ASSERT_TRUE(r_result);
  auto &r = r_result.value();
  r.become_candidate();
  r.become_leader();

  // Throw away all the messages relating to the initial election.
  r.read_messages();

  // While node 2 is in probe state, propose a bunch of entries.
  r.trk_.progress_map_mutable_view().mutable_view().at(2).become_probe();
  auto blob = std::string(1000, 'a');
  auto large = std::string(5000, 'b');
  for (auto i = 0; i < 22; ++i) {
    auto entry_data = blob;
    if (i >= 10 && i < 16) {
      entry_data = large;
    }
    auto msg = new_pb_message(1, 1, raftpb::MSG_PROP);
    msg.add_entries()->mutable_data()->swap(entry_data);
    r.step(std::move(msg));
  }

  auto msgs = r.read_messages();
  // First append has two entries: the empty entry to confirm the
  // election, and the first proposal (only one proposal gets sent
  // because we're in probe state).
  ASSERT_EQ(1, msgs.size());
  ASSERT_EQ(raftpb::message_type::MSG_APP, msgs[0].type());
  ASSERT_EQ(2, msgs[0].entries_size());
  ASSERT_TRUE(msgs[0].entries().at(0).data().empty());
  ASSERT_EQ(1000, msgs[0].entries().at(1).data().size());

  auto ack_and_verify = [&](std::uint64_t index, std::vector<int> exp_entries) -> std::uint64_t {
    auto msg = new_pb_message(2, 1, raftpb::MSG_APP_RESP);
    msg.set_index(index);
    r.step(std::move(msg));
    auto msgs = r.read_messages();
    auto msgs_size = msgs.size();
    auto exp_entries_size = static_cast<int>(exp_entries.size());
    assert(msgs_size == exp_entries_size);
    for (int i = 0; i < msgs.size(); ++i) {
      assert(raftpb::message_type::MSG_APP == msgs[i].type());
      const auto entries_size = msgs[i].entries_size();
      assert(exp_entries[static_cast<std::size_t>(i)] == entries_size);
    }
    auto last = msgs.at(msgs.size() - 1).entries();
    if (last.empty()) {
      return index;
    }
    return last.at(last.size() - 1).index();
  };

  // When this append is acked, we change to replicate state and can
  // send multiple messages at once.
  auto index = ack_and_verify(msgs.at(0).entries().at(1).index(), {2, 2, 2});
  // Ack all three of those messages together and get another 3 messages. The
  // third message contains a single large entry, in contrast to 2 before.
  index = ack_and_verify(index, {2, 1, 1});
  // All subsequent messages contain one large entry, and we cap at 2 messages
  // because it overflows MaxInflightBytes.
  index = ack_and_verify(index, {1, 1});
  index = ack_and_verify(index, {1, 1});
  // Start getting small messages again.
  index = ack_and_verify(index, {1, 2, 2});
  ack_and_verify(index, {2});
}