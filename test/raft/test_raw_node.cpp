#include <gtest/gtest.h>
#include <proxy.h>
#include <raft.pb.h>

#include <cmath>
#include <cstddef>
#include <cstdio>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "conf_change.h"
#include "config.h"
#include "describe.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "lepton_error.h"
#include "magic_enum.hpp"
#include "memory_storage.h"
#include "node.h"
#include "protobuf.h"
#include "raft.h"
#include "raft_log.h"
#include "raw_node.h"
#include "read_only.h"
#include "spdlog/spdlog.h"
#include "state.h"
#include "storage.h"
#include "test_diff.h"
#include "test_raft_protobuf.h"
#include "test_raft_utils.h"
#include "test_utility_data.h"
#include "types.h"
using namespace lepton;

// rawNodeAdapter is essentially a lint that makes sure that RawNode implements
// "most of" Node. The exceptions (some of which are easy to fix) are listed
// below.
struct raw_node_adapter {
  lepton::raw_node node;

  // TransferLeadership is to test when node specifies lead, which is pointless, can just be filled in.
  void transfer_leadership(std::uint64_t transferee) { node.transfer_leadership(transferee); }

  // ForgetLeader takes a context, RawNode doesn't need it.
  void forget_leader() { node.forget_leader(); }

  // Stop when node has a goroutine, RawNode doesn't need this.
  void stop() {}

  // Status retirns RawNode's status as *Status.
  auto status() const { return node.status(); }

  // Advance is when RawNode takes a Ready. It doesn't really have to do that I think? It can hold on
  // to it internally. But maybe that approach is frail.
  void advance() { node.advance(); }

  // Ready when RawNode returns a Ready, not a chan of one.
  ready_channel_handle ready() { return nullptr; }

  // Node takes more contexts. Easy enough to fix.

  leaf::result<void> campaign() { return node.campaign(); }

  leaf::result<void> read_index(std::string&& rctx) {
    node.read_index(std::move(rctx));
    // RawNode swallowed the error in ReadIndex, it probably should not do that.
    return {};
  }

  leaf::result<void> step(raftpb::message&& m) { return node.step(std::move(m)); }

  leaf::result<void> propose(std::string&& data) { return node.propose(std::move(data)); }

  leaf::result<void> propose_conf_change(const lepton::pb::conf_change_var& cc) { return node.propose_conf_change(cc); }
};

class raw_node_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

// TestRawNodeStep ensures that RawNode.Step ignore local message.
TEST_F(raw_node_test_suit, test_raw_node_step) {
  for (std::size_t i = 0; i < all_raftpb_message_types.size(); ++i) {
    auto msg_type = all_raftpb_message_types[i];

    auto ms = new_test_memory_storage({});
    ASSERT_TRUE(ms.append(create_entries(1, {1})));
    raftpb::hard_state hard_state;
    hard_state.set_term(1);
    hard_state.set_commit(1);
    ms.set_hard_state(std::move(hard_state));
    ASSERT_TRUE(ms.apply_snapshot(create_snapshot(1, 1, {1})));
    // Append an empty entry to make sure the non-local messages (like
    // vote requests) are ignored and don't trigger assertions.
    auto raw_node_result =
        lepton::new_raw_node(new_test_config(1, 10, 1, pro::make_proxy<storage_builer>(std::move(ms))));
    ASSERT_TRUE(raw_node_result);
    auto& raw_node = *raw_node_result;
    raftpb::message msg;
    msg.set_type(msg_type);
    auto err_code = EC_SUCCESS;
    auto has_called_error = false;
    auto step_resilt = leaf::try_handle_some(
        [&]() -> leaf::result<void> {
          BOOST_LEAF_CHECK(raw_node.step(std::move(msg)));
          return {};
        },
        [&](const lepton_error& e) -> leaf::result<void> {
          has_called_error = true;
          err_code = e.err_code;
          SPDLOG_ERROR("RawNode step error: {}", e.message);
          return new_error(e);
        });
    if (lepton::pb::is_local_msg(msg_type)) {
      ASSERT_EQ(lepton::raft_error::STEP_LOCAL_MSG, err_code);
      ASSERT_TRUE(has_called_error);
    }
  }
}

// TestNodeStepUnblock from node_test.go has no equivalent in rawNode because there is
// no goroutine in RawNode.

// TestRawNodeProposeAndConfChange tests the configuration change mechanism. Each
// test case sends a configuration change which is either simple or joint, verifies
// that it applies and that the resulting ConfState matches expectations, and for
// joint configurations makes sure that they are exited successfully.
TEST_F(raw_node_test_suit, test_raw_node_propose_and_conf_change) {
  struct test_case {
    lepton::pb::conf_change_var cc;
    raftpb::conf_state exp;
    std::optional<raftpb::conf_state> exp2;
  };
  std::vector<test_case> tests = {
      // V1 config change.
      // 测试用例 1: V1 配置变更
      {
          .cc = create_conf_change_v1(2, raftpb::CONF_CHANGE_ADD_NODE),
          .exp = create_conf_state({1, 2}, {}, {}, {}),
          .exp2 = std::nullopt,
      },
      // Proposing the same as a V2 change works just the same, without entering
      // a joint config.
      // 测试用例 2: V2 添加节点
      {
          .cc = create_conf_change_v2(2, raftpb::CONF_CHANGE_ADD_NODE),
          .exp = create_conf_state({1, 2}, {}, {}, {}),
          .exp2 = std::nullopt,
      },
      // Ditto if we add it as a learner instead.
      // 测试用例 3: 添加 Learner
      {
          .cc = create_conf_change_v2(2, raftpb::CONF_CHANGE_ADD_LEARNER_NODE),
          .exp = create_conf_state({1}, {}, {2}, {}),
          .exp2 = std::nullopt,
      },
      // We can ask explicitly for joint consensus if we want it.
      // 测试用例 4: 显式联合共识
      {
          .cc = create_conf_change_v2(2, raftpb::CONF_CHANGE_ADD_LEARNER_NODE,
                                      raftpb::CONF_CHANGE_TRANSITION_JOINT_EXPLICIT),
          .exp = create_conf_state({1}, {1}, {2}, {}),
          .exp2 = create_conf_state({1}, {}, {2}, {}),
      },
      // Ditto, but with implicit transition (the harness checks this).
      // 测试用例 5: 隐式联合共识
      {
          .cc = create_conf_change_v2(2, raftpb::CONF_CHANGE_ADD_LEARNER_NODE,
                                      raftpb::CONF_CHANGE_TRANSITION_JOINT_IMPLICIT),
          .exp = create_conf_state({1}, {1}, {2}, {}, true),
          .exp2 = create_conf_state({1}, {}, {2}, {}),
      },
      // Add a new node and demote n1. This exercises the interesting case in
      // which we really need joint config changes and also need LearnersNext.
      // 测试用例 6: 多节点变更（默认）
      {
          .cc = create_conf_change_v2({{2, raftpb::CONF_CHANGE_ADD_NODE},
                                       {1, raftpb::CONF_CHANGE_ADD_LEARNER_NODE},
                                       {3, raftpb::CONF_CHANGE_ADD_LEARNER_NODE}}),
          .exp = create_conf_state({2}, {1}, {3}, {1}, true),
          .exp2 = create_conf_state({2}, {}, {1, 3}, {}),
      },
      // Ditto explicit.
      // 测试用例 7: 显式多节点变更
      {
          .cc = create_conf_change_v2({{2, raftpb::CONF_CHANGE_ADD_NODE},
                                       {1, raftpb::CONF_CHANGE_ADD_LEARNER_NODE},
                                       {3, raftpb::CONF_CHANGE_ADD_LEARNER_NODE}},
                                      raftpb::CONF_CHANGE_TRANSITION_JOINT_EXPLICIT),
          .exp = create_conf_state({2}, {1}, {3}, {1}),
          .exp2 = create_conf_state({2}, {}, {1, 3}, {}),
      },
      // Ditto implicit.
      // 测试用例 8: 隐式多节点变更
      {
          .cc = create_conf_change_v2({{2, raftpb::CONF_CHANGE_ADD_NODE},
                                       {1, raftpb::CONF_CHANGE_ADD_LEARNER_NODE},
                                       {3, raftpb::CONF_CHANGE_ADD_LEARNER_NODE}},
                                      raftpb::CONF_CHANGE_TRANSITION_JOINT_IMPLICIT),
          .exp = create_conf_state({2}, {1}, {3}, {1}, true),
          .exp2 = create_conf_state({2}, {}, {1, 3}, {}),
      },
  };

  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto& tt = tests[i];
    auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
    auto& mm_storage = *mm_storage_ptr;
    pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
    auto raw_node_result = lepton::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
    ASSERT_TRUE(raw_node_result);
    auto& raw_node = *raw_node_result;
    raw_node.campaign();
    auto proposed = false;
    std::uint64_t last_index = 0;
    std::string ccdata;
    // Propose the ConfChange, wait until it applies, save the resulting
    // ConfState.
    std::optional<raftpb::conf_state> conf_state;
    while (!conf_state.has_value()) {
      auto rd = raw_node.ready();
      ASSERT_TRUE(mm_storage.append(std::move(rd.entries)));
      for (auto& ent : rd.committed_entries) {
        lepton::pb::conf_change_var cc = std::monostate{};
        if (ent.type() == raftpb::ENTRY_CONF_CHANGE) {
          raftpb::conf_change cc_v1;
          ASSERT_TRUE(cc_v1.ParseFromString(ent.data()));
          cc = std::move(cc_v1);
        } else if (ent.type() == raftpb::ENTRY_CONF_CHANGE_V2) {
          raftpb::conf_change_v2 cc_v2;
          ASSERT_TRUE(cc_v2.ParseFromString(ent.data()));
          cc = std::move(cc_v2);
        }
        if (!std::holds_alternative<std::monostate>(cc)) {
          conf_state = raw_node.apply_conf_change(lepton::pb::conf_change_var_as_v2(std::move(cc)));
        }
      }
      raw_node.advance();
      // Once we are the leader, propose a command and a ConfChange.
      if (!proposed && rd.soft_state->leader_id == raw_node.raft_.id()) {
        ASSERT_TRUE(raw_node.propose("somedata"));
        if (auto ccv1 = test_conf_change_var_as_v1(tt.cc); ccv1.has_value()) {
          ccdata = ccv1->SerializeAsString();
          raw_node.propose_conf_change(*ccv1);
        } else {
          auto ccv2 = test_conf_change_var_as_v2(tt.cc);
          ccdata = ccv2.SerializeAsString();
          raw_node.propose_conf_change(ccv2);
        }
        proposed = true;
      }
    }

    // Check that the last index is exactly the conf change we put in,
    // down to the bits. Note that this comes from the Storage, which
    // will not reflect any unstable entries that we'll only be presented
    // with in the next Ready.
    auto last_index_result = mm_storage.last_index();
    ASSERT_TRUE(last_index_result);
    last_index = *last_index_result;

    auto entris_res = mm_storage.entries(last_index - 1, last_index + 1, lepton::NO_LIMIT);
    ASSERT_TRUE(entris_res);
    auto& entries = *entris_res;
    ASSERT_EQ(2, entries.size());
    ASSERT_EQ("somedata", entries[0].data());

    auto type = raftpb::ENTRY_CONF_CHANGE;
    if (auto ccv1 = test_conf_change_var_as_v1(tt.cc); !ccv1.has_value()) {
      type = raftpb::ENTRY_CONF_CHANGE_V2;
    }
    ASSERT_EQ(type, entries[1].type());
    ASSERT_EQ(ccdata, entries[1].data());
    ASSERT_TRUE(compare_optional_conf_state(tt.exp, conf_state)) << fmt::format("#{}", i);

    std::uint64_t maybe_plus_one = 0;
    if (auto [ok, autoleve] = lepton::pb::enter_joint(test_conf_change_var_as_v2(tt.cc)); ok && autoleve) {
      // If this is an auto-leaving joint conf change, it will have
      // appended the entry that auto-leaves, so add one to the last
      // index that forms the basis of our expectations on
      // pendingConfIndex. (Recall that lastIndex was taken from stable
      // storage, but this auto-leaving entry isn't on stable storage
      // yet).
      maybe_plus_one = 1;
    }
    ASSERT_EQ(last_index + maybe_plus_one, raw_node.raft_.pending_conf_index_);

    // Move the RawNode along. If the ConfChange was simple, nothing else
    // should happen. Otherwise, we're in a joint state, which is either
    // left automatically or not. If not, we add the proposal that leaves
    // it manually.
    auto rd = raw_node.ready();
    std::string context;
    if (!tt.exp.auto_leave()) {
      ASSERT_TRUE(rd.entries.empty());
      raw_node.advance();
      if (!tt.exp2.has_value()) {
        return;
      }
      context = "manual";
      SPDLOG_INFO("leaving joint state manually");
      raftpb::conf_change_v2 cc_v2;
      cc_v2.set_context(context);
      ASSERT_TRUE(raw_node.propose_conf_change(std::move(cc_v2)));
      rd = raw_node.ready();
    }

    // Check that the right ConfChange comes out.
    ASSERT_EQ(1, rd.entries.size());
    ASSERT_EQ(raftpb::ENTRY_CONF_CHANGE_V2, rd.entries[0].type());
    raftpb::conf_change_v2 cc_v2;
    ASSERT_TRUE(cc_v2.ParseFromString(rd.entries[0].data()));

    // Lie and pretend the ConfChange applied. It won't do so because now
    // we require the joint quorum and we're only running one node.
    conf_state = raw_node.apply_conf_change(std::move(cc_v2));
    ASSERT_TRUE(compare_optional_conf_state(tt.exp2, conf_state));

    raw_node.advance();
  }
}

// TestRawNodeJointAutoLeave tests the configuration change auto leave even leader
// lost leadership.
TEST_F(raw_node_test_suit, test_raw_node_joint_auto_leave) {
  auto test_cc =
      create_conf_change_v2(2, raftpb::CONF_CHANGE_ADD_LEARNER_NODE, raftpb::CONF_CHANGE_TRANSITION_JOINT_IMPLICIT);
  auto exp_cs = create_conf_state({1}, {1}, {2}, {}, true);
  auto exp2_cs = create_conf_state({1}, {}, {2}, {});
  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  auto raw_node_result = lepton::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto& raw_node = *raw_node_result;
  raw_node.campaign();
  auto proposed = false;
  std::uint64_t last_index = 0;
  std::string ccdata;
  std::optional<raftpb::conf_state> conf_state;
  while (!conf_state.has_value()) {
    auto rd = raw_node.ready();
    ASSERT_TRUE(mm_storage.append(std::move(rd.entries)));
    for (auto& ent : rd.committed_entries) {
      lepton::pb::conf_change_var cc = std::monostate{};
      if (ent.type() == raftpb::ENTRY_CONF_CHANGE_V2) {
        raftpb::conf_change_v2 cc_v2;
        ASSERT_TRUE(cc_v2.ParseFromString(ent.data()));
        cc = std::move(cc_v2);
      }
      if (!std::holds_alternative<std::monostate>(cc)) {
        // Force it step down.
        raftpb::message msg;
        msg.set_type(raftpb::message_type::MSG_HEARTBEAT_RESP);
        msg.set_from(1);
        msg.set_term(raw_node.raft_.term() + 1);
        raw_node.step(std::move(msg));
        conf_state = raw_node.apply_conf_change(lepton::pb::conf_change_var_as_v2(std::move(cc)));
      }
    }
    raw_node.advance();
    // Once we are the leader, propose a command and a ConfChange.
    if (!proposed && rd.soft_state->leader_id == raw_node.raft_.id()) {
      ASSERT_TRUE(raw_node.propose("somedata"));
      ccdata = test_cc.SerializeAsString();
      raw_node.propose_conf_change(test_cc);
      proposed = true;
    }
  }

  // Check that the last index is exactly the conf change we put in,
  // down to the bits. Note that this comes from the Storage, which
  // will not reflect any unstable entries that we'll only be presented
  // with in the next Ready.
  auto last_index_result = mm_storage.last_index();
  ASSERT_TRUE(last_index_result);
  last_index = *last_index_result;

  auto entris_res = mm_storage.entries(last_index - 1, last_index + 1, lepton::NO_LIMIT);
  ASSERT_TRUE(entris_res);
  auto& entries = *entris_res;
  ASSERT_EQ(2, entries.size());
  ASSERT_EQ("somedata", entries[0].data());
  ASSERT_EQ(raftpb::ENTRY_CONF_CHANGE_V2, entries[1].type());
  ASSERT_EQ(ccdata, entries[1].data());
  ASSERT_TRUE(compare_optional_conf_state(exp_cs, conf_state));

  ASSERT_EQ(0, raw_node.raft_.pending_conf_index_);

  // Move the RawNode along. It should not leave joint because it's follower.
  auto rd = raw_node.ready_without_accept();
  // Check that the right ConfChange comes out.
  ASSERT_TRUE(rd.entries.empty());

  // Make it leader again. It should leave joint automatically after moving apply index.
  raw_node.campaign();
  rd = raw_node.ready();
  SPDLOG_INFO(lepton::describe_ready(rd));
  mm_storage.append(std::move(rd.entries));
  raw_node.advance();

  rd = raw_node.ready();
  SPDLOG_INFO(lepton::describe_ready(rd));
  mm_storage.append(std::move(rd.entries));
  raw_node.advance();

  rd = raw_node.ready();
  SPDLOG_INFO(lepton::describe_ready(rd));
  mm_storage.append(std::move(rd.entries));
  raw_node.advance();

  rd = raw_node.ready();
  SPDLOG_INFO(lepton::describe_ready(rd));
  // Check that the right ConfChange comes out.
  ASSERT_EQ(1, rd.entries.size());
  ASSERT_EQ(raftpb::ENTRY_CONF_CHANGE_V2, rd.entries[0].type());
  raftpb::conf_change_v2 cc_v2;
  ASSERT_TRUE(cc_v2.ParseFromString(rd.entries[0].data()));
  mm_storage.append(std::move(rd.entries));

  raftpb::conf_change_v2 expected_cc;
  ASSERT_EQ(expected_cc.DebugString(), cc_v2.DebugString());
  // Lie and pretend the ConfChange applied. It won't do so because now
  // we require the joint quorum and we're only running one node.
  auto cs = raw_node.apply_conf_change(std::move(cc_v2));
  ASSERT_TRUE(compare_optional_conf_state(exp2_cs, cs));
}

// TestRawNodeProposeAddDuplicateNode ensures that two proposes to add the same node should
// not affect the later propose to add new node.
TEST_F(raw_node_test_suit, test_raw_node_propose_add_duplicate_node) {
  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  auto raw_node_result = lepton::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto& raw_node = *raw_node_result;

  auto rd = raw_node.ready();
  ASSERT_TRUE(mm_storage.append(std::move(rd.entries)));
  raw_node.advance();

  raw_node.campaign();
  while (true) {
    auto rd = raw_node.ready();
    ASSERT_TRUE(mm_storage.append(std::move(rd.entries)));
    raw_node.advance();
    if (rd.soft_state->leader_id == raw_node.raft_.id()) {
      break;
    }
  }

  auto propose_conf_change_and_apply_func = [&](const raftpb::conf_change& cc) {
    raw_node.propose_conf_change(cc);
    auto rd = raw_node.ready();
    ASSERT_TRUE(mm_storage.append(std::move(rd.entries)));
    for (auto& ent : rd.committed_entries) {
      lepton::pb::conf_change_var cc = std::monostate{};
      if (ent.type() == raftpb::ENTRY_CONF_CHANGE) {
        raftpb::conf_change cc_v1;
        ASSERT_TRUE(cc_v1.ParseFromString(ent.data()));
        cc = std::move(cc_v1);
      }
    }
    raw_node.advance();
  };
  auto cc1 = create_conf_change_v1(1, raftpb::conf_change_type::CONF_CHANGE_ADD_NODE);
  auto ccdata1 = cc1.SerializeAsString();
  propose_conf_change_and_apply_func(cc1);

  // try to add the same node again
  propose_conf_change_and_apply_func(cc1);

  // the new node join should be ok
  auto cc2 = create_conf_change_v1(1, raftpb::conf_change_type::CONF_CHANGE_ADD_NODE);
  auto ccdata2 = cc2.SerializeAsString();
  propose_conf_change_and_apply_func(cc2);

  auto last_index_result = mm_storage.last_index();
  ASSERT_TRUE(last_index_result);
  auto last_index = *last_index_result;

  // the last three entries should be: ConfChange cc1, cc1, cc2
  auto entris_res = mm_storage.entries(last_index - 2, last_index + 1, lepton::NO_LIMIT);
  ASSERT_TRUE(entris_res);
  auto& entries = *entris_res;
  ASSERT_EQ(3, entries.size());
  ASSERT_EQ(ccdata1, entries[0].data());
  ASSERT_EQ(ccdata1, entries[2].data());
}

// TestRawNodeReadIndex ensures that Rawnode.ReadIndex sends the MsgReadIndex message
// to the underlying raft. It also ensures that ReadState can be read out.
TEST_F(raw_node_test_suit, test_raw_node_read_index) {
  lepton::pb::repeated_message msgs;
  auto append_step = [&](raft& _, raftpb::message&& m) -> leaf::result<void> {
    msgs.Add(std::move(m));
    return {};
  };

  auto wrs = std::vector<lepton::read_state>{
      {.index = 1, .request_ctx = "somedata"},
  };

  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  auto raw_node_result = lepton::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto& raw_node = *raw_node_result;
  raw_node.raft_.read_states_ = wrs;
  // ensure the ReadStates can be read out
  ASSERT_TRUE(raw_node.has_ready());
  auto rd = raw_node.ready();
  ASSERT_TRUE(compare_read_states(wrs, rd.read_states));
  mm_storage.append(std::move(rd.entries));
  raw_node.advance();
  // ensure raft.readStates is reset after advance
  ASSERT_TRUE(raw_node.raft_.read_states_.empty());

  auto wrequest_ctx = "somedata";
  raw_node.campaign();
  while (true) {
    rd = raw_node.ready();
    ASSERT_TRUE(mm_storage.append(std::move(rd.entries)));
    raw_node.advance();
    if (rd.soft_state->leader_id == raw_node.raft_.id()) {
      // Once we are the leader, issue a ReadIndex request
      raw_node.raft_.step_func_ = append_step;
      raw_node.read_index(wrequest_ctx);
      break;
    }
  }
  // ensure that MsgReadIndex message is sent to the underlying raft
  ASSERT_EQ(1, msgs.size());
  ASSERT_EQ(raftpb::message_type::MSG_READ_INDEX, msgs[0].type());
  ASSERT_EQ(wrequest_ctx, msgs[0].entries(0).data());
}

// TestBlockProposal from node_test.go has no equivalent in rawNode because there is
// no leader check in RawNode.

// TestNodeTick from node_test.go has no equivalent in rawNode because
// it reaches into the raft object which is not exposed.

// TestNodeStop from node_test.go has no equivalent in rawNode because there is
// no goroutine in RawNode.

// TestRawNodeStart ensures that a node can be started correctly. Note that RawNode
// requires the application to bootstrap the state, i.e. it does not accept peers
// and will not create faux configuration change entries.
TEST_F(raw_node_test_suit, test_raw_node_start) {
  lepton::pb::repeated_entry entries;
  {
    auto entry1 = entries.Add();  // empty entry
    entry1->set_term(1);
    entry1->set_index(2);
    auto entry2 = entries.Add();  // non-empty entry
    entry2->set_term(1);
    entry2->set_index(3);
    entry2->set_data("foo");
  }
  lepton::ready want;
  want.soft_state = lepton::soft_state{.leader_id = 1, .raft_state = lepton::state_type::LEADER};
  want.hard_state.set_term(1);
  want.hard_state.set_commit(3);
  want.hard_state.set_vote(1);
  want.committed_entries = entries;

  auto mm_storage_ptr = new_test_memory_storage_ptr({});
  auto& mm_storage = *mm_storage_ptr;
  mm_storage.mutable_ents()[0].set_index(1);
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  // TODO(tbg): this is a first prototype of what bootstrapping could look
  // like (without the annoying faux ConfChanges). We want to persist a
  // ConfState at some index and make sure that this index can't be reached
  // from log position 1, so that followers are forced to pick up the
  // ConfState in order to move away from log position 1 (unless they got
  // bootstrapped in the same way already). Failing to do so would mean that
  // followers diverge from the bootstrapped nodes and don't learn about the
  // initial config.
  //
  // NB: this is exactly what CockroachDB does. The Raft log really begins at
  // index 10, so empty followers (at index 1) always need a snapshot first.
  auto verify_bootstap = [](memory_storage& storage, const raftpb::conf_state& cs) {
    ASSERT_TRUE(!cs.voters().empty()) << "no voters specified";

    auto first_index = storage.first_index();
    ASSERT_TRUE(first_index);
    ASSERT_GE(*first_index, 2) << "FirstIndex >= 2 is prerequisite for bootstrap";

    auto entris_res = storage.entries(*first_index, *first_index, lepton::NO_LIMIT);
    // TODO(tbg): match exact error
    ASSERT_FALSE(entris_res) << "should not have been able to load first index";

    auto last_index_result = storage.last_index();
    ASSERT_TRUE(last_index_result);
    auto last_index = *last_index_result;

    entris_res = storage.entries(last_index, last_index, lepton::NO_LIMIT);
    // TODO(tbg): match exact error
    ASSERT_FALSE(entris_res) << "should not have been able to load last index";

    auto inital_states = storage.initial_state();
    ASSERT_TRUE(inital_states);
    auto& [hard_state, conf_state] = *inital_states;
    ASSERT_TRUE(lepton::pb::is_empty_hard_state(hard_state));
    ASSERT_TRUE(conf_state.voters().empty());
  };
  auto bootstap = [&](memory_storage& storage, raftpb::conf_state&& cs) -> lepton::leaf::result<void> {
    verify_bootstap(storage, cs);
    raftpb::snapshot snap;
    auto meta = snap.mutable_metadata();
    meta->set_index(1);
    meta->set_term(0);
    *meta->mutable_conf_state() = std::move(cs);
    return storage.apply_snapshot(std::move(snap));
  };
  ASSERT_TRUE(bootstap(mm_storage, create_conf_state({1}, {}, {}, {})));
  auto raw_node_result = lepton::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto& raw_node = *raw_node_result;
  ASSERT_FALSE(raw_node.has_ready());

  raw_node.campaign();
  auto rd = raw_node.ready();
  mm_storage.append(std::move(rd.entries));
  raw_node.advance();
  raw_node.propose("foo");
  ASSERT_TRUE(raw_node.has_ready());

  rd = raw_node.ready();
  ASSERT_TRUE(compare_repeated_entry(entries, rd.entries));
  mm_storage.append(std::move(rd.entries));
  raw_node.advance();

  ASSERT_TRUE(raw_node.has_ready());
  rd = raw_node.ready();
  ASSERT_TRUE(rd.entries.empty());
  ASSERT_FALSE(rd.must_sync);
  raw_node.advance();

  rd.soft_state.reset();
  want.soft_state.reset();
  ASSERT_TRUE(compare_ready(rd, want));
  ASSERT_FALSE(raw_node.has_ready());
}