#include <gtest/gtest.h>
#include <proxy.h>
#include <raft.pb.h>

#include <cmath>
#include <cstddef>
#include <cstdio>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "conf_change.h"
#include "config.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "magic_enum.hpp"
#include "protobuf.h"
#include "raft.h"
#include "raft_log.h"
#include "raw_node.h"
#include "spdlog/spdlog.h"
#include "storage.h"
#include "test_diff.h"
#include "test_raft_protobuf.h"
#include "test_raft_utils.h"
#include "test_utility_data.h"
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
TEST_F(raw_node_test_suit, test_node_step_unblock) {
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
    std::uint64_t last_index;
    std::string ccdata;
    // Propose the ConfChange, wait until it applies, save the resulting
    // ConfState.
    std::optional<raftpb::conf_state> conf_state;
    while (!conf_state.has_value()) {
      auto rd = raw_node.ready();
      ASSERT_TRUE(mm_storage.append(std::move(rd.entries)));
      for (auto& ent : rd.committed_entries) {
        lepton::pb::conf_change_var cc;
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