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

static raftpb::hard_state EMPTY_STATE;

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

leaf::result<void> run_awaitable(auto&& awaitable) {
  asio::io_context ctx;
  leaf::result<void> result;

  asio::co_spawn(
      ctx, [&]() -> asio::awaitable<void> { result = co_await std::forward<decltype(awaitable)>(awaitable); },
      asio::detached);

  ctx.run();
  return result;
}

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
    ASSERT_TRUE(raw_node.campaign());

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

TEST_F(raw_node_test_suit, test_raw_node_restart) {
  lepton::pb::repeated_entry entries;
  {
    auto entry1 = entries.Add();  // empty entry
    entry1->set_term(1);
    entry1->set_index(1);
    auto entry2 = entries.Add();  // non-empty entry
    entry2->set_term(1);
    entry2->set_index(2);
    entry2->set_data("foo");
  }

  raftpb::hard_state st;
  st.set_term(1);
  st.set_commit(1);

  lepton::ready want;
  {
    // commit up to commit index in st
    auto entry1 = want.committed_entries.Add();
    entry1->set_term(1);
    entry1->set_index(1);
  }

  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  mm_storage.set_hard_state(st);
  mm_storage.append(std::move(entries));

  auto raw_node_result = lepton::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto& raw_node = *raw_node_result;
  auto rd = raw_node.ready();
  ASSERT_TRUE(compare_ready(rd, want));
  raw_node.advance();
  ASSERT_FALSE(raw_node.has_ready());
}

TEST_F(raw_node_test_suit, test_raw_node_restart_from_snapshot) {
  auto snap = create_snapshot(2, 1, {1, 2});
  lepton::pb::repeated_entry entries;
  {
    auto entry1 = entries.Add();
    entry1->set_term(1);
    entry1->set_index(3);
    entry1->set_data("foo");
  }

  raftpb::hard_state st;
  st.set_term(1);
  st.set_commit(3);

  lepton::ready want;
  want.committed_entries = entries;

  auto mm_storage_ptr = new_test_memory_storage_ptr({});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  mm_storage.set_hard_state(st);
  mm_storage.apply_snapshot(std::move(snap));
  mm_storage.append(std::move(entries));

  auto raw_node_result = lepton::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto& raw_node = *raw_node_result;
  auto rd = raw_node.ready();
  ASSERT_TRUE(compare_ready(rd, want));
  raw_node.advance();
  ASSERT_FALSE(raw_node.has_ready());
}

// TestNodeAdvance from node_test.go has no equivalent in rawNode because there is
// no dependency check between Ready() and Advance()
TEST_F(raw_node_test_suit, test_node_advance) {
  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  auto raw_node_result = lepton::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto& raw_node = *raw_node_result;
  ASSERT_TRUE(raw_node.status().progress.view().empty());
  ASSERT_TRUE(raw_node.campaign());

  auto rd = raw_node.ready();
  mm_storage.append(std::move(rd.entries));
  raw_node.advance();
  auto status = raw_node.status();
  ASSERT_EQ(1, status.basic_status.soft_state.leader_id);
  ASSERT_EQ(lepton::state_type::LEADER, status.basic_status.soft_state.raft_state);
  const auto& lhs_cfg = raw_node.raft_.trk_.progress_map_view().view().at(1);
  const auto& rhs_cfg = status.progress.view().at(1);
  ASSERT_EQ(lhs_cfg, rhs_cfg);

  lepton::tracker::config exp_cfg;
  lepton::quorum::majority_config majority_cfg{std::set<std::uint64_t>{1}};
  lepton::quorum::joint_config joint_cfg{std::move(majority_cfg)};
  exp_cfg.voters = std::move(joint_cfg);
  ASSERT_EQ(exp_cfg, status.config);
}

// TestRawNodeCommitPaginationAfterRestart is the RawNode version of
// TestNodeCommitPaginationAfterRestart. The anomaly here was even worse as the
// Raft group would forget to apply entries:
//
//   - node learns that index 11 is committed
//   - nextCommittedEnts returns index 1..10 in CommittedEntries (but index 10
//     already exceeds maxBytes), which isn't noticed internally by Raft
//   - Commit index gets bumped to 10
//   - the node persists the HardState, but crashes before applying the entries
//   - upon restart, the storage returns the same entries, but `slice` takes a
//     different code path and removes the last entry.
//   - Raft does not emit a HardState, but when the app calls Advance(), it bumps
//     its internal applied index cursor to 10 (when it should be 9)
//   - the next Ready asks the app to apply index 11 (omitting index 10), losing a
//     write.
/*
模拟一种临界情况：节点知道索引 11 已提交（Commit:11），但在应用日志前崩溃。
重启前状态：
​​存储状态​​：持久化的 HardState 中 Commit:10（但存在索引 11 的条目）。
​ - ​日志条目​​：索引 1-11 的条目已保存，其中索引 1-10 大小总和为 size。
​ - ​配置限制​​：MaxSizePerMsg = size - 最后一个条目的大小 -
1，故意使单次消息无法容纳全部索引 1-10 的条目。

重启后 Raft 内部可能错误地认为索引 1-10 已全部应用（实际未应用），导致：
  - ​​问题 1​​：跳过索引 10 的应用（直接尝试应用索引 11）。
  - ​​问题 2​​：日志条目丢失（如索引 10 未被应用）。

验证的核心问题​​
- ​​存储分页边界处理​​：重启后若 Ready() 返回的条目被错误截断（如索引
1-9），导致内部状态认为索引 10 已应用（实际未应用），后续可能跳过索引 10。
​- ​应用状态一致性​​：Advance() 调用后，Raft 内部维护的 applied index
必须与真实应用进度严格一致。
​​- 崩溃恢复鲁棒性​​：即使崩溃发生在状态持久化与应用条目的间隙，
重启后仍需保证所有已提交条目被精确应用。
*/
TEST_F(raw_node_test_suit, test_raw_node_commit_pagination_after_restart) {
  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();

  raftpb::hard_state persisted_hard_state;
  persisted_hard_state.set_term(1);
  persisted_hard_state.set_commit(1);
  persisted_hard_state.set_commit(10);
  mm_storage.set_hard_state(persisted_hard_state);

  auto& ents = mm_storage.mutable_ents();
  std::uint64_t size = 0;
  for (std::uint64_t i = 0; i < 10; ++i) {
    auto entry = ents.Add();
    entry->set_term(1);
    entry->set_index(i + 1);
    entry->set_type(raftpb::ENTRY_NORMAL);
    entry->set_data("a");
    size += entry->ByteSizeLong();
  }

  auto cfg = new_test_config(1, 10, 1, std::move(storage_proxy));
  // Set a MaxSizePerMsg that would suggest to Raft that the last committed entry should
  // not be included in the initial rd.CommittedEntries. However, our storage will ignore
  // this and *will* return it (which is how the Commit index ended up being 10 initially).
  cfg.max_size_per_msg = size - ents.at(ents.size() - 1).ByteSizeLong() - 1;

  {
    auto entry = ents.Add();
    entry->set_term(1);
    entry->set_index(11);
    entry->set_type(raftpb::ENTRY_NORMAL);
    entry->set_data("boom");
  }

  auto raw_node_result = lepton::new_raw_node(std::move(cfg));
  ASSERT_TRUE(raw_node_result);
  auto& raw_node = *raw_node_result;

  // 循环检查每次 Ready() 返回的 CommittedEntries：
  // ​​连续性验证​​：新条目必须紧接着已应用的索引（如 highestApplied=9
  // 时，下一条必须是索引 10）。 ​​完整性验证​​：最终必须应用索引
  // 11（highestApplied=11）。
  for (std::uint64_t highest_applied = 0; highest_applied != 11;) {
    auto rd = raw_node.ready();
    auto n = rd.committed_entries.size();
    ASSERT_NE(0, n) << "stopped applying entries at index " << highest_applied;
    auto next = rd.committed_entries.begin()->index();
    ASSERT_FALSE(highest_applied != 0 && highest_applied + 1 != next)
        << fmt::format("attempting to apply index {} after index {}, leaving a gap", next, highest_applied);
    highest_applied = rd.committed_entries[n - 1].index();
    raw_node.advance();
    raftpb::message msg;
    msg.set_type(::raftpb::message_type::MSG_HEARTBEAT);
    msg.set_to(1);
    msg.set_from(2);  // illegal, but we get away with it
    msg.set_term(1);
    // 强制更新 Commit Index​
    msg.set_commit(11);
    raw_node.step(std::move(msg));
  }
}

// TestRawNodeBoundedLogGrowthWithPartition tests a scenario where a leader is
// partitioned from a quorum of nodes. It verifies that the leader's log is
// protected from unbounded growth even as new entries continue to be proposed.
// This protection is provided by the MaxUncommittedEntriesSize configuration.
// 验证目的：在网络分区期间，通过配置 MaxUncommittedEntriesSize 来防止 leader
// 节点的日志无限增长​​。
TEST_F(raw_node_test_suit, test_raw_node_bounded_log_growth_with_partition) {
  constexpr auto MAX_ENTRIES = 16;
  std::string data = "testdata";
  raftpb::entry test_entry;
  test_entry.set_data(data);
  auto max_entry_size = MAX_ENTRIES * lepton::pb::payloads_size(test_entry);

  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  auto cfg = new_test_config(1, 10, 1, std::move(storage_proxy));
  cfg.max_uncommitted_entries_size = max_entry_size;

  auto raw_node_result = lepton::new_raw_node(std::move(cfg));
  ASSERT_TRUE(raw_node_result);
  auto& raw_node = *raw_node_result;

  // Become the leader and apply empty entry.
  raw_node.campaign();
  while (true) {
    auto rd = raw_node.ready();
    mm_storage.append(std::move(rd.entries));
    raw_node.advance();
    if (!rd.committed_entries.empty()) {
      break;
    }
  }

  // Simulate a network partition while we make our proposals by never
  // committing anything. These proposals should not cause the leader's
  // log to grow indefinitely.
  // 模拟分区​​：leader 无法联系 follower，无法复制日志
  // ​​关键风险​​：若无保护机制，leader 将积累1024个未提交条目
  for (auto i = 0; i < 1024; ++i) {
    raw_node.propose(std::string{data});
  }

  // Check the size of leader's uncommitted log tail. It should not exceed the
  // MaxUncommittedEntriesSize limit.
  auto check_uncommitted = [&raw_node](lepton::pb::entry_payload_size exp) {
    ASSERT_EQ(exp, raw_node.raft_.uncommitted_size_);
  };
  check_uncommitted(max_entry_size);

  // Recover from the partition. The uncommitted tail of the Raft log should
  // disappear as entries are committed.
  auto rd = raw_node.ready();
  ASSERT_EQ(MAX_ENTRIES, rd.entries.size());
  mm_storage.append(std::move(rd.entries));
  raw_node.advance();

  // Entries are appended, but not applied.
  check_uncommitted(max_entry_size);

  rd = raw_node.ready();
  ASSERT_TRUE(rd.entries.empty());
  ASSERT_EQ(MAX_ENTRIES, rd.committed_entries.size());
  raw_node.advance();
  check_uncommitted(0);
}

TEST_F(raw_node_test_suit, test_raw_node_consume_ready) {
  // Check that readyWithoutAccept() does not call acceptReady (which resets
  // the messages) but Ready() does.
  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  // auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  auto raw_node_result = lepton::new_raw_node(new_test_config(1, 3, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto& raw_node = *raw_node_result;

  raftpb::message m1;
  m1.set_context("foo");
  raftpb::message m2;
  m2.set_context("bar");

  // Inject first message, make sure it's visible via readyWithoutAccept.
  raw_node.raft_.msgs_.Add(raftpb::message{m1});
  auto rd = raw_node.ready_without_accept();
  ASSERT_EQ(1, rd.messages.size());
  ASSERT_EQ(m1.DebugString(), rd.messages[0].DebugString());
  ASSERT_EQ(1, raw_node.raft_.msgs_.size());
  ASSERT_EQ(m1.DebugString(), raw_node.raft_.msgs_[0].DebugString());

  // Now call Ready() which should move the message into the Ready (as opposed
  // to leaving it in both places).
  rd = raw_node.ready();
  ASSERT_TRUE(raw_node.raft_.msgs_.empty());
  ASSERT_EQ(1, rd.messages.size());
  ASSERT_EQ(m1.DebugString(), rd.messages[0].DebugString());

  // Add a message to raft to make sure that Advance() doesn't drop it.
  raw_node.raft_.msgs_.Add(raftpb::message{m2});
  raw_node.advance();
  ASSERT_EQ(1, raw_node.raft_.msgs_.size());
  ASSERT_EQ(m2.DebugString(), raw_node.raft_.msgs_[0].DebugString());
}