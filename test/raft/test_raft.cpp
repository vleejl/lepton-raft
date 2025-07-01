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
#include "error.h"
#include "fmt/base.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
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

class raft_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

TEST_F(raft_test_suit, progress_leader) {
  auto storage = pro::make_proxy<storage_builer>(new_test_memory_storage({with_peers({1, 2})}));
  auto r = new_test_raft(1, 5, 1, std::move(storage));
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
  auto storage = pro::make_proxy<storage_builer>(new_test_memory_storage({with_peers({1, 2})}));
  auto r = new_test_raft(1, 5, 1, std::move(storage));
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
  auto storage = pro::make_proxy<storage_builer>(new_test_memory_storage({with_peers({1, 2})}));
  auto r = new_test_raft(1, 5, 1, std::move(storage));
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
  auto storage = pro::make_proxy<storage_builer>(new_test_memory_storage({with_peers({1, 2})}));
  auto cfg = new_test_config(1, 5, 1, std::move(storage));
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

TEST_F(raft_test_suit, uncommitted_entry_limit) {
  // Use a relatively large number of entries here to prevent regression of a
  // bug which computed the size before it was fixed. This test would fail
  // with the bug, either because we'd get dropped proposals earlier than we
  // expect them, or because the final tally ends up nonzero. (At the time of
  // writing, the former).
  constexpr auto max_entries = 1024;
  raftpb::entry test_entry;
  test_entry.set_data("testdata");
  auto max_entry_size = max_entries * lepton::pb::payloads_size(test_entry);

  ASSERT_EQ(0, lepton::pb::payloads_size(raftpb::entry{}));

  auto storage = pro::make_proxy<storage_builer>(new_test_memory_storage({with_peers({1, 2, 3})}));
  auto cfg = new_test_config(1, 5, 1, std::move(storage));
  cfg.max_uncommitted_entries_size = max_entry_size;
  cfg.max_inflight_msgs = 2 * 1024;  // avoid interference
  auto r_result = new_raft(std::move(cfg));
  ASSERT_TRUE(r_result);
  auto &r = r_result.value();
  r.become_candidate();
  r.become_leader();
  ASSERT_EQ(0, r.uncommitted_size_);

  // Set the two followers to the replicate state. Commit to tail of log.
  constexpr auto num_followers = 2;
  r.trk_.progress_map_mutable_view().mutable_view().at(2).become_replicate();
  r.trk_.progress_map_mutable_view().mutable_view().at(3).become_replicate();
  r.uncommitted_size_ = 0;

  // Send proposals to r1. The first 5 entries should be appended to the log.
  auto prop_msg = new_pb_message(1, 1, raftpb::message_type::MSG_PROP);
  prop_msg.mutable_entries()->Add()->CopyFrom(test_entry);
  lepton::pb::repeated_entry prop_ents;
  prop_ents.Reserve(max_entries);
  for (std::size_t i = 0; i < max_entries; ++i) {
    auto result = r.step(raftpb::message{prop_msg});
    ASSERT_TRUE(result);
    prop_ents.Add()->CopyFrom(test_entry);
  }

  // Send one more proposal to r1. It should be rejected.
  std::error_code err_code = EC_SUCCESS;
  auto has_called_error = false;
  auto step_resilt = leaf::try_handle_some(
      [&]() -> leaf::result<void> {
        BOOST_LEAF_CHECK(r.step(raftpb::message{prop_msg}));
        return {};
      },
      [&](const lepton_error &e) -> leaf::result<void> {
        has_called_error = true;
        err_code = e.err_code;
        return new_error(e);
      });
  ASSERT_TRUE(has_called_error);
  ASSERT_EQ(err_code, lepton::logic_error::PROPOSAL_DROPPED);

  // Read messages and reduce the uncommitted size as if we had committed
  // these entries.
  auto msgs = r.read_messages();
  ASSERT_EQ(max_entries * num_followers, msgs.size());
  r.reduce_uncommitted_size(lepton::pb::payloads_size(prop_ents));
  ASSERT_EQ(0, r.uncommitted_size_);

  // Send a single large proposal to r1. Should be accepted even though it
  // pushes us above the limit because we were beneath it before the proposal.
  prop_ents.Clear();
  prop_ents.Reserve(2 * max_entries);
  for (std::size_t i = 0; i < 2 * max_entries; ++i) {
    prop_ents.Add()->CopyFrom(test_entry);
  }
  auto prop_msg_large = new_pb_message(1, 1, raftpb::message_type::MSG_PROP);
  prop_msg_large.mutable_entries()->Add(prop_ents.begin(), prop_ents.end());
  auto result = r.step(raftpb::message{prop_msg_large});
  ASSERT_TRUE(result);

  // Send one more proposal to r1. It should be rejected, again.
  err_code = EC_SUCCESS;
  has_called_error = false;
  step_resilt = leaf::try_handle_some(
      [&]() -> leaf::result<void> {
        BOOST_LEAF_CHECK(r.step(raftpb::message{prop_msg}));
        return {};
      },
      [&](const lepton_error &e) -> leaf::result<void> {
        has_called_error = true;
        err_code = e.err_code;
        return new_error(e);
      });
  ASSERT_TRUE(has_called_error);
  ASSERT_EQ(err_code, lepton::logic_error::PROPOSAL_DROPPED);

  // But we can always append an entry with no Data. This is used both for the
  // leader's first empty entry and for auto-transitioning out of joint config
  // states.
  raftpb::message empty_msg = new_pb_message(1, 1, raftpb::message_type::MSG_PROP);
  empty_msg.add_entries();
  result = r.step(raftpb::message{empty_msg});
  ASSERT_TRUE(result);

  // Read messages and reduce the uncommitted size as if we had committed
  // these entries.
  msgs = r.read_messages();
  ASSERT_EQ(2 * num_followers, msgs.size());
  r.reduce_uncommitted_size(lepton::pb::payloads_size(prop_ents));
  ASSERT_EQ(0, r.uncommitted_size_);
}

static void pre_vote_config(config &cfg) { cfg.pre_vote = true; }

static void test_leader_election(bool pre_vote) {
  std::function<void(lepton::config &)> config_func;
  auto cand_state = lepton::state_type::CANDIDATE;
  std::uint64_t cand_term = 1;
  if (pre_vote) {
    config_func = pre_vote_config;
    // In pre-vote mode, an election that fails to complete
    // leaves the node in pre-candidate state without advancing
    // the term.
    cand_state = lepton::state_type::PRE_CANDIDATE;
    cand_term = 0;
  }

  struct test_case {
    network nw;
    lepton::state_type state;
    std::uint64_t expr_term;
    test_case(network &&network, lepton::state_type s, std::uint64_t term)
        : nw(std::move(network)), state(s), expr_term(term) {}
  };
  std::vector<test_case> test_cases;
  {
    std::vector<state_machine_builer_pair> peers;
    emplace_nil_peer(peers);
    emplace_nil_peer(peers);
    emplace_nil_peer(peers);
    test_cases.emplace_back(new_network_with_config(config_func, std::move(peers)), lepton::state_type::LEADER, 1);
  }
  {
    std::vector<state_machine_builer_pair> peers;
    emplace_nil_peer(peers);
    emplace_nil_peer(peers);
    emplace_nop_stepper(peers);
    test_cases.emplace_back(new_network_with_config(config_func, std::move(peers)), lepton::state_type::LEADER, 1);
  }
  {
    std::vector<state_machine_builer_pair> peers;
    emplace_nil_peer(peers);
    emplace_nop_stepper(peers);
    emplace_nop_stepper(peers);
    test_cases.emplace_back(new_network_with_config(config_func, std::move(peers)), cand_state, cand_term);
  }
  {
    std::vector<state_machine_builer_pair> peers;
    emplace_nil_peer(peers);
    emplace_nop_stepper(peers);
    emplace_nop_stepper(peers);
    emplace_nil_peer(peers);
    test_cases.emplace_back(new_network_with_config(config_func, std::move(peers)), cand_state, cand_term);
  }
  {
    std::vector<state_machine_builer_pair> peers;
    emplace_nil_peer(peers);
    peers.emplace_back(ents_with_config(config_func, {1}));
    peers.emplace_back(ents_with_config(config_func, {1}));
    peers.emplace_back(ents_with_config(config_func, {1}));
    test_cases.emplace_back(new_network_with_config(config_func, std::move(peers)), lepton::state_type::FOLLOWER, 1);
  }

  for (auto &test_case : test_cases) {
    test_case.nw.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
    auto &iter = test_case.nw.peers.at(1);
    // ASSERT_NE(iter, test_case.nw.peers.end());
    ASSERT_NE(nullptr, iter.raft_handle);
    auto &raft_handle = *iter.raft_handle;
    ASSERT_EQ(magic_enum::enum_name(test_case.state), magic_enum::enum_name(raft_handle.state_type_));
    ASSERT_EQ(test_case.expr_term, raft_handle.term_);
  }
}

TEST_F(raft_test_suit, test_leader_election) { test_leader_election(false); }

TEST_F(raft_test_suit, test_leader_election_pre_vote) { test_leader_election(true); }

// TestLearnerElectionTimeout verfies that the leader should not start election even
// when times out.
TEST_F(raft_test_suit, test_learner_election_timeout) {
  auto n1 = new_test_learner_raft(
      1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1}), with_learners({2})}})));
  auto n2 = new_test_learner_raft(
      2, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1}), with_learners({2})}})));

  n1.become_follower(1, NONE);
  n2.become_follower(1, NONE);

  // n2 is learner. Learner should not start election even when times out.
  set_randomized_election_timeout(n2, n2.election_timeout_);
  for (int i = 0; i < n2.election_timeout_; ++i) {
    n2.tick();
  }

  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::FOLLOWER), magic_enum::enum_name(n2.state_type_));
}

// TestLearnerPromotion verifies that the learner should not election until
// it is promoted to a normal peer.
TEST_F(raft_test_suit, test_learner_promotion) {
  auto n1 = new_test_learner_raft(
      1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1}), with_learners({2})}})));
  auto n2 = new_test_learner_raft(
      2, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1}), with_learners({2})}})));

  n1.become_follower(1, NONE);
  n2.become_follower(1, NONE);

  std::vector<state_machine_builer_pair> peers;
  peers.emplace_back(state_machine_builer_pair{n1});
  peers.emplace_back(state_machine_builer_pair{n2});

  auto nt = new_network(std::move(peers));

  ASSERT_NE(magic_enum::enum_name(lepton::state_type::LEADER), magic_enum::enum_name(n1.state_type_));

  // n1 should become leader
  set_randomized_election_timeout(n1, n1.election_timeout_);
  for (int i = 0; i < n1.election_timeout_; ++i) {
    n1.tick();
  }
  n1.advance_messages_after_append();
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER), magic_enum::enum_name(n1.state_type_));
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::FOLLOWER), magic_enum::enum_name(n2.state_type_));

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_BEAT)});

  n1.apply_conf_change(
      lepton::pb::conf_change_as_v2(create_conf_change(2, raftpb::conf_change_type::CONF_CHANGE_ADD_NODE)));
  n2.apply_conf_change(
      lepton::pb::conf_change_as_v2(create_conf_change(2, raftpb::conf_change_type::CONF_CHANGE_ADD_NODE)));
  ASSERT_FALSE(n2.is_learner_);

  // n2 start election, should become leader
  set_randomized_election_timeout(n2, n2.election_timeout_);
  for (int i = 0; i < n2.election_timeout_; ++i) {
    n2.tick();
  }
  n2.advance_messages_after_append();
  nt.send({new_pb_message(2, 2, raftpb::message_type::MSG_BEAT)});
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::FOLLOWER), magic_enum::enum_name(n1.state_type_));
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER), magic_enum::enum_name(n2.state_type_));
}

// TestLearnerCanVote checks that a learner can vote when it receives a valid Vote request.
// See (*raft).Step for why this is necessary and correct behavior.
TEST_F(raft_test_suit, test_learner_can_vote) {
  auto n2 = new_test_learner_raft(
      2, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1}), with_learners({2})}})));

  n2.become_follower(1, NONE);

  // Send a vote request to n2.
  raftpb::message vote_req = new_pb_message(1, 2, raftpb::message_type::MSG_VOTE);
  vote_req.set_term(2);
  vote_req.set_log_term(11);
  vote_req.set_index(11);
  auto result = n2.step(std::move(vote_req));
  ASSERT_TRUE(result);

  auto msgs = n2.read_messages();
  ASSERT_EQ(1, msgs.size());
  ASSERT_EQ(raftpb::message_type::MSG_VOTE_RESP, msgs[0].type());
  ASSERT_FALSE(msgs[0].reject());
}

// testLeaderCycle verifies that each node in a cluster can campaign
// and be elected in turn. This ensures that elections (including
// pre-vote) work when not starting from a clean slate (as they do in
// TestLeaderElection)
void leader_cycle(bool pre_vote) {
  std::function<void(lepton::config &)> config_func;
  if (pre_vote) {
    config_func = pre_vote_config;
  }
  std::vector<state_machine_builer_pair> peers;
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  auto n = new_network_with_config(config_func, std::move(peers));
  for (std::uint64_t campaigner_id = 1; campaigner_id <= 3; ++campaigner_id) {
    n.send({new_pb_message(campaigner_id, campaigner_id, raftpb::message_type::MSG_HUP)});

    for (auto &iter : n.peers) {
      auto &raft_handle = *iter.second.raft_handle;
      if (raft_handle.id_ == campaigner_id) {
        ASSERT_EQ(lepton::state_type::LEADER, raft_handle.state_type_);
      } else {
        ASSERT_EQ(lepton::state_type::FOLLOWER, raft_handle.state_type_);
      }
    }
  }
}

TEST_F(raft_test_suit, test_leader_cycle) { leader_cycle(false); }

TEST_F(raft_test_suit, test_leader_cycle_pre_vote) { leader_cycle(true); }

static void test_leader_election_overwrite_newer_logs(bool pre_vote) {
  std::function<void(lepton::config &)> config_func;
  if (pre_vote) {
    config_func = pre_vote_config;
  }
  // This network represents the results of the following sequence of
  // events:
  // - Node 1 won the election in term 1.
  // - Node 1 replicated a log entry to node 2 but died before sending
  //   it to other nodes.
  // - Node 3 won the second election in term 2.
  // - Node 3 wrote an entry to its logs but died without sending it
  //   to any other nodes.
  //
  // At this point, nodes 1, 2, and 3 all have uncommitted entries in
  // their logs and could win an election at term 3. The winner's log
  // entry overwrites the losers'. (TestLeaderSyncFollowerLog tests
  // the case where older log entries are overwritten, so this test
  // focuses on the case where the newer entries are lost).
  std::vector<state_machine_builer_pair> peers;
  peers.emplace_back(ents_with_config(config_func, {1}));    // Node 1: Won first election
  peers.emplace_back(ents_with_config(config_func, {1}));    // Node 2: Got logs from node 1
  peers.emplace_back(ents_with_config(config_func, {2}));    // Node 3: Won second election
  peers.emplace_back(voted_with_config(config_func, 3, 2));  // Node 4: Voted but didn't get logs
  peers.emplace_back(voted_with_config(config_func, 3, 2));  // Node 5: Voted but didn't get logs
  auto n = new_network_with_config(config_func, std::move(peers));

  // Node 1 campaigns. The election fails because a quorum of nodes
  // know about the election that already happened at term 2. Node 1's
  // term is pushed ahead to 2.
  n.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  auto &raft_handle = *n.peers.at(1).raft_handle;
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::FOLLOWER), magic_enum::enum_name(raft_handle.state_type_));
  ASSERT_EQ(2, raft_handle.term_);

  // Node 1 campaigns again with a higher term. This time it succeeds.
  n.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER), magic_enum::enum_name(raft_handle.state_type_));
  ASSERT_EQ(3, raft_handle.term_);

  // Now all nodes agree on a log entry with term 1 at index 1 (and
  // term 3 at index 2).
  for (auto &iter : n.peers) {
    auto &raft_handle = *iter.second.raft_handle;
    auto all_entries = raft_handle.raft_log_handle_.all_entries();
    ASSERT_EQ(2, all_entries.size());
    ASSERT_EQ(1, all_entries[0].term());
    ASSERT_EQ(3, all_entries[1].term());
  }
}

// TestLeaderElectionOverwriteNewerLogs tests a scenario in which a
// newly-elected leader does *not* have the newest (i.e. highest term)
// log entries, and must overwrite higher-term log entries with
// lower-term ones.
TEST_F(raft_test_suit, test_leader_election_overwrite_newer_logs) { test_leader_election_overwrite_newer_logs(false); }

TEST_F(raft_test_suit, test_leader_election_overwrite_newer_logs_pre_vote) {
  test_leader_election_overwrite_newer_logs(true);
}

static void test_state_from_any_state(raftpb::message_type vt) {
  for (auto st_idx = static_cast<std::uint64_t>(lepton::state_type::FOLLOWER);
       st_idx <= static_cast<std::uint64_t>(lepton::state_type::PRE_CANDIDATE); ++st_idx) {
    auto st = static_cast<lepton::state_type>(st_idx);
    auto r =
        new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
    r.term_ = 1;

    switch (st) {
      case state_type::FOLLOWER: {
        r.become_follower(r.term_, 3);
        break;
      }
      case state_type::CANDIDATE: {
        r.become_candidate();
        break;
      }
      case state_type::LEADER: {
        r.become_candidate();
        r.become_leader();
        break;
      }
      case state_type::PRE_CANDIDATE: {
        r.become_pre_candidate();
        break;
      }
    }

    // Note that setting our state above may have advanced r.Term
    // past its initial value.
    const auto origin_term = r.term_;
    const auto new_term = r.term_ + 1;

    auto msg = new_pb_message(2, 1, vt);
    msg.set_term(new_term);
    msg.set_log_term(new_term);
    msg.set_index(42);
    ASSERT_TRUE(r.step(std::move(msg)));
    auto msgs = r.read_messages();
    ASSERT_EQ(1, msgs.size());
    auto resp = msgs[0];
    ASSERT_EQ(magic_enum::enum_name(lepton::pb::vote_resp_msg_type(vt)), magic_enum::enum_name(resp.type()));
    ASSERT_FALSE(resp.reject());

    // If this was a real vote, we reset our state and term.
    if (vt == raftpb::message_type::MSG_VOTE) {
      ASSERT_EQ(magic_enum::enum_name(lepton::state_type::FOLLOWER), magic_enum::enum_name(r.state_type_));
      ASSERT_EQ(new_term, r.term_);
      ASSERT_EQ(2, r.vote_id_);
    } else {
      // In a prevote, nothing changes.
      ASSERT_EQ(magic_enum::enum_name(st), magic_enum::enum_name(r.state_type_));
      ASSERT_EQ(origin_term, r.term_);
      // if st == StateFollower or StatePreCandidate, r hasn't voted yet.
      // In StateCandidate or StateLeader, it's voted for itself.
      ASSERT_TRUE(r.vote_id_ == NONE || 1 == r.vote_id_);
    }
  }
}

TEST_F(raft_test_suit, vote_from_any_state) { test_state_from_any_state(raftpb::message_type::MSG_VOTE); }

TEST_F(raft_test_suit, pre_vote_from_any_state) { test_state_from_any_state(raftpb::message_type::MSG_PRE_VOTE); }

TEST_F(raft_test_suit, log_replication) {
  struct test_case {
    network nw;
    std::vector<raftpb::message> msgs;
    std::uint64_t wcommitted;
  };
  std::vector<test_case> test_cases;
  {
    std::vector<state_machine_builer_pair> peers;
    emplace_nil_peer(peers);
    emplace_nil_peer(peers);
    emplace_nil_peer(peers);
    auto nt = new_network(std::move(peers));
    test_cases.push_back({
        .nw = std::move(nt),
        .msgs =
            {
                new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "somedata"),
            },
        .wcommitted = 2,
    });
  }
  {
    std::vector<state_machine_builer_pair> peers;
    emplace_nil_peer(peers);
    emplace_nil_peer(peers);
    emplace_nil_peer(peers);
    auto nt = new_network(std::move(peers));
    test_cases.push_back({
        .nw = std::move(nt),
        .msgs =
            {
                new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "somedata"),
                new_pb_message(1, 2, raftpb::message_type::MSG_HUP),
                new_pb_message(1, 2, raftpb::message_type::MSG_PROP, "somedata"),
            },
        .wcommitted = 4,
    });
  }
  for (auto &test_case : test_cases) {
    auto &nw = test_case.nw;
    nw.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
    {
      auto &raft_handle = *nw.peers.at(1).raft_handle;
      ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER), magic_enum::enum_name(raft_handle.state_type_));
    }

    for (auto msg : test_case.msgs) {
      nw.send({msg});
    }

    for (auto &[j, x] : nw.peers) {
      auto &raft_handle = *x.raft_handle;
      ASSERT_EQ(test_case.wcommitted, raft_handle.raft_log_handle_.committed())
          << fmt::format("id: {} committed not math expected", j);

      lepton::pb::repeated_entry entries;
      auto next_entries = next_ents(raft_handle, *nw.storage.at(j));
      for (auto &entry : next_entries) {
        if (entry.has_data()) {
          entries.Add()->CopyFrom(entry);
        }
      }

      lepton::pb::repeated_message msgs;
      for (auto &msg : test_case.msgs) {
        if (msg.type() == raftpb::message_type::MSG_PROP) {
          // Only add proposal messages to the output.
          msgs.Add()->CopyFrom(msg);
        }
      }

      // 验证各个节点里存储的数据和预期的数据一致
      for (auto i = 0; i < entries.size(); ++i) {
        ASSERT_EQ(entries[i].data(), msgs[i].entries(0).data())
            << "Entries mismatch at index " << i << ": expected " << entries[i].data() << ", got "
            << msgs[i].entries(0).data();
      }
    }
  }
}

// TestLearnerLogReplication tests that a learner can receive entries from the leader.
TEST_F(raft_test_suit, learner_log_replication) {
  auto n1 = new_test_learner_raft(
      1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({with_peers({1}), with_learners({2})})));
  auto n2 = new_test_learner_raft(
      2, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({with_peers({1}), with_learners({2})})));
  std::vector<state_machine_builer_pair> peers;
  peers.emplace_back(state_machine_builer_pair{n1});
  peers.emplace_back(state_machine_builer_pair{n2});
  auto nt = new_network(std::move(peers));

  n1.become_follower(1, NONE);
  n2.become_follower(1, NONE);
  ASSERT_EQ(0, n1.raft_log_handle_.committed());
  ASSERT_EQ(0, n2.raft_log_handle_.committed());

  set_randomized_election_timeout(n1, n1.election_timeout_);
  for (auto i = 0; i < n1.election_timeout_; ++i) {
    n1.tick();
  }
  n1.advance_messages_after_append();
  // raft leader(node 1) 因为触发leader election，会发送一个 empty_ent，所以 commit 变为1；
  // 但是此时leader没有发送心跳消息，所以leadner commit仍然为0
  ASSERT_EQ(1, n1.raft_log_handle_.committed());
  ASSERT_EQ(0, n2.raft_log_handle_.committed());

  // 发送心跳消息以后，learner 与 leader 的 commit 保持一致
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_BEAT)});
  ASSERT_EQ(n1.raft_log_handle_.committed(), n2.raft_log_handle_.committed());

  // n1 is leader and n2 is learner
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER), magic_enum::enum_name(n1.state_type_));
  ASSERT_TRUE(n2.is_learner_);
  // 有leader以后，向leader发送数据，learner应该与leader保持一致
  std::uint64_t next_committed = 2;
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "sometestdata")});

  ASSERT_EQ(next_committed, n1.raft_log_handle_.committed());
  ASSERT_EQ(n1.raft_log_handle_.committed(), n2.raft_log_handle_.committed());
  ASSERT_EQ(n2.raft_log_handle_.committed(), n1.trk_.progress_map_view().view().at(2).match());
}

TEST_F(raft_test_suit, test_single_node_commit) {
  auto r = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({with_peers({1})})));
  std::vector<state_machine_builer_pair> peers;
  peers.emplace_back(state_machine_builer_pair{r});
  auto nt = new_network(std::move(peers));
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "some data")});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "some data")});

  auto &sm = *nt.peers.at(1).raft_handle;
  ASSERT_EQ(3, sm.raft_log_handle_.committed());
}

// TestCannotCommitWithoutNewTermEntry tests the entries cannot be committed
// when leader changes, no new proposal comes in and ChangeTerm proposal is
// filtered.
TEST_F(raft_test_suit, cannot_commit_without_new_term_entry) {
  std::vector<state_machine_builer_pair> peers;
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  auto nt = new_network(std::move(peers));
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER),
            magic_enum::enum_name(nt.peers.at(1).raft_handle->state_type_));
  for (auto &[id, pr] : nt.peers) {
    ASSERT_EQ(1, pr.raft_handle->raft_log_handle_.committed()) << fmt::format("id: {}", id);
  }

  // 0 cannot reach 2,3,4
  nt.cut(1, 3);
  nt.cut(1, 4);
  nt.cut(1, 5);

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "some data")});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "some data")});
  ASSERT_EQ(1, nt.peers.at(1).raft_handle->raft_log_handle_.committed());

  // network recovery
  nt.recover();
  // avoid committing ChangeTerm proposal
  nt.ignore(raftpb::message_type::MSG_APP);

  // elect 2 as the new leader with term 2
  nt.send({new_pb_message(2, 2, raftpb::message_type::MSG_HUP)});
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER),
            magic_enum::enum_name(nt.peers.at(2).raft_handle->state_type_));
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::FOLLOWER),
            magic_enum::enum_name(nt.peers.at(1).raft_handle->state_type_));

  // no log entries from previous term should be committed
  ASSERT_EQ(1, nt.peers.at(2).raft_handle->raft_log_handle_.committed());

  nt.recover();
  // send heartbeat; reset wait
  // leader 在收到follower MSG_HEARTBEAT_RESP 以后，会给 follower 发送 MSG_APPEND
  // follower 受到 MSG_APPEND 以后，如果当前 leader log index 与本地一致，则会返回成功，并更新本地commit；
  // 否则返回拒绝，返回拒绝时会反馈给follower丢失的log index 位置，方便leader选择正确的位置发送MSG_APP
  nt.send({new_pb_message(2, 2, raftpb::message_type::MSG_BEAT)});
  // node 1 选举为 leader 的 empty entry
  // node 2 选举为 leader 的 empty entry
  // 发给 node 1 的两条 PROP Message
  ASSERT_EQ(4, nt.peers.at(2).raft_handle->raft_log_handle_.committed());
  // append an entry at current term
  nt.send({new_pb_message(2, 2, raftpb::message_type::MSG_PROP, "some data")});
  // expect the committed to be advanced
  ASSERT_EQ(5, nt.peers.at(2).raft_handle->raft_log_handle_.committed());
}

TEST_F(raft_test_suit, dueling_candidates) {
  auto a = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  auto b = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  auto c = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  std::vector<state_machine_builer_pair> peers;
  peers.emplace_back(state_machine_builer_pair{a});
  peers.emplace_back(state_machine_builer_pair{b});
  peers.emplace_back(state_machine_builer_pair{c});
  auto nt = new_network(std::move(peers));

  nt.cut(1, 3);
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});

  // 1 becomes leader since it receives votes from 1 and 2
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER),
            magic_enum::enum_name(nt.peers.at(1).raft_handle->state_type_));

  // 3 stays as candidate since it receives a vote from 3 and a rejection from 2
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::CANDIDATE),
            magic_enum::enum_name(nt.peers.at(3).raft_handle->state_type_));
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::CANDIDATE, 1, 0},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.recover();

  // candidate 3 now increases its term and tries to vote again
  // we expect it to disrupt the leader 1 since it has a higher term
  // 3 will be follower again since both 1 and 2 rejects its vote request since 3 does not have a long enough log
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::FOLLOWER),
            magic_enum::enum_name(nt.peers.at(3).raft_handle->state_type_));
  {
    // 根据 Raft 协议，节点在收到更高任期的请求时必须更新自己的任期并转换为 Follower 状态。原因如下：
    /*
    日志提交安全：

      旧 Leader 可能提交了未被多数接受的日志
      新任期 Leader 必须覆盖这些不安全日志
      只有承认更高任期，才能中断旧 Leader 操作
    选举安全：
      节点更新任期后，会拒绝旧任期的投票请求
      避免同一个节点在不同分区多次投票
    状态机安全：
      强制状态转换确保所有节点最终同意最新任期
      这是实现强一致性的基础
     */
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 0},
    };
    check_raft_node_after_send_msg(tests);
  }
}

TEST_F(raft_test_suit, dueling_pre_candidates) {
  auto a_cfg =
      new_test_config(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  auto b_cfg =
      new_test_config(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  auto c_cfg =
      new_test_config(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  a_cfg.pre_vote = true;
  b_cfg.pre_vote = true;
  c_cfg.pre_vote = true;
  auto a = new_test_raft(std::move(a_cfg));
  auto b = new_test_raft(std::move(b_cfg));
  auto c = new_test_raft(std::move(c_cfg));

  std::vector<state_machine_builer_pair> peers;
  peers.emplace_back(state_machine_builer_pair{a});
  peers.emplace_back(state_machine_builer_pair{b});
  peers.emplace_back(state_machine_builer_pair{c});
  auto nt = new_network(std::move(peers));

  nt.cut(1, 3);
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});

  // 1 becomes leader since it receives votes from 1 and 2
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER),
            magic_enum::enum_name(nt.peers.at(1).raft_handle->state_type_));

  // 3 campaigns then reverts to follower when its PreVote is rejected
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::FOLLOWER),
            magic_enum::enum_name(nt.peers.at(3).raft_handle->state_type_));
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 0},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.recover();

  // Candidate 3 now increases its term and tries to vote again.
  // With PreVote, it does not disrupt the leader.
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});

  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 0},
    };
    check_raft_node_after_send_msg(tests);
  }
}

TEST_F(raft_test_suit, candidates_concede) {
  std::vector<state_machine_builer_pair> peers;
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  auto nt = new_network(std::move(peers));

  nt.isolate(1);

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::CANDIDATE, 1, 0},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::LEADER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // heal the partition
  nt.recover();
  // send heartbeat; reset wait
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_BEAT)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::LEADER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  std::string data{"force follower"};
  // send a proposal to 3 to flush out a MsgApp to 1
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_PROP, data)});
  // send heartbeat; flush out commit
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_BEAT)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 1, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::LEADER, 1, 2},
    };
    check_raft_node_after_send_msg(tests);
  }

  // 预期两个 entry
  // 第一个：leader 选举触发的empty enrty
  // 第二个：发送的 PROP 消息
  lepton::pb::repeated_entry entries;
  auto entry1 = entries.Add();
  entry1->set_term(1);
  entry1->set_index(1);
  auto entry2 = entries.Add();
  entry2->set_term(1);
  entry2->set_index(2);
  entry2->set_data(data);

  auto mm_storage_ptr = std::make_unique<lepton::memory_storage>();
  auto &mm_storage = *mm_storage_ptr;
  mm_storage.append(std::move(entries));
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  auto raft_log = new_raft_log(std::move(storage_proxy));
  auto want_log = ltoa(*raft_log);
  for (auto &[id, p] : nt.peers) {
    auto l = ltoa(p.raft_handle->raft_log_handle_);
    ASSERT_EQ(want_log, l);
    ASSERT_EQ(0, diffu(want_log, l).size());
  }
}

TEST_F(raft_test_suit, single_node_candidate) {
  std::vector<state_machine_builer_pair> peers;
  emplace_nil_peer(peers);
  auto nt = new_network(std::move(peers));

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER),
            magic_enum::enum_name(nt.peers.at(1).raft_handle->state_type_));
}

TEST_F(raft_test_suit, single_node_pre_candidate) {
  std::vector<state_machine_builer_pair> peers;
  emplace_nil_peer(peers);
  auto nt = new_network_with_config(pre_vote_config, std::move(peers));

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER),
            magic_enum::enum_name(nt.peers.at(1).raft_handle->state_type_));
}

TEST_F(raft_test_suit, old_messages) {
  std::vector<state_machine_builer_pair> peers;
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  auto nt = new_network(std::move(peers));

  // make 0 leader @ term 3
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  nt.send({new_pb_message(2, 2, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
    };
    check_raft_node_after_send_msg(tests);
  }
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 3, 3},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 3, 3},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 3, 3},
    };
    check_raft_node_after_send_msg(tests);
  }
  // pretend we're an old leader trying to make progress; this entry is expected to be ignored.
  auto msg1 = new_pb_message(2, 1, raftpb::message_type::MSG_APP);
  msg1.set_term(2);
  auto entries1 = create_entries(3, {2});
  msg1.mutable_entries()->Swap(&entries1);
  nt.send({msg1});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 3, 3},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 3, 3},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 3, 3},
    };
    check_raft_node_after_send_msg(tests);
  }
  // commit a new entry
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "somedata")});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 3, 4},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 3, 4},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 3, 4},
    };
    check_raft_node_after_send_msg(tests);
  }

  auto entries2 = create_entries(0, {0, 1, 2, 3, 3});
  entries2[4].set_data("somedata");
  auto mm_storage_ptr = std::make_unique<lepton::memory_storage>();
  auto &mm_storage = *mm_storage_ptr;
  mm_storage.append(std::move(entries2));
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  auto ilog = new_raft_log(std::move(storage_proxy));
  auto base = ltoa(*ilog);
  for (auto &[id, p] : nt.peers) {
    auto l = ltoa(p.raft_handle->raft_log_handle_);
    ASSERT_EQ(base, l) << fmt::format("====== id: {}\n", id) << fmt::format("====== base: {}\n", base)
                       << fmt::format("====== l: {}", l);
    ASSERT_EQ(0, diffu(base, l).size());
  }
}

TEST_F(raft_test_suit, proposal) {
  struct test_case {
    network nt;
    bool success;
    test_case(network &&network, bool success) : nt(std::move(network)), success(success) {}
  };
  std::vector<test_case> test_cases;
  {
    std::vector<state_machine_builer_pair> peers;
    emplace_nil_peer(peers);
    emplace_nil_peer(peers);
    emplace_nil_peer(peers);
    test_cases.emplace_back(new_network(std::move(peers)), true);
  }
  {
    std::vector<state_machine_builer_pair> peers;
    emplace_nil_peer(peers);
    emplace_nil_peer(peers);
    emplace_nop_stepper(peers);
    test_cases.emplace_back(new_network(std::move(peers)), true);
  }
  {
    std::vector<state_machine_builer_pair> peers;
    emplace_nil_peer(peers);
    emplace_nop_stepper(peers);
    emplace_nop_stepper(peers);
    test_cases.emplace_back(new_network(std::move(peers)), false);
  }
  {
    std::vector<state_machine_builer_pair> peers;
    emplace_nil_peer(peers);
    emplace_nop_stepper(peers);
    emplace_nop_stepper(peers);
    emplace_nil_peer(peers);
    test_cases.emplace_back(new_network(std::move(peers)), false);
  }
  {
    std::vector<state_machine_builer_pair> peers;
    emplace_nil_peer(peers);
    emplace_nop_stepper(peers);
    emplace_nop_stepper(peers);
    emplace_nil_peer(peers);
    emplace_nil_peer(peers);
    test_cases.emplace_back(new_network(std::move(peers)), true);
  }
  for (std::size_t i = 0; i < test_cases.size(); ++i) {
    auto &tt = test_cases[i];

    std::string data{"somedata"};
    // promote 1 to become leader
    tt.nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
    tt.nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_PROP, data)});
    auto mm_storage_ptr = std::make_unique<lepton::memory_storage>();
    if (tt.success) {
      auto &mm_storage = *mm_storage_ptr;
      lepton::pb::repeated_entry entries;
      auto entry1 = entries.Add();
      entry1->set_term(1);
      entry1->set_index(1);
      auto entry2 = entries.Add();
      entry2->set_term(1);
      entry2->set_index(2);
      entry2->set_data(std::string{data});
      mm_storage.append(std::move(entries));
    }
    pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
    auto raft_log = new_raft_log(std::move(storage_proxy));
    auto base = ltoa(*raft_log);
    for (std::size_t idx = 0; idx < tt.nt.peers.size(); ++idx) {
      for (auto &[id, p] : tt.nt.peers) {
        if (p.raft_handle == nullptr) {
          ASSERT_TRUE(p.black_hole_builder.has_value());
          std::cout << fmt::format("#{}: peer {} empty log\n", i, id);
          continue;
        }
        ASSERT_FALSE(p.black_hole_builder.has_value());
        ASSERT_NE(nullptr, p.raft_handle) << fmt::format("====== has exception test case: {}\n", i);
        auto l = ltoa(p.raft_handle->raft_log_handle_);
        ASSERT_EQ(base, l) << fmt::format("====== has exception test case: {}\n", i)
                           << fmt::format("====== id: {}\n", id) << fmt::format("====== expected: {}\n", base)
                           << fmt::format("====== actual: {}", l);
        ASSERT_EQ(0, diffu(base, l).size());
        ASSERT_EQ(1, tt.nt.peers.at(1).raft_handle->term_);
      }
    }
  }
}

TEST_F(raft_test_suit, proposal_by_proxy) {
  struct test_case {
    network nt;
    test_case(network &&network) : nt(std::move(network)) {}
  };
  std::vector<test_case> test_cases;
  {
    std::vector<state_machine_builer_pair> peers;
    emplace_nil_peer(peers);
    emplace_nil_peer(peers);
    emplace_nil_peer(peers);
    test_cases.emplace_back(new_network(std::move(peers)));
  }
  {
    std::vector<state_machine_builer_pair> peers;
    emplace_nil_peer(peers);
    emplace_nil_peer(peers);
    emplace_nop_stepper(peers);
    test_cases.emplace_back(new_network(std::move(peers)));
  }
  for (std::size_t i = 0; i < test_cases.size(); ++i) {
    auto &tt = test_cases[i];

    std::string data{"somedata"};
    // promote 1 to become leader
    tt.nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
    // propose via follower
    tt.nt.send({new_pb_message(2, 2, raftpb::message_type::MSG_PROP, data)});
    auto mm_storage_ptr = std::make_unique<lepton::memory_storage>();
    auto &mm_storage = *mm_storage_ptr;
    lepton::pb::repeated_entry entries;
    auto entry1 = entries.Add();
    entry1->set_term(1);
    entry1->set_index(1);
    auto entry2 = entries.Add();
    entry2->set_term(1);
    entry2->set_index(2);
    entry2->set_data(std::string{data});
    mm_storage.append(std::move(entries));
    pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
    auto raft_log = new_raft_log(std::move(storage_proxy));
    auto base = ltoa(*raft_log);
    for (std::size_t idx = 0; idx < tt.nt.peers.size(); ++idx) {
      for (auto &[id, p] : tt.nt.peers) {
        if (p.raft_handle == nullptr) {
          ASSERT_TRUE(p.black_hole_builder.has_value());
          std::cout << fmt::format("#{}: peer {} empty log\n", i, id);
          continue;
        }
        ASSERT_FALSE(p.black_hole_builder.has_value());
        ASSERT_NE(nullptr, p.raft_handle) << fmt::format("====== has exception test case: {}\n", i);
        auto l = ltoa(p.raft_handle->raft_log_handle_);
        ASSERT_EQ(base, l) << fmt::format("====== has exception test case: {}\n", i)
                           << fmt::format("====== id: {}\n", id) << fmt::format("====== expected: {}\n", base)
                           << fmt::format("====== actual: {}", l);
        ASSERT_EQ(0, diffu(base, l).size());
        ASSERT_EQ(1, tt.nt.peers.at(1).raft_handle->term_);
      }
    }
  }
}

TEST_F(raft_test_suit, commit) {
  struct test_case {
    std::vector<std::uint64_t> matches;
    lepton::pb::repeated_entry logs;
    std::uint64_t sm_term;
    std::uint64_t want;
  };

  std::vector<test_case> tests{
      // single
      {
          .matches = std::vector<std::uint64_t>{1},
          .logs = create_entries(1, {1}),
          .sm_term = 1,
          .want = 1,
      },
      {
          .matches = std::vector<std::uint64_t>{1},
          .logs = create_entries(1, {1}),
          .sm_term = 2,
          .want = 0,
      },
      {
          .matches = std::vector<std::uint64_t>{2},
          .logs = create_entries(1, {1, 2}),
          .sm_term = 2,
          .want = 2,
      },
      {
          .matches = std::vector<std::uint64_t>{1},
          .logs = create_entries(1, {2}),
          .sm_term = 2,
          .want = 1,
      },

      // odd
      {
          .matches = std::vector<std::uint64_t>{2, 1, 1},
          .logs = create_entries(1, {1, 2}),
          .sm_term = 1,
          .want = 1,
      },
      {
          .matches = std::vector<std::uint64_t>{2, 1, 1},
          .logs = create_entries(1, {1, 1}),
          .sm_term = 2,
          .want = 0,
      },
      {
          .matches = std::vector<std::uint64_t>{2, 1, 2},
          .logs = create_entries(1, {1, 2}),
          .sm_term = 2,
          .want = 2,
      },
      {
          .matches = std::vector<std::uint64_t>{2, 1, 2},
          .logs = create_entries(1, {1, 1}),
          .sm_term = 2,
          .want = 0,
      },

      // even
      {
          .matches = std::vector<std::uint64_t>{2, 1, 1, 1},
          .logs = create_entries(1, {1, 2}),
          .sm_term = 1,
          .want = 1,
      },
      {
          .matches = std::vector<std::uint64_t>{2, 1, 1, 1},
          .logs = create_entries(1, {1, 1}),
          .sm_term = 2,
          .want = 0,
      },
      {
          .matches = std::vector<std::uint64_t>{2, 1, 1, 2},
          .logs = create_entries(1, {1, 2}),
          .sm_term = 1,
          .want = 1,
      },
      {
          .matches = std::vector<std::uint64_t>{2, 1, 1, 2},
          .logs = create_entries(1, {1, 1}),
          .sm_term = 2,
          .want = 0,
      },
      {
          .matches = std::vector<std::uint64_t>{2, 1, 2, 2},
          .logs = create_entries(1, {1, 2}),
          .sm_term = 2,
          .want = 2,
      },
      {
          .matches = std::vector<std::uint64_t>{2, 1, 2, 2},
          .logs = create_entries(1, {1, 1}),
          .sm_term = 2,
          .want = 0,
      },
  };

  int test_case_idx = -1;
  for (auto &iter : tests) {
    ++test_case_idx;
    auto mm_storage = new_test_memory_storage({with_peers({1})});
    mm_storage.append(std::move(iter.logs));
    raftpb::hard_state hs;
    hs.set_term(iter.sm_term);
    mm_storage.set_hard_state(hs);

    auto sm = new_test_raft(1, 10, 2, pro::make_proxy<storage_builer>(std::move(mm_storage)));
    for (std::size_t j = 0; j < iter.matches.size(); ++j) {
      auto id = j + 1;
      if (id > 1) {
        sm.apply_conf_change(
            lepton::pb::conf_change_as_v2(create_conf_change(id, raftpb::conf_change_type::CONF_CHANGE_ADD_NODE)));
      }
      auto &pr = sm.trk_.progress_map_mutable_view().mutable_view().at(id);
      pr.set_match(iter.matches[j]);
      pr.set_next(iter.matches[j] + 1);
    }
    sm.maybe_commit();
    ASSERT_EQ(iter.want, sm.raft_log_handle_.committed()) << fmt::format("has exception test case: {}", test_case_idx);
  }
}

TEST_F(raft_test_suit, past_election_timeout) {
  struct test_case {
    int elapse;
    double wprobability;
    bool round;
  };

  std::vector<test_case> tests{
      {.elapse = 5, .wprobability = 0, .round = false},
      {10, 0.1, true},
      {13, 0.4, true},
      {15, 0.6, true},
      {18, 0.9, true},
      {20, 1, false},
  };

  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto &tt = tests[i];
    auto mm_storage = new_test_memory_storage({with_peers({1})});
    auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(std::move(mm_storage)));
    sm.election_elapsed_ = tt.elapse;
    auto c = 0;
    for (auto j = 0; j < 10000; ++j) {
      sm.reset_randomized_election_timeout();
      if (sm.past_election_timeout()) {
        c++;
      }
    }
    auto got = static_cast<double>(c) / 10000.0;
    if (tt.round) {  // 如果需要四舍五入
      got = std::floor(got * 10 + 0.5) / 10.0;
    }
    ASSERT_EQ(tt.wprobability, got) << fmt::format("has exception test case: {}", i);
  }
}

// TestStepIgnoreOldTermMsg to ensure that the Step function ignores the message
// from old term and does not pass it to the actual stepX function.
TEST_F(raft_test_suit, step_ignore_old_term_msg) {
  auto called = false;
  auto sm =
      new_test_learner_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1})}})));
  sm.step_func_ = [&](lepton::raft &_, raftpb::message &&msg) -> leaf::result<void> {
    called = true;
    return {};
  };
  sm.term_ = 2;
  raftpb::message msg;
  msg.set_type(raftpb::message_type::MSG_APP);
  msg.set_term(sm.term_ - 1);
  sm.step(std::move(msg));
  ASSERT_FALSE(called);
}

// TestHandleMsgApp ensures:
//  1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
//  2. If an existing entry conflicts with a new one (same index but different terms),
//     delete the existing entry and all that follow it; append any new entries not already in the log.
//  3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
TEST_F(raft_test_suit, handle_msg_app) {
  struct test_case {
    raftpb::message m;
    std::uint64_t w_index;
    std::uint64_t w_commit;
    bool w_reject;
  };

  std::vector<test_case> tests{
      // 确保1: 日志不匹配的情况
      {.m = convert_test_pb_message({
           // previous log mismatch
           .msg_type = raftpb::message_type::MSG_APP,
           .term = 2,
           .log_term = 3,
           .index = 2,
           .commit = 3,
           .entries = {},
       }),
       .w_index = 2,
       .w_commit = 0,
       .w_reject = true},

      {.m = convert_test_pb_message({
           // previous log non-exist
           .msg_type = raftpb::message_type::MSG_APP,
           .term = 2,
           .log_term = 3,
           .index = 3,
           .commit = 3,
           .entries = {},
       }),
       .w_index = 2,
       .w_commit = 0,
       .w_reject = true},

      // 确保2: 各种日志追加场景
      {.m = convert_test_pb_message({
           .msg_type = raftpb::message_type::MSG_APP,
           .term = 2,
           .log_term = 1,
           .index = 1,
           .commit = 1,
           .entries = {},
       }),
       .w_index = 2,
       .w_commit = 1,
       .w_reject = false},

      {.m = convert_test_pb_message({
           .msg_type = raftpb::message_type::MSG_APP,
           .term = 2,
           .log_term = 0,
           .index = 0,
           .commit = 1,
           .entries = {test_pb::entry{.index = 1, .term = 2}},
       }),
       .w_index = 1,
       .w_commit = 1,
       .w_reject = false},

      {.m = convert_test_pb_message({
           .msg_type = raftpb::message_type::MSG_APP,
           .term = 2,
           .log_term = 2,
           .index = 2,
           .commit = 3,
           .entries = {test_pb::entry{.index = 3, .term = 2}, test_pb::entry{.index = 4, .term = 2}},
       }),
       .w_index = 4,
       .w_commit = 3,
       .w_reject = false},

      {.m = convert_test_pb_message({
           .msg_type = raftpb::message_type::MSG_APP,
           .term = 2,
           .log_term = 2,
           .index = 2,
           .commit = 4,
           .entries = {test_pb::entry{.index = 3, .term = 2}},
       }),
       .w_index = 3,
       .w_commit = 3,
       .w_reject = false},

      {.m = convert_test_pb_message({
           .msg_type = raftpb::message_type::MSG_APP,
           .term = 2,
           .log_term = 1,
           .index = 1,
           .commit = 4,
           .entries = {test_pb::entry{.index = 2, .term = 2}},
       }),
       .w_index = 2,
       .w_commit = 2,
       .w_reject = false},

      // 确保3: 提交索引的处理
      {.m = convert_test_pb_message({
           // match entry 1, commit up to last new entry 1
           .msg_type = raftpb::message_type::MSG_APP,
           .term = 1,
           .log_term = 1,
           .index = 1,
           .commit = 3,
           .entries = {},
       }),
       .w_index = 2,
       .w_commit = 1,
       .w_reject = false},

      {.m = convert_test_pb_message({
           // match entry 1, commit up to last new entry 2
           .msg_type = raftpb::message_type::MSG_APP,
           .term = 1,
           .log_term = 1,
           .index = 1,
           .commit = 3,
           .entries = {test_pb::entry{.index = 2, .term = 2}},
       }),
       .w_index = 2,
       .w_commit = 2,
       .w_reject = false},

      {.m = convert_test_pb_message({
           // match entry 2, commit up to last new entry 2
           .msg_type = raftpb::message_type::MSG_APP,
           .term = 2,
           .log_term = 2,
           .index = 2,
           .commit = 3,
           .entries = {},
       }),
       .w_index = 2,
       .w_commit = 2,
       .w_reject = false},

      {.m = convert_test_pb_message({
           // commit up to log.last()
           .msg_type = raftpb::message_type::MSG_APP,
           .term = 2,
           .log_term = 2,
           .index = 2,
           .commit = 4,
           .entries = {},
       }),
       .w_index = 2,
       .w_commit = 2,
       .w_reject = false}};

  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto &tt = tests[i];
    auto mm_storage = new_test_memory_storage({with_peers({1})});
    ASSERT_TRUE(mm_storage.append(create_entries(1, {1, 2})));
    auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(std::move(mm_storage)));
    sm.become_follower(2, NONE);

    sm.handle_append_entries(std::move(tt.m));
    ASSERT_EQ(tt.w_index, sm.raft_log_handle_.last_index()) << fmt::format("#{}\n", i);
    ASSERT_EQ(tt.w_commit, sm.raft_log_handle_.committed()) << fmt::format("#{}\n", i);
    auto m = sm.read_messages();
    ASSERT_EQ(1, m.size()) << fmt::format("#{}\n", i);
    ASSERT_EQ(tt.w_reject, m[0].reject()) << fmt::format("#{}\n", i);
  }
}

// TestHandleHeartbeat ensures that the follower commits to the commit in the message.
TEST_F(raft_test_suit, handle_heartbeat) {
  constexpr std::uint64_t commit = 2;
  struct test_case {
    raftpb::message m;
    std::uint64_t w_commit;
  };

  std::vector<test_case> tests{
      {
          .m = convert_test_pb_message({
              .msg_type = raftpb::message_type::MSG_HEARTBEAT,
              .from = 2,
              .to = 1,
              .term = 2,
              .commit = commit + 1,
          }),
          .w_commit = commit + 1,
      },
      {
          // do not decrease commit
          .m = convert_test_pb_message({
              .msg_type = raftpb::message_type::MSG_HEARTBEAT,
              .from = 2,
              .to = 1,
              .term = 2,
              .commit = commit - 1,
          }),
          .w_commit = commit,
      },
  };
  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto &tt = tests[i];
    auto mm_storage = new_test_memory_storage({with_peers({1, 2})});
    ASSERT_TRUE(mm_storage.append(create_entries(1, {1, 2, 3})));
    auto sm = new_test_raft(1, 5, 1, pro::make_proxy<storage_builer>(std::move(mm_storage)));
    sm.become_follower(2, 2);
    sm.raft_log_handle_.commit_to(commit);
    sm.handle_heartbeat(std::move(tt.m));
    ASSERT_EQ(tt.w_commit, sm.raft_log_handle_.committed());
    auto m = sm.read_messages();
    ASSERT_EQ(1, m.size());
    ASSERT_EQ(raftpb::message_type::MSG_HEARTBEAT_RESP, m[0].type());
  }
}

// TestHandleHeartbeatResp ensures that we re-send log entries when we get a heartbeat response.
TEST_F(raft_test_suit, handle_heartbeat_resp) {
  auto mm_storage = new_test_memory_storage({with_peers({1, 2})});
  ASSERT_TRUE(mm_storage.append(create_entries(1, {1, 2, 3})));
  auto sm = new_test_raft(1, 5, 1, pro::make_proxy<storage_builer>(std::move(mm_storage)));
  sm.become_candidate();
  sm.become_leader();
  sm.raft_log_handle_.commit_to(sm.raft_log_handle_.last_index());

  // A heartbeat response from a node that is behind; re-send MsgApp
  sm.step(convert_test_pb_message(test_pb::message{.msg_type = raftpb::message_type::MSG_HEARTBEAT_RESP, .from = 2}));
  auto msgs = sm.read_messages();
  ASSERT_EQ(1, msgs.size());
  ASSERT_EQ(magic_enum::enum_name(raftpb::message_type::MSG_APP), magic_enum::enum_name(msgs[0].type()));

  // A second heartbeat response generates another MsgApp re-send
  sm.step(convert_test_pb_message(test_pb::message{.msg_type = raftpb::message_type::MSG_HEARTBEAT_RESP, .from = 2}));
  msgs = sm.read_messages();
  ASSERT_EQ(1, msgs.size());
  ASSERT_EQ(magic_enum::enum_name(raftpb::message_type::MSG_APP), magic_enum::enum_name(msgs[0].type()));

  // Once we have an MsgAppResp, heartbeats no longer send MsgApp.
  sm.step(convert_test_pb_message(
      test_pb::message{.msg_type = raftpb::message_type::MSG_APP_RESP,
                       .from = 2,
                       .index = msgs[0].index() + static_cast<std::uint64_t>(msgs[0].entries_size())}));
  // Consume the message sent in response to MsgAppResp
  msgs = sm.read_messages();
  ASSERT_EQ(0, msgs.size());

  // 伪造 MSG_HEARTBEAT_APP index 已确认，不会再发 MSG_APP 补齐 entry
  sm.step(convert_test_pb_message(test_pb::message{.msg_type = raftpb::message_type::MSG_HEARTBEAT_RESP, .from = 2}));
  msgs = sm.read_messages();
  ASSERT_EQ(0, msgs.size());
}

// TestRaftFreesReadOnlyMem ensures raft will free read request from
// readOnly readIndexQueue and pendingReadIndex map.
// related issue: https://github.com/etcd-io/etcd/issues/7571
TEST_F(raft_test_suit, raft_frees_read_only_mem) {
  auto mm_storage = new_test_memory_storage({with_peers({1, 2})});
  ASSERT_TRUE(mm_storage.append(create_entries(1, {1, 2, 3})));
  auto sm = new_test_raft(1, 5, 1, pro::make_proxy<storage_builer>(std::move(mm_storage)));
  sm.become_candidate();
  sm.become_leader();
  sm.raft_log_handle_.commit_to(sm.raft_log_handle_.last_index());

  std::string ctx{"ctx"};

  // leader starts linearizable read request.
  // more info: raft dissertation 6.4, step 2.
  sm.step(convert_test_pb_message(
      test_pb::message{.msg_type = raftpb::message_type::MSG_READ_INDEX, .from = 2, .entries = {{.data = ctx}}}));
  auto msgs = sm.read_messages();
  ASSERT_EQ(1, msgs.size());
  ASSERT_EQ(magic_enum::enum_name(raftpb::message_type::MSG_HEARTBEAT), magic_enum::enum_name(msgs[0].type()));
  ASSERT_EQ(ctx, msgs[0].context());
  ASSERT_EQ(1, sm.read_only_.read_index_queue().size());
  ASSERT_EQ(1, sm.read_only_.pending_read_index().size());
  ASSERT_TRUE(sm.read_only_.pending_read_index().contains(ctx));

  // heartbeat responses from majority of followers (1 in this case)
  // acknowledge the authority of the leader.
  // more info: raft dissertation 6.4, step 3.
  sm.step(convert_test_pb_message(
      test_pb::message{.msg_type = raftpb::message_type::MSG_HEARTBEAT_RESP, .from = 2, .ctx = ctx}));
  ASSERT_EQ(0, sm.read_only_.read_index_queue().size());
  ASSERT_EQ(0, sm.read_only_.pending_read_index().size());
  ASSERT_FALSE(sm.read_only_.pending_read_index().contains(ctx));
}

// TestMsgAppRespWaitReset verifies the resume behavior of a leader
// MsgAppResp.
TEST_F(raft_test_suit, msg_app_resp_wait_reset) {
  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1, 2, 3})});
  auto &mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  auto sm = new_test_raft(1, 5, 1, std::move(storage_proxy));
  sm.become_candidate();
  sm.become_leader();
  ASSERT_EQ(0, sm.raft_log_handle_.committed());

  // Run n1 which includes sending a message like the below
  // one to n2, but also appending to its own log.
  next_ents(sm, mm_storage);
  ASSERT_EQ(0, sm.raft_log_handle_.committed());

  // Node 2 acks the first entry, making it committed.
  // ACK Leader 选举触发的 EMPTY Entry
  sm.step(convert_test_pb_message({.msg_type = raftpb::message_type::MSG_APP_RESP, .from = 2, .index = 1}));
  ASSERT_EQ(1, sm.raft_log_handle_.committed());

  // Also consume the MsgApp messages that update Commit on the followers.
  sm.read_messages();

  // A new command is now proposed on node 1.
  sm.step(convert_test_pb_message({.msg_type = raftpb::message_type::MSG_PROP, .from = 1, .entries = {{}}}));
  // 没有收到 MSG_APP_RESP 消息,commit 没有变化
  ASSERT_EQ(1, sm.raft_log_handle_.committed());

  // The command is broadcast to all nodes not in the wait state.
  // Node 2 left the wait state due to its MsgAppResp, but node 3 is still waiting.
  // 虽然有了新的 MSG_PROP 消息, 但这里 leader 并没有向 Node 3 发送 MSG_APP 消息, 因为 Node 3 没有回复 MSG_APP_RESP
  auto msgs = sm.read_messages();
  ASSERT_EQ(1, msgs.size());
  ASSERT_EQ(magic_enum::enum_name(raftpb::message_type::MSG_APP), magic_enum::enum_name(msgs[0].type()));
  ASSERT_EQ(2, msgs[0].to());
  // 各个成员节点状态没有变化, term 也不会变化
  ASSERT_EQ(1, msgs[0].term());
  ASSERT_EQ(1, msgs[0].entries_size());
  ASSERT_EQ(2, msgs[0].entries(0).index());
  // 没有收到 MSG_APP_RESP 消息,commit 没有变化
  ASSERT_EQ(1, sm.raft_log_handle_.committed());

  // Now Node 3 acks the first entry. This releases the wait and entry 2 is sent.
  sm.step(convert_test_pb_message({.msg_type = raftpb::message_type::MSG_APP_RESP, .from = 3, .index = 1}));
  // Node 3 确认以后, leader 向 Node 3 发送了新的 MSG_APP 消息
  msgs = sm.read_messages();
  ASSERT_EQ(1, msgs.size());
  ASSERT_EQ(magic_enum::enum_name(raftpb::message_type::MSG_APP), magic_enum::enum_name(msgs[0].type()));
  ASSERT_EQ(3, msgs[0].to());
  // 各个成员节点状态没有变化, term 也不会变化
  ASSERT_EQ(1, msgs[0].term());
  ASSERT_EQ(1, msgs[0].entries_size());
  ASSERT_EQ(2, msgs[0].entries(0).index());
  // 收到 Node 3 的 MSG_APP_RESP 消息, 但是 index 1 已经确认, 所以 commit 不会发生变化
  ASSERT_EQ(1, sm.raft_log_handle_.committed());
}

static void test_recv_msg_vote(raftpb::message_type msg_type) {
  // 测试用例结构体 - 明确指定字段顺序
  struct test_case {
    state_type state;
    std::uint64_t index;
    std::uint64_t log_term;
    std::uint64_t vote_for;
    bool wreject;
  };

  std::vector<test_case> tests{
      {.state = state_type::FOLLOWER, .index = 0, .log_term = 0, .vote_for = NONE, .wreject = true},
      {.state = state_type::FOLLOWER, .index = 0, .log_term = 1, .vote_for = NONE, .wreject = true},
      {.state = state_type::FOLLOWER, .index = 0, .log_term = 2, .vote_for = NONE, .wreject = true},
      {.state = state_type::FOLLOWER, .index = 0, .log_term = 3, .vote_for = NONE, .wreject = false},

      {.state = state_type::FOLLOWER, .index = 1, .log_term = 0, .vote_for = NONE, .wreject = true},
      {.state = state_type::FOLLOWER, .index = 1, .log_term = 1, .vote_for = NONE, .wreject = true},
      {.state = state_type::FOLLOWER, .index = 1, .log_term = 2, .vote_for = NONE, .wreject = true},
      {.state = state_type::FOLLOWER, .index = 1, .log_term = 3, .vote_for = NONE, .wreject = false},

      {.state = state_type::FOLLOWER, .index = 2, .log_term = 0, .vote_for = NONE, .wreject = true},
      {.state = state_type::FOLLOWER, .index = 2, .log_term = 1, .vote_for = NONE, .wreject = true},
      {.state = state_type::FOLLOWER, .index = 2, .log_term = 2, .vote_for = NONE, .wreject = false},
      {.state = state_type::FOLLOWER, .index = 2, .log_term = 3, .vote_for = NONE, .wreject = false},

      {.state = state_type::FOLLOWER, .index = 3, .log_term = 0, .vote_for = NONE, .wreject = true},
      {.state = state_type::FOLLOWER, .index = 3, .log_term = 1, .vote_for = NONE, .wreject = true},
      {.state = state_type::FOLLOWER, .index = 3, .log_term = 2, .vote_for = NONE, .wreject = false},
      {.state = state_type::FOLLOWER, .index = 3, .log_term = 3, .vote_for = NONE, .wreject = false},

      {.state = state_type::FOLLOWER, .index = 3, .log_term = 2, .vote_for = 2, .wreject = false},
      {.state = state_type::FOLLOWER, .index = 3, .log_term = 2, .vote_for = 1, .wreject = true},

      {.state = state_type::LEADER, .index = 3, .log_term = 3, .vote_for = 1, .wreject = true},
      {.state = state_type::PRE_CANDIDATE, .index = 3, .log_term = 3, .vote_for = 1, .wreject = true},
      {.state = state_type::CANDIDATE, .index = 3, .log_term = 3, .vote_for = 1, .wreject = true},
  };

  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto &tt = tests[i];
    auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({with_peers({1})})));
    switch (tt.state) {
      case state_type::FOLLOWER:
        sm.step_func_ = step_follower;
        break;
      case state_type::CANDIDATE:
      case state_type::PRE_CANDIDATE:
        sm.step_func_ = step_candidate;
        break;
      case state_type::LEADER:
        sm.step_func_ = step_leader;
        break;
    }
    sm.vote_id_ = tt.vote_for;
    auto mm_storage_ptr = std::make_unique<lepton::memory_storage>();
    auto &mm_storage = *mm_storage_ptr;
    mm_storage.append(create_entries(0, {0, 2, 2}));
    pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
    auto raft_log = new_raft_log(std::move(storage_proxy));
    sm.raft_log_handle_ = std::move(*raft_log);

    // raft.Term is greater than or equal to raft.raftLog.lastTerm. In this
    // test we're only testing MsgVote responses when the campaigning node
    // has a different raft log compared to the recipient node.
    // Additionally we're verifying behaviour when the recipient node has
    // already given out its vote for its current term. We're not testing
    // what the recipient node does when receiving a message with a
    // different term number, so we simply initialize both term numbers to
    // be the same.
    auto term = std::max(sm.raft_log_handle_.last_entry_id().term, tt.log_term);
    sm.term_ = term;
    sm.step(convert_test_pb_message(
        {.msg_type = msg_type, .from = 2, .term = term, .log_term = tt.log_term, .index = tt.index}));

    auto msgs = sm.read_messages();
    ASSERT_EQ(1, msgs.size());
    ASSERT_EQ(magic_enum::enum_name(lepton::pb::vote_resp_msg_type(msg_type)), magic_enum::enum_name(msgs[0].type()));
    ASSERT_EQ(tt.wreject, msgs[0].reject()) << fmt::format("#{}", i);
  }
}

TEST_F(raft_test_suit, recv_msg_vote) { test_recv_msg_vote(raftpb::message_type::MSG_VOTE); }

TEST_F(raft_test_suit, recv_msg_pre_vote) { test_recv_msg_vote(raftpb::message_type::MSG_PRE_VOTE); }

TEST_F(raft_test_suit, state_transition) {
  struct test_case {
    state_type from_state;
    state_type to_state;
    bool wallow;
    std::uint64_t wterm;
    std::uint64_t wlead;
  };
  std::vector<test_case> tests = {
      {.from_state = state_type::FOLLOWER, .to_state = state_type::FOLLOWER, .wallow = true, .wterm = 1, .wlead = NONE},
      {.from_state = state_type::FOLLOWER,
       .to_state = state_type::PRE_CANDIDATE,
       .wallow = true,
       .wterm = 0,
       .wlead = NONE},
      {.from_state = state_type::FOLLOWER,
       .to_state = state_type::CANDIDATE,
       .wallow = true,
       .wterm = 1,
       .wlead = NONE},
      {.from_state = state_type::FOLLOWER, .to_state = state_type::LEADER, .wallow = false, .wterm = 0, .wlead = NONE},

      {.from_state = state_type::PRE_CANDIDATE,
       .to_state = state_type::FOLLOWER,
       .wallow = true,
       .wterm = 0,
       .wlead = NONE},
      {.from_state = state_type::PRE_CANDIDATE,
       .to_state = state_type::PRE_CANDIDATE,
       .wallow = true,
       .wterm = 0,
       .wlead = NONE},
      {.from_state = state_type::PRE_CANDIDATE,
       .to_state = state_type::CANDIDATE,
       .wallow = true,
       .wterm = 1,
       .wlead = NONE},
      {.from_state = state_type::PRE_CANDIDATE, .to_state = state_type::LEADER, .wallow = true, .wterm = 0, .wlead = 1},

      {.from_state = state_type::CANDIDATE,
       .to_state = state_type::FOLLOWER,
       .wallow = true,
       .wterm = 0,
       .wlead = NONE},
      {.from_state = state_type::CANDIDATE,
       .to_state = state_type::PRE_CANDIDATE,
       .wallow = true,
       .wterm = 0,
       .wlead = NONE},
      {.from_state = state_type::CANDIDATE,
       .to_state = state_type::CANDIDATE,
       .wallow = true,
       .wterm = 1,
       .wlead = NONE},
      {.from_state = state_type::CANDIDATE, .to_state = state_type::LEADER, .wallow = true, .wterm = 0, .wlead = 1},

      {.from_state = state_type::LEADER, .to_state = state_type::FOLLOWER, .wallow = true, .wterm = 1, .wlead = NONE},
      {.from_state = state_type::LEADER,
       .to_state = state_type::PRE_CANDIDATE,
       .wallow = false,
       .wterm = 0,
       .wlead = NONE},
      {.from_state = state_type::LEADER, .to_state = state_type::CANDIDATE, .wallow = false, .wterm = 0, .wlead = NONE},
      {.from_state = state_type::LEADER, .to_state = state_type::LEADER, .wallow = true, .wterm = 0, .wlead = 1},
  };
  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto &tt = tests[i];
    auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({with_peers({1})})));
    sm.state_type_ = tt.from_state;
    switch (tt.to_state) {
      case state_type::FOLLOWER:
        sm.become_follower(tt.wterm, tt.wlead);
        break;
      case state_type::CANDIDATE:
        if (!tt.wallow) {
          ASSERT_DEATH(sm.become_candidate(), "");
        } else {
          sm.become_candidate();
        }
        break;
      case state_type::PRE_CANDIDATE:
        if (!tt.wallow) {
          ASSERT_DEATH(sm.become_pre_candidate(), "");
        } else {
          sm.become_pre_candidate();
        }
        break;
      case state_type::LEADER:
        if (!tt.wallow) {
          ASSERT_DEATH(sm.become_leader(), "");
        } else {
          sm.become_leader();
        }
        break;
    }
    ASSERT_EQ(tt.wterm, sm.term_) << fmt::format("#{}", i);
    ASSERT_EQ(tt.wlead, sm.lead_) << fmt::format("#{}", i);
  }
}

TEST_F(raft_test_suit, all_server_stepdown) {
  struct test_case {
    state_type state;      // 初始状态
    state_type wstate;     // 期望结果状态
    std::uint64_t wterm;   // 期望结果任期
    std::uint64_t windex;  // 期望结果索引
  };

  std::vector<test_case> tests = {
      {.state = state_type::FOLLOWER, .wstate = state_type::FOLLOWER, .wterm = 3, .windex = 0},
      {.state = state_type::PRE_CANDIDATE, .wstate = state_type::FOLLOWER, .wterm = 3, .windex = 0},
      {.state = state_type::CANDIDATE, .wstate = state_type::FOLLOWER, .wterm = 3, .windex = 0},
      {.state = state_type::LEADER, .wstate = state_type::FOLLOWER, .wterm = 3, .windex = 1},
  };
  std::vector<raftpb::message_type> tmsg_types{raftpb::message_type::MSG_VOTE, raftpb::message_type::MSG_APP};
  std::uint64_t tterm = 3;
  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto &tt = tests[i];
    auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1, 2, 3})});
    pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
    auto sm = new_test_raft(1, 10, 1, std::move(storage_proxy));

    switch (tt.state) {
      case state_type::FOLLOWER:
        sm.become_follower(tt.wterm, NONE);
        break;
      case state_type::CANDIDATE:
        sm.become_candidate();
        break;
      case state_type::PRE_CANDIDATE:
        sm.become_pre_candidate();
        break;
      case state_type::LEADER:
        sm.become_candidate();
        sm.become_leader();
        break;
    }
    for (std::size_t j = 0; j < tmsg_types.size(); ++j) {
      auto msg_type = tmsg_types[j];
      sm.step(convert_test_pb_message({.msg_type = msg_type, .from = 2, .term = tterm, .log_term = tterm}));
      ASSERT_EQ(magic_enum::enum_name(tt.wstate), magic_enum::enum_name(sm.state_type_)) << fmt::format("#{}.{}", i, j);
      ASSERT_EQ(tt.wterm, sm.term_) << fmt::format("#{}.{}", i, j);
      ASSERT_EQ(tt.windex, sm.raft_log_handle_.last_index()) << fmt::format("#{}.{}", i, j);
      ASSERT_EQ(tt.windex, sm.raft_log_handle_.all_entries().size()) << fmt::format("#{}.{}", i, j);

      std::uint64_t wlead = 2;
      if (msg_type == raftpb::message_type::MSG_VOTE) {
        wlead = NONE;
      }
      ASSERT_EQ(wlead, sm.lead_) << fmt::format("#{}.{}", i, j);
    }
  }
}

// testCandidateResetTerm tests when a candidate receives a
// MsgHeartbeat or MsgApp from leader, "Step" resets the term
// with leader's and reverts back to follower.
static void test_candidate_reset_term(raftpb::message_type mt) {
  auto a = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  auto b = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  auto c = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  std::vector<state_machine_builer_pair> peers;
  peers.emplace_back(state_machine_builer_pair{a});
  peers.emplace_back(state_machine_builer_pair{b});
  peers.emplace_back(state_machine_builer_pair{c});
  auto nt = new_network(std::move(peers));

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // isolate 3 and increase term in rest
  nt.isolate(3);
  nt.send({new_pb_message(2, 2, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 3, 3},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 3, 3},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // trigger campaign in isolated c
  c.reset_randomized_election_timeout();
  for (int i = 0; i < c.randomized_election_timeout_; ++i) {
    c.tick();
  }
  c.advance_messages_after_append();
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 3, 3},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 3, 3},
        {nt.peers.at(3).raft_handle, lepton::state_type::CANDIDATE, 2, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.recover();

  // leader sends to isolated candidate
  // and expects candidate to revert to follower
  nt.send({convert_test_pb_message({.msg_type = mt, .from = 1, .to = 3, .term = a.term_})});
  // follower c term is reset with leader's
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 3, 3},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 3, 3},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 3, 3},
    };
    check_raft_node_after_send_msg(tests);
  }
}

TEST_F(raft_test_suit, candidate_reset_term_msg_heartbeat) {
  test_candidate_reset_term(raftpb::message_type::MSG_HEARTBEAT);
}

TEST_F(raft_test_suit, candidate_reset_term_msg_app) { test_candidate_reset_term(raftpb::message_type::MSG_APP); }

// The following three tests exercise the behavior of a (pre-)candidate when its
// own self-vote is delivered back to itself after the peer has already learned
// that it has lost the election. The self-vote should be ignored in these cases.
// 在 node 1 即使开始进入选举 (pre)candidate 状态时，如果此时收到了来自其他节点的消息得知已经有节点赢得选举
// 则 node 1 会忽略 self vote 消息
static void test_candidate_self_vote_after_lost_election(bool pre_vote) {
  auto sm_cfg =
      new_test_config(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  sm_cfg.pre_vote = pre_vote;
  auto sm = new_test_raft(std::move(sm_cfg));
  ASSERT_EQ(pre_vote, sm.pre_vote_);
  ASSERT_EQ(0, sm.trk_.votes_size());

  // n1 calls an election.
  sm.step({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  ASSERT_EQ(0, sm.trk_.votes_size());
  auto steps = sm.take_messages_after_append();
  ASSERT_EQ(1, steps.size());
  if (pre_vote) {
    ASSERT_EQ(magic_enum::enum_name(raftpb::MSG_PRE_VOTE_RESP), magic_enum::enum_name(steps[0].type()));
    ASSERT_EQ(magic_enum::enum_name(lepton::state_type::PRE_CANDIDATE), magic_enum::enum_name(sm.state_type_));
  } else {
    ASSERT_EQ(magic_enum::enum_name(raftpb::MSG_VOTE_RESP), magic_enum::enum_name(steps[0].type()));
    ASSERT_EQ(magic_enum::enum_name(lepton::state_type::CANDIDATE), magic_enum::enum_name(sm.state_type_));
  }

  // n1 hears that n2 already won the election before it has had a
  // change to sync its vote to disk and account for its self-vote.
  // Becomes a follower.
  sm.step({convert_test_pb_message(
      {.msg_type = raftpb::message_type::MSG_HEARTBEAT, .from = 2, .to = 1, .term = sm.term_})});
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::FOLLOWER), magic_enum::enum_name(sm.state_type_));
  ASSERT_EQ(0, sm.trk_.votes_size());

  // n1 remains a follower even after its self-vote is delivered.
  sm.step_or_send(std::move(steps));
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::FOLLOWER), magic_enum::enum_name(sm.state_type_));
  ASSERT_EQ(0, sm.trk_.votes_size());

  // Its self-vote does not make its way to its ProgressTracker.
  const auto [gr, rj, res] = sm.trk_.tally_votes();
  ASSERT_EQ(0, gr);
}

TEST_F(raft_test_suit, candidate_self_vote_after_lost_election) { test_candidate_self_vote_after_lost_election(false); }

TEST_F(raft_test_suit, candidate_self_vote_after_lost_election_pre_vote) {
  test_candidate_self_vote_after_lost_election(true);
}

// 验证当节点在 PreCandidate 阶段发送给自己的预投票请求（MsgPreVote） 被延迟处理（直到节点已转变为 Candidate 后）时：
// 延迟的自我预投票响应不会破坏状态机一致性
// 自我投票仅在正式投票阶段计数
// 状态转换（Candidate → Leader）严格依赖正式投票的法定人数
TEST_F(raft_test_suit, candidate_delivers_pre_candidate_self_vote_after_becoming_candidate) {
  auto sm_cfg =
      new_test_config(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  sm_cfg.pre_vote = true;
  auto sm = new_test_raft(std::move(sm_cfg));
  ASSERT_EQ(true, sm.pre_vote_);

  // n1 calls an election.
  sm.step({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  auto steps = sm.take_messages_after_append();
  ASSERT_EQ(1, steps.size());
  ASSERT_EQ(magic_enum::enum_name(raftpb::MSG_PRE_VOTE_RESP), magic_enum::enum_name(steps[0].type()));
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::PRE_CANDIDATE), magic_enum::enum_name(sm.state_type_));

  // n1 receives pre-candidate votes from both other peers before
  // voting for itself. n1 becomes a candidate.
  // NB: pre-vote messages carry the local term + 1.
  sm.step({convert_test_pb_message(
      {.msg_type = raftpb::message_type::MSG_PRE_VOTE_RESP, .from = 2, .to = 1, .term = sm.term_ + 1})});
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::PRE_CANDIDATE), magic_enum::enum_name(sm.state_type_));
  sm.step({convert_test_pb_message(
      {.msg_type = raftpb::message_type::MSG_PRE_VOTE_RESP, .from = 3, .to = 1, .term = sm.term_ + 1})});
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::CANDIDATE), magic_enum::enum_name(sm.state_type_));
  ASSERT_EQ(0, sm.trk_.votes_size());

  // n1 remains a candidate even after its delayed pre-vote self-vote is delivered.
  // 延迟收到了来自自己的 MSG_PRE_VOTE_RESP, 不影响状态机一致性
  sm.step_or_send(std::move(steps));
  ASSERT_EQ(0, sm.trk_.votes_size());
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::CANDIDATE), magic_enum::enum_name(sm.state_type_));
  // Its pre-vote self-vote does not make its way to its ProgressTracker.
  const auto [gr, rj, res] = sm.trk_.tally_votes();
  ASSERT_EQ(0, gr);

  steps = sm.take_messages_after_append();

  // A single vote from n2 does not move n1 to the leader.
  sm.step({convert_test_pb_message(
      {.msg_type = raftpb::message_type::MSG_VOTE_RESP, .from = 2, .to = 1, .term = sm.term_})});
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::CANDIDATE), magic_enum::enum_name(sm.state_type_));

  // n1 becomes the leader once its self-vote is received because now
  // quorum is reached.
  sm.step_or_send(std::move(steps));
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER), magic_enum::enum_name(sm.state_type_));
}

// 验证 Leader 在任期变更后对延迟的自我日志确认消息（MsgAppResp）的处理机制，
// 确保在发生任期变更时不会错误处理旧的自我确认消息。
TEST_F(raft_test_suit, leader_msg_app_self_ack_after_term_change) {
  auto sm = new_test_raft(1, 5, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  sm.become_candidate();
  sm.become_leader();
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER), magic_enum::enum_name(sm.state_type_));
  ASSERT_EQ(0, sm.raft_log_handle_.committed());

  // n1 proposes a write.
  auto prop_msg = new_pb_message(1, 1, raftpb::MSG_PROP);
  prop_msg.add_entries()->set_data("somedata");
  sm.step(std::move(prop_msg));
  ASSERT_EQ(0, sm.raft_log_handle_.committed());
  auto steps = sm.take_messages_after_append();

  // n1 hears that n2 is the new leader.
  // 收到更高任期的心跳，退位为 Follower
  sm.step({convert_test_pb_message(
      {.msg_type = raftpb::message_type::MSG_HEARTBEAT, .from = 2, .to = 1, .term = sm.term_ + 1})});
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::FOLLOWER), magic_enum::enum_name(sm.state_type_));
  ASSERT_EQ(0, sm.raft_log_handle_.committed());

  // n1 advances, ignoring its earlier self-ack of its MsgApp. The
  // corresponding MsgAppResp is ignored because it carries an earlier term.
  sm.step_or_send(std::move(steps));
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::FOLLOWER), magic_enum::enum_name(sm.state_type_));
  ASSERT_EQ(0, sm.raft_log_handle_.committed());
}

// Leader 在启用 checkQuorum 机制时，当持续收到法定节点的心跳响应时不会错误退位
TEST_F(raft_test_suit, leader_stepdown_when_quorum_active) {
  auto sm_cfg =
      new_test_config(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  sm_cfg.check_quorum = true;
  auto sm = new_test_raft(std::move(sm_cfg));
  ASSERT_EQ(true, sm.check_quorum_);

  sm.become_candidate();
  sm.become_leader();
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER), magic_enum::enum_name(sm.state_type_));

  for (int i = 0; i < sm.election_timeout_ + 1; ++i) {
    sm.step(
        {convert_test_pb_message({.msg_type = raftpb::message_type::MSG_HEARTBEAT_RESP, .from = 2, .term = sm.term_})});
    sm.tick();
  }
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER), magic_enum::enum_name(sm.state_type_));
}

// 与用例 leader_stepdown_when_quorum_active 验证的场景类似，开启 quorum
// 后必须得收到来自法定节点的心跳消息，否则会退化成follower
TEST_F(raft_test_suit, leader_stepdown_when_quorum_lost) {
  auto sm_cfg =
      new_test_config(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  sm_cfg.check_quorum = true;
  auto sm = new_test_raft(std::move(sm_cfg));
  ASSERT_EQ(true, sm.check_quorum_);

  sm.become_candidate();
  sm.become_leader();
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER), magic_enum::enum_name(sm.state_type_));

  for (int i = 0; i < sm.election_timeout_ + 1; ++i) {
    sm.tick();
  }
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::FOLLOWER), magic_enum::enum_name(sm.state_type_));
}

// 验证在启用了严格法定人数检查的环境中：
// ​​新领导者不会轻易当选​​（需满足超时条件）。
// ​​旧领导者失效后，新领导者能按预期取代​​（避免双主问题）。
// Raft 的 ​​electionTimeout 机制​​协同 checkQuorum 共同保障系统一致性。
TEST_F(raft_test_suit, leader_superseding_with_check_quorum) {
  auto nt = init_network({1, 2, 3}, {raft_config_quorum_hook}, {raft_quorum_hook});
  set_randomized_election_timeout(*nt.peers.at(2).raft_handle, nt.peers.at(2).raft_handle->election_timeout_ + 1);

  for (int i = 0; i < nt.peers.at(2).raft_handle->election_timeout_; ++i) {
    ASSERT_NE(nullptr, nt.peers.at(2).raft_handle);
    nt.peers.at(2).raft_handle->tick();
  }
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  // Peer b rejected c's vote since its electionElapsed had not reached to electionTimeout
  // ​​节点 b 拒绝投票​​：因为 b 的选举计时器 (electionElapsed) 尚未达到超时（10 <
  // 11）。b 认为当前领导者 a 仍可能活跃（checkQuorum 机制）。
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::CANDIDATE, 2, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  set_randomized_election_timeout(*nt.peers.at(2).raft_handle, nt.peers.at(2).raft_handle->election_timeout_);
  // Letting b's electionElapsed reach to electionTimeout
  // 让 b 的选举计时器超时（再等 10 tick）s
  for (int i = 0; i < nt.peers.at(2).raft_handle->election_timeout_; ++i) {
    ASSERT_NE(nullptr, nt.peers.at(2).raft_handle);
    nt.peers.at(2).raft_handle->tick();
  }
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::CANDIDATE, 2, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::CANDIDATE, 2, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  // 由于node 2进入candidate状态以后，没有leader；所以再次收到node3的投票消息时，会投票给node3
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 3, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 3, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::LEADER, 3, 2},
    };
    check_raft_node_after_send_msg(tests);
  }
}

// lepton raft add test suit
// 在 leader_superseding_with_check_quorum 用例的基础上开启pre_vote，避免新的leader选举后term过大
TEST_F(raft_test_suit, leader_superseding_with_check_quorum_and_pre_vote) {
  auto nt =
      init_network({1, 2, 3}, {raft_config_quorum_hook, raft_config_pre_vote}, {raft_quorum_hook, raft_pre_vote_hook});
  set_randomized_election_timeout(*nt.peers.at(2).raft_handle, nt.peers.at(2).raft_handle->election_timeout_ + 1);

  for (int i = 0; i < nt.peers.at(2).raft_handle->election_timeout_; ++i) {
    ASSERT_NE(nullptr, nt.peers.at(2).raft_handle);
    nt.peers.at(2).raft_handle->tick();
  }
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  // Peer b rejected c's vote since its electionElapsed had not reached to electionTimeout
  // ​​节点 b 拒绝投票​​：因为 b 的选举计时器 (electionElapsed) 尚未达到超时（10 <
  // 11）。b 认为当前领导者 a 仍可能活跃（checkQuorum 机制）。
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::PRE_CANDIDATE, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // Letting b's electionElapsed reach to electionTimeout
  // 让 b 的选举计时器超时（再等 10 tick）s
  set_randomized_election_timeout(*nt.peers.at(2).raft_handle, nt.peers.at(2).raft_handle->election_timeout_);
  for (int i = 0; i < nt.peers.at(2).raft_handle->election_timeout_; ++i) {
    ASSERT_NE(nullptr, nt.peers.at(2).raft_handle);
    nt.peers.at(2).raft_handle->tick();
  }
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::PRE_CANDIDATE, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::PRE_CANDIDATE, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  // 由于 node 2 和 node 3 收到来自对方的pre_vote小时时，msg_term都比当前term高，导致都能进入candidate状态
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::CANDIDATE, 2, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::CANDIDATE, 2, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 3, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 3, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::LEADER, 3, 2},
    };
    check_raft_node_after_send_msg(tests);
  }
}

TEST_F(raft_test_suit, leader_superseding_with_check_quorum_and_pre_vote_2) {
  auto nt =
      init_network({1, 2, 3}, {raft_config_quorum_hook, raft_config_pre_vote}, {raft_quorum_hook, raft_pre_vote_hook});
  set_randomized_election_timeout(*nt.peers.at(2).raft_handle, nt.peers.at(2).raft_handle->election_timeout_ + 1);

  for (int i = 0; i < nt.peers.at(2).raft_handle->election_timeout_; ++i) {
    ASSERT_NE(nullptr, nt.peers.at(2).raft_handle);
    nt.peers.at(2).raft_handle->tick();
  }
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  // Peer b rejected c's vote since its electionElapsed had not reached to electionTimeout
  // ​​节点 b 拒绝投票​​：因为 b 的选举计时器 (electionElapsed) 尚未达到超时（10 <
  // 11）。b 认为当前领导者 a 仍可能活跃（checkQuorum 机制）。
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::PRE_CANDIDATE, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  // ========================================与上一个用例区别在此================================================
  // Letting b's electionElapsed reach to electionTimeout
  // 让 b 的选举计时器超时（再等 10 + 1 tick）s
  set_randomized_election_timeout(*nt.peers.at(2).raft_handle, nt.peers.at(2).raft_handle->election_timeout_ + 1);
  for (int i = 0; i < nt.peers.at(2).raft_handle->election_timeout_; ++i) {
    ASSERT_NE(nullptr, nt.peers.at(2).raft_handle);
    nt.peers.at(2).raft_handle->tick();
  }
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::PRE_CANDIDATE, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  // 再次触发选举, 由于node3 term高所以选举为leader
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::LEADER, 2, 2},
    };
    check_raft_node_after_send_msg(tests);
  }
}

TEST_F(raft_test_suit, leader_election_with_check_quorum) {
  auto nt = init_network({1, 2, 3}, {raft_config_quorum_hook}, {raft_quorum_hook});

  set_randomized_election_timeout(*nt.peers.at(1).raft_handle, nt.peers.at(1).raft_handle->election_timeout_ + 1);
  set_randomized_election_timeout(*nt.peers.at(2).raft_handle, nt.peers.at(2).raft_handle->election_timeout_ + 2);

  // Immediately after creation, votes are cast regardless of the
  // election timeout.
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // need to reset randomizedElectionTimeout larger than electionTimeout again,
  // because the value might be reset to electionTimeout since the last state changes
  set_randomized_election_timeout(*nt.peers.at(1).raft_handle, nt.peers.at(1).raft_handle->election_timeout_ + 1);
  set_randomized_election_timeout(*nt.peers.at(2).raft_handle, nt.peers.at(2).raft_handle->election_timeout_ + 2);
  // 将 node 1 和 node 2 的 election_elapsed_ 设置为超时
  for (int i = 0; i < nt.peers.at(1).raft_handle->election_timeout_; ++i) {
    ASSERT_NE(nullptr, nt.peers.at(1).raft_handle);
    nt.peers.at(1).raft_handle->tick();
  }
  for (int i = 0; i < nt.peers.at(2).raft_handle->election_timeout_; ++i) {
    ASSERT_NE(nullptr, nt.peers.at(2).raft_handle);
    nt.peers.at(2).raft_handle->tick();
  }
  // 由于 node 1 和 node 2 超时，所以 node 3 的选举会成功，term 和 log index 会变为2
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::LEADER, 2, 2},
    };
    check_raft_node_after_send_msg(tests);
  }
}

// lepton raft add test suit
TEST_F(raft_test_suit, leader_election_with_check_quorum_and_pre_vote) {
  auto nt =
      init_network({1, 2, 3}, {raft_config_quorum_hook, raft_config_pre_vote}, {raft_quorum_hook, raft_pre_vote_hook});

  set_randomized_election_timeout(*nt.peers.at(1).raft_handle, nt.peers.at(1).raft_handle->election_timeout_ + 1);
  set_randomized_election_timeout(*nt.peers.at(2).raft_handle, nt.peers.at(2).raft_handle->election_timeout_ + 2);

  // Immediately after creation, votes are cast regardless of the
  // election timeout.
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // need to reset randomizedElectionTimeout larger than electionTimeout again,
  // because the value might be reset to electionTimeout since the last state changes
  set_randomized_election_timeout(*nt.peers.at(1).raft_handle, nt.peers.at(1).raft_handle->election_timeout_ + 1);
  set_randomized_election_timeout(*nt.peers.at(2).raft_handle, nt.peers.at(2).raft_handle->election_timeout_ + 2);
  for (int i = 0; i < nt.peers.at(1).raft_handle->election_timeout_; ++i) {
    ASSERT_NE(nullptr, nt.peers.at(1).raft_handle);
    nt.peers.at(1).raft_handle->tick();
  }
  for (int i = 0; i < nt.peers.at(2).raft_handle->election_timeout_; ++i) {
    ASSERT_NE(nullptr, nt.peers.at(2).raft_handle);
    nt.peers.at(2).raft_handle->tick();
  }
  // 因为也触发了 node 1的超时，node 1给 node 2 和 node 3 发送了heartbeat消息，所以node 3停止选举
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
}

// TestFreeStuckCandidateWithCheckQuorum ensures that a candidate with a higher term
// can disrupt the leader even if the leader still "officially" holds the lease, The
// leader is expected to step down and adopt the candidate's term
// 高 term 的 candidate 如何强制低 term 的 leader 下台，特别是在启用 checkQuorum 的场景下
// 测试目的, 验证当集群出现网络分区时：
//    高 term candidate 能否强制低 term leader 下台
//    checkQuorum 机制如何影响领导权转移
//    节点如何正确处理 term 冲突
TEST_F(raft_test_suit, free_stuck_candidate_with_check_quorum) {
  auto nt = init_network({1, 2, 3}, {raft_config_quorum_hook}, {raft_quorum_hook});

  set_randomized_election_timeout(*nt.peers.at(2).raft_handle, nt.peers.at(2).raft_handle->election_timeout_ + 1);

  // 这里对 node 2 设置超时没有什么用，由于当前没有leader，所以在收到投票消息后会选举成功
  // for (int i = 0; i < nt.peers.at(2).raft_handle->election_timeout_; ++i) {
  //   ASSERT_NE(nullptr, nt.peers.at(2).raft_handle);
  //   nt.peers.at(2).raft_handle->tick();
  // }
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // 网络隔离
  nt.isolate(1);

  // 由于 node 2 election_elapsed_ 为 0，所以忽略了来自 node 3 的投票消息
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::CANDIDATE, 2, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // Vote again for safety
  // 由于 node 2 election_elapsed_ 为 0，所以再一次忽略了来自 node 3 的投票消息
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::CANDIDATE, 3, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.recover();
  nt.send({convert_test_pb_message({.msg_type = raftpb::message_type::MSG_HEARTBEAT, .from = 1, .to = 3, .term = 1})});
  // Disrupt the leader so that the stuck peer is freed
  // 由于发现在发送了heartbeat心跳消息以后，term比当前leader的大，所以当前leader要降级
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 3, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::CANDIDATE, 3, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 4, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 4, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::LEADER, 4, 2},
    };
    check_raft_node_after_send_msg(tests);
  }
}

// 想验证当节点被移除出集群后，它的行为是否符合预期
TEST_F(raft_test_suit, non_promotable_voter_with_check_quorum) {
  // ========================== init ==========================
  std::vector<state_machine_builer_pair> peers;
  auto append_raft_node_func = [&](std::uint64_t id, std::vector<std::uint64_t> &&peer_ids) {
    auto sm_cfg = new_test_config(
        id, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers(std::move(peer_ids))}})));
    sm_cfg.check_quorum = true;
    auto sm = new_test_raft(std::move(sm_cfg));
    ASSERT_EQ(true, sm.check_quorum_);
    peers.emplace_back(state_machine_builer_pair{std::make_unique<lepton::raft>(std::move(sm))});
  };
  append_raft_node_func(1, {1, 2});
  append_raft_node_func(2, {1});
  ASSERT_FALSE(peers[1].raft_handle->promotable());
  // new_network 会尝试重新初始化progress tracker，导致已经移除的 node 2 被重新添加回来
  auto nt = new_network(std::move(peers));
  set_randomized_election_timeout(*nt.peers.at(2).raft_handle, nt.peers.at(2).raft_handle->election_timeout_ + 1);
  // Need to remove 2 again to make it a non-promotable node since newNetwork overwritten some internal states
  raftpb::conf_change cc1;
  cc1.set_node_id(2);
  cc1.set_type(raftpb::conf_change_type::CONF_CHANGE_REMOVE_NODE);
  nt.peers.at(2).raft_handle->apply_conf_change(lepton::pb::conf_change_as_v2(std::move(cc1)));
  ASSERT_FALSE(nt.peers.at(2).raft_handle->promotable());

  for (int i = 0; i < nt.peers.at(2).raft_handle->election_timeout_; ++i) {
    ASSERT_NE(nullptr, nt.peers.at(2).raft_handle);
    nt.peers.at(2).raft_handle->tick();
  }
  // 虽然已经从node 2 中移除了这个节点本身，但是由于没有真正的从网络中移除该节点，且node 1视角下 node 2
  // 仍然参与投票，所以此时node 2 任然会参与选举
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  ASSERT_EQ(1, nt.peers.at(2).raft_handle->lead_);
}

// TestDisruptiveFollower tests isolated follower,
// with slow network incoming from leader, election times out
// to become a candidate with an increased term. Then, the
// candiate's response to late leader heartbeat forces the leader
// to step down.
TEST_F(raft_test_suit, disruptive_follower) {
  auto nt = init_network({1, 2, 3}, {raft_config_quorum_hook}, {raft_quorum_hook, raft_become_follower_hook});

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // etcd server "advanceTicksForElection" on restart;
  // this is to expedite campaign trigger when given larger
  // election timeouts (e.g. multi-datacenter deploy)
  // Or leader messages are being delayed while ticks elapse
  set_randomized_election_timeout(*nt.peers.at(3).raft_handle, nt.peers.at(3).raft_handle->election_timeout_ + 2);
  for (int i = 0; i < nt.peers.at(3).raft_handle->randomized_election_timeout_ - 1; ++i) {
    ASSERT_NE(nullptr, nt.peers.at(3).raft_handle);
    nt.peers.at(3).raft_handle->tick();
  }

  // ideally, before last election tick elapses,
  // the follower n3 receives "pb.MsgApp" or "pb.MsgHeartbeat"
  // from leader n1, and then resets its "electionElapsed"
  // however, last tick may elapse before receiving any
  // messages from leader, thus triggering campaign
  nt.peers.at(3).raft_handle->tick();

  // n1 is still leader yet
  // while its heartbeat to candidate n3 is being delayed
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::CANDIDATE, 3, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // while outgoing vote requests are still queued in n3,
  // leader heartbeat finally arrives at candidate n3
  // however, due to delayed network from leader, leader
  // heartbeat was sent with lower term than candidate's
  nt.send({convert_test_pb_message({.msg_type = raftpb::message_type::MSG_HEARTBEAT, .from = 1, .to = 3, .term = 2})});

  // then candidate n3 responds with "pb.MsgAppResp" of higher term
  // and leader steps down from a message with higher term
  // this is to disrupt the current leader, so that candidate
  // with higher term can be freed with following election
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 3, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::CANDIDATE, 3, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
}

// TestDisruptiveFollowerPreVote tests isolated follower,
// with slow network incoming from leader, election times out
// to become a pre-candidate with less log than current leader.
// Then pre-vote phase prevents this isolated node from forcing
// current leader to step down, thus less disruptions.
TEST_F(raft_test_suit, disruptive_follower_pre_vote) {
  auto nt = init_network({1, 2, 3}, {raft_config_quorum_hook}, {raft_quorum_hook, raft_become_follower_hook});

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.isolate(3);
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "somedata")});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "somedata")});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "somedata")});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 4},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 4},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  auto modify_pre_vote_func = [&](std::uint64_t id) {
    bool *pre_vote = const_cast<bool *>(&nt.peers.at(id).raft_handle->pre_vote_);
    *pre_vote = true;
    ASSERT_TRUE(nt.peers.at(id).raft_handle->pre_vote_);
  };
  modify_pre_vote_func(1);
  modify_pre_vote_func(2);
  modify_pre_vote_func(3);
  nt.recover();
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 4},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 4},
        {nt.peers.at(3).raft_handle, lepton::state_type::PRE_CANDIDATE, 2, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // delayed leader heartbeat does not force current leader to step down
  nt.send({convert_test_pb_message({.msg_type = raftpb::message_type::MSG_HEARTBEAT, .from = 1, .to = 3, .term = 2})});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 4},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 4},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 4},
    };
    check_raft_node_after_send_msg(tests);
  }
}

TEST_F(raft_test_suit, read_only_option_safe) {
  auto nt = init_network({1, 2, 3}, {}, {});
  set_randomized_election_timeout(*nt.peers.at(2).raft_handle, nt.peers.at(2).raft_handle->election_timeout_ + 2);
  for (int i = 0; i < nt.peers.at(2).raft_handle->election_timeout_; ++i) {
    ASSERT_NE(nullptr, nt.peers.at(2).raft_handle);
    nt.peers.at(2).raft_handle->tick();
  }
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  struct test_case {
    raft *raft_handle;
    int proposals;
    std::uint64_t wri;
    std::string wctx;
  };
  std::vector<test_case> test_cases{
      {nt.peers.at(1).raft_handle, 10, 11, "ctx1"}, {nt.peers.at(2).raft_handle, 10, 21, "ctx2"},
      {nt.peers.at(3).raft_handle, 10, 31, "ctx3"}, {nt.peers.at(1).raft_handle, 10, 41, "ctx4"},
      {nt.peers.at(2).raft_handle, 10, 51, "ctx5"}, {nt.peers.at(3).raft_handle, 10, 61, "ctx6"},
  };
  for (std::size_t i = 0; i < test_cases.size(); ++i) {
    auto &tt = test_cases[i];
    for (int j = 0; j < tt.proposals; ++j) {
      nt.send(
          {convert_test_pb_message({.msg_type = raftpb::message_type::MSG_PROP, .from = 1, .to = 1, .entries = {{}}})});
    }
    nt.send({new_pb_message(tt.raft_handle->id_, tt.raft_handle->id_, raftpb::message_type::MSG_READ_INDEX, tt.wctx)});

    auto &r = *tt.raft_handle;
    ASSERT_FALSE(r.read_states_.empty());
    const auto &rs = r.read_states_[0];
    ASSERT_EQ(tt.wri, rs.index);
    ASSERT_EQ(tt.wctx, rs.request_ctx);
    r.read_states_.clear();
  }
}

TEST_F(raft_test_suit, read_only_with_learner) {
  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1}), with_learners({2})});
  auto &mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  auto a = new_test_learner_raft(1, 10, 1, std::move(storage_proxy));
  auto b = new_test_learner_raft(
      2, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1}), with_learners({2})}})));

  std::vector<state_machine_builer_pair> peers;
  peers.emplace_back(state_machine_builer_pair{std::make_unique<lepton::raft>(std::move(a))});
  peers.emplace_back(state_machine_builer_pair{std::make_unique<lepton::raft>(std::move(b))});
  auto nt = new_network(std::move(peers));
  set_randomized_election_timeout(*nt.peers.at(2).raft_handle, nt.peers.at(2).raft_handle->election_timeout_ + 1);
  for (int i = 0; i < nt.peers.at(2).raft_handle->election_timeout_; ++i) {
    ASSERT_NE(nullptr, nt.peers.at(2).raft_handle);
    nt.peers.at(2).raft_handle->tick();
  }
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  struct test_case {
    raft *raft_handle;
    int proposals;
    std::uint64_t wri;
    std::string wctx;
  };
  std::vector<test_case> test_cases{
      {nt.peers.at(1).raft_handle, 10, 11, "ctx1"},
      {nt.peers.at(2).raft_handle, 10, 21, "ctx2"},
      {nt.peers.at(1).raft_handle, 10, 31, "ctx3"},
      {nt.peers.at(2).raft_handle, 10, 41, "ctx4"},
  };
  for (std::size_t i = 0; i < test_cases.size(); ++i) {
    auto &tt = test_cases[i];
    for (int j = 0; j < tt.proposals; ++j) {
      nt.send(
          {convert_test_pb_message({.msg_type = raftpb::message_type::MSG_PROP, .from = 1, .to = 1, .entries = {{}}})});
      next_ents(*nt.peers.at(1).raft_handle, mm_storage);
    }
    nt.send({new_pb_message(tt.raft_handle->id_, tt.raft_handle->id_, raftpb::message_type::MSG_READ_INDEX, tt.wctx)});

    auto &r = *tt.raft_handle;
    ASSERT_FALSE(r.read_states_.empty());
    const auto &rs = r.read_states_[0];
    ASSERT_EQ(tt.wri, rs.index);
    ASSERT_EQ(tt.wctx, rs.request_ctx);
    r.read_states_.clear();
  }
}

TEST_F(raft_test_suit, read_only_option_lease) {
  auto nt = init_network({1, 2, 3}, {raft_config_quorum_hook, raft_config_read_only_lease_based},
                         {raft_quorum_hook, raft_read_only_lease_based_hook});
  set_randomized_election_timeout(*nt.peers.at(2).raft_handle, nt.peers.at(2).raft_handle->election_timeout_ + 1);
  for (int i = 0; i < nt.peers.at(2).raft_handle->election_timeout_; ++i) {
    ASSERT_NE(nullptr, nt.peers.at(2).raft_handle);
    nt.peers.at(2).raft_handle->tick();
  }
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  struct test_case {
    raft *raft_handle;
    int proposals;
    std::uint64_t wri;
    std::string wctx;
  };
  std::vector<test_case> test_cases{
      {nt.peers.at(1).raft_handle, 10, 11, "ctx1"}, {nt.peers.at(2).raft_handle, 10, 21, "ctx2"},
      {nt.peers.at(3).raft_handle, 10, 31, "ctx3"}, {nt.peers.at(1).raft_handle, 10, 41, "ctx4"},
      {nt.peers.at(2).raft_handle, 10, 51, "ctx5"}, {nt.peers.at(3).raft_handle, 10, 61, "ctx6"},
  };
  for (std::size_t i = 0; i < test_cases.size(); ++i) {
    auto &tt = test_cases[i];
    for (int j = 0; j < tt.proposals; ++j) {
      nt.send(
          {convert_test_pb_message({.msg_type = raftpb::message_type::MSG_PROP, .from = 1, .to = 1, .entries = {{}}})});
    }
    nt.send({new_pb_message(tt.raft_handle->id_, tt.raft_handle->id_, raftpb::message_type::MSG_READ_INDEX, tt.wctx)});

    auto &r = *tt.raft_handle;
    ASSERT_FALSE(r.read_states_.empty());
    const auto &rs = r.read_states_[0];
    ASSERT_EQ(tt.wri, rs.index);
    ASSERT_EQ(tt.wctx, rs.request_ctx);
    r.read_states_.clear();
  }
}

// TestReadOnlyForNewLeader ensures that a leader only accepts MsgReadIndex message
// when it commits at least one log entry at it term.
TEST_F(raft_test_suit, read_only_for_new_leader) {
  struct node_config {
    std::uint64_t id;
    std::uint64_t committed;
    std::uint64_t applied;
    std::uint64_t compact_index;
  };

  std::vector<node_config> node_configs = {{1, 1, 1, 0}, {2, 2, 2, 2}, {3, 2, 2, 2}};

  std::vector<state_machine_builer_pair> peers;
  for (const auto &c : node_configs) {
    auto ms = new_test_memory_storage({with_peers({1, 2, 3})});
    ASSERT_TRUE(ms.append(create_entries(1, {1, 1})));
    raftpb::hard_state hard_state;
    hard_state.set_term(1);
    hard_state.set_commit(c.committed);
    ms.set_hard_state(std::move(hard_state));
    if (c.compact_index != 0) {
      ms.compact(c.compact_index);
    }
    auto cfg = new_test_config(c.id, 10, 1, pro::make_proxy<storage_builer>(std::move(ms)));
    cfg.applied_index = c.applied;
    auto r = new_raft(std::move(cfg));
    assert(r);
    peers.emplace_back(state_machine_builer_pair{std::make_unique<lepton::raft>(std::move(r.value()))});
  }
  auto nt = new_network(std::move(peers));

  // Drop MsgApp to forbid peer a to commit any log entry at its term after it becomes leader.
  nt.ignore(raftpb::message_type::MSG_APP);
  // Force peer a to become leader.
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    // 只 有leader 自己收到了选举成功后生成的 Empty MSG_APP
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 3},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
    };
    check_raft_node_after_send_msg(tests);
  }

  // Ensure peer a drops read only request.
  std::uint64_t windex = 4;
  std::string wctx{"ctx"};
  auto &sm = *nt.peers.at(1).raft_handle;
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_READ_INDEX, wctx)});
  ASSERT_TRUE(sm.read_states_.empty());

  nt.recover();

  // Force peer a to commit a log entry at its term
  for (int i = 0; i < sm.heartbeat_timeout_; ++i) {
    sm.tick();
  }
  nt.send({convert_test_pb_message({.msg_type = raftpb::message_type::MSG_PROP, .from = 1, .to = 1, .entries = {{}}})});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 4},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 4},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 4},
    };
    check_raft_node_after_send_msg(tests);
  }
  auto last_log_term = sm.raft_log_handle_.zero_term_on_err_compacted(sm.raft_log_handle_.committed());
  ASSERT_EQ(sm.term_, last_log_term);

  // Ensure peer a processed postponed read only request after it committed an entry at its term.
  ASSERT_EQ(1, sm.read_states_.size());
  const auto &rs1 = sm.read_states_[0];
  ASSERT_EQ(windex, rs1.index);
  ASSERT_EQ(wctx, rs1.request_ctx);

  // Ensure peer a accepts read only request after it committed an entry at its term.
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_READ_INDEX, wctx)});
  ASSERT_EQ(2, sm.read_states_.size());
  const auto &rs2 = sm.read_states_[1];
  ASSERT_EQ(windex, rs2.index);
  ASSERT_EQ(wctx, rs2.request_ctx);
}

TEST_F(raft_test_suit, leader_app_resp) {
  struct test_case {
    uint64_t index;       // Message index received
    bool reject;          // Was the message rejected?
    uint64_t wmatch;      // Expected match index after processing
    uint64_t wnext;       // Expected next index after processing
    int wmsgNum;          // Expected number of messages generated
    uint64_t windex;      // Expected message index in response
    uint64_t wcommitted;  // Expected committed index
  };
  // initial progress: match = 0; next = 3
  std::vector<test_case> test_cases = {
      // stale response; no replies generated
      // ​​过时拒绝响应​​场景
      // 1. 不更新进度状态
      // 2. 不发送任何消息（避免无用重试）
      {3, true, 0, 3, 0, 0, 0},
      // denied resp; leader does not commit; decrease next and send probing msg
      // ​​有效拒绝响应​​
      // 1. 降低Next索引至2
      // 2. 发送单条探测消息（索引1）准备重同步
      {2, true, 0, 2, 1, 1, 0},
      // accept resp; leader commits; broadcast with commit index
      // ​​成功接受响应​​
      // 1. 提升Match=2，Next=41. 提升Match=2，Next=4
      // 2. 发送2条MsgApp（广播提交）
      // 3. 提交索引更新至2
      {2, false, 2, 4, 2, 2, 2},
      // Follower is StateProbing at 0, it sends MsgAppResp for 0 (which
      // matches the pr.Match) so it is moved to StateReplicate and as many
      // entries as possible are sent to it (1, 2, and 3). Correspondingly the
      // Next is then 4 (an Entry at 4 does not exist, indicating the follower
      // will be up to date should it process the emitted MsgApp).
      // ​​状态转换边界​​
      // 1. 保持Match=0但Next跳至4
      // 2. 发送单消息包含索引1-3
      // 3. 验证状态机转换逻辑
      {0, false, 0, 4, 1, 0, 0},
  };
  for (std::size_t i = 0; i < test_cases.size(); ++i) {
    auto &tt = test_cases[i];
    // index  term
    // 0      0
    // 1      1
    // 2      1
    auto mm_storage = new_test_memory_storage({with_peers({1, 2, 3})});
    ASSERT_TRUE(mm_storage.append(create_entries(0, {0, 1, 1})));
    auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(std::move(mm_storage)));
    sm.become_candidate();
    sm.become_leader();
    sm.read_messages();
    auto msg = convert_test_pb_message(
        {.msg_type = raftpb::message_type::MSG_APP_RESP, .from = 2, .term = sm.term_, .index = tt.index});
    msg.set_reject(tt.reject);
    msg.set_reject_hint(tt.index);
    ASSERT_TRUE(sm.step(std::move(msg)));

    auto &p = sm.trk_.progress_map_view().view().at(2);
    ASSERT_EQ(tt.wmatch, p.match());
    ASSERT_EQ(tt.wnext, p.next());

    auto msgs = sm.read_messages();
    ASSERT_EQ(tt.wmsgNum, msgs.size());
    for (auto &iter_msg : msgs) {
      ASSERT_EQ(tt.windex, iter_msg.index()) << iter_msg.DebugString();
      ASSERT_EQ(tt.wcommitted, iter_msg.commit()) << iter_msg.DebugString();
    }
  }
}

// TestBcastBeat is when the leader receives a heartbeat tick, it should
// send a MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries.
TEST_F(raft_test_suit, bcast_beat) {
  constexpr std::uint64_t offset = 1000;
  // make a state machine with log.offset = 1000
  raftpb::snapshot s = create_snapshot(offset, 1, {1, 2, 3});
  lepton::memory_storage ms;
  ms.apply_snapshot(std::move(s));
  auto storage = pro::make_proxy<storage_builer, memory_storage>(std::move(ms));
  auto sm = new_test_raft(1, 10, 1, std::move(storage));
  sm.term_ = 1;
  sm.become_candidate();
  sm.become_leader();
  for (std::uint64_t i = 0; i < 10; ++i) {
    lepton::pb::repeated_entry entries;
    auto entry = entries.Add();
    entry->set_index(i + 1);
    must_append_entry(sm, std::move(entries));
  }
  sm.advance_messages_after_append();

  // slow follower
  sm.trk_.progress_map_mutable_view().mutable_view().at(2).set_match(5);
  sm.trk_.progress_map_mutable_view().mutable_view().at(2).set_next(6);

  // normal follower
  sm.trk_.progress_map_mutable_view().mutable_view().at(3).set_match(sm.raft_log_handle_.last_index());
  sm.trk_.progress_map_mutable_view().mutable_view().at(3).set_next(sm.raft_log_handle_.last_index() + 1);

  sm.step(convert_test_pb_message({.msg_type = raftpb::message_type::MSG_BEAT}));
  auto msgs = sm.read_messages();
  ASSERT_EQ(2, msgs.size());

  std::unordered_map<std::uint64_t, std::uint64_t> want_commit_map = {
      {2, std::min(sm.raft_log_handle_.committed(), sm.trk_.progress_map_view().view().at(2).match())},
      {3, std::min(sm.raft_log_handle_.committed(), sm.trk_.progress_map_view().view().at(3).match())},
  };
  for (auto &iter_msg : msgs) {
    ASSERT_EQ(raftpb::message_type::MSG_HEARTBEAT, iter_msg.type());
    ASSERT_EQ(0, iter_msg.index());
    ASSERT_EQ(0, iter_msg.log_term());

    auto iter_commit = want_commit_map.find(iter_msg.to());
    ASSERT_NE(iter_commit, want_commit_map.end());
    ASSERT_EQ(iter_commit->second, iter_msg.commit());
    want_commit_map.erase(iter_commit);
    ASSERT_EQ(0, iter_msg.entries_size());
  }
}

// TestRecvMsgBeat tests the output of the state machine when receiving MsgBeat
TEST_F(raft_test_suit, recv_msg_beat) {
  struct test_case {
    lepton::state_type state;
    int wmsg;
  };
  std::vector<test_case> tests = {
      {.state = lepton::state_type::LEADER, .wmsg = 2},
      // candidate and follower should ignore MsgBeat
      {.state = lepton::state_type::CANDIDATE, .wmsg = 0},
      {.state = lepton::state_type::CANDIDATE, .wmsg = 0},
  };

  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto &tt = tests[i];
    auto mm_storage = new_test_memory_storage({with_peers({1, 2, 3})});
    ASSERT_TRUE(mm_storage.append(create_entries(0, {0, 1, 1})));
    auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(std::move(mm_storage)));
    sm.term_ = 1;
    sm.state_type_ = tt.state;
    switch (tt.state) {
      case state_type::FOLLOWER:
        sm.step_func_ = step_follower;
        break;
      case state_type::CANDIDATE:
      case state_type::PRE_CANDIDATE:
        sm.step_func_ = step_candidate;
        break;
      case state_type::LEADER:
        sm.step_func_ = step_leader;
        break;
    }

    sm.step(convert_test_pb_message({.msg_type = raftpb::message_type::MSG_BEAT, .from = 1, .to = 1}));
    auto msgs = sm.read_messages();
    ASSERT_EQ(tt.wmsg, msgs.size());
    for (auto &iter_msg : msgs) {
      ASSERT_EQ(raftpb::message_type::MSG_HEARTBEAT, iter_msg.type());
      ASSERT_EQ(0, iter_msg.index());
      ASSERT_EQ(0, iter_msg.log_term());
    }
  }
}

TEST_F(raft_test_suit, leader_increase_next) {
  auto previous_ents = create_entries(1, {1, 2, 3});
  struct test_case {
    // progress
    lepton::tracker::state_type state;
    std::uint64_t next;

    std::uint64_t wnext;
  };
  std::vector<test_case> tests = {
      // state replicate, optimistically increase next
      // previous entries + noop entry + propose + 1
      {.state = lepton::tracker::state_type::STATE_REPLICATE, .next = 2, .wnext = 6},
      // state probe, not optimistically increase next
      {.state = lepton::tracker::state_type::STATE_PROBE, .next = 2, .wnext = 2},
  };
  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto &tt = tests[i];
    // 初始状态有已经持久化的3个entry日志
    // 成为leader后会有一个empty entry
    auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2})}})));
    sm.raft_log_handle_.append(create_entries(1, {1, 2, 3}));
    sm.become_candidate();
    sm.become_leader();
    ASSERT_EQ(4, sm.raft_log_handle_.last_index());
    sm.trk_.progress_map_mutable_view().mutable_view().at(2).set_state(tt.state);
    sm.trk_.progress_map_mutable_view().mutable_view().at(2).set_next(tt.next);
    auto prop_msg = new_pb_message(1, 1, raftpb::MSG_PROP);
    prop_msg.add_entries()->set_data("somedata");
    sm.step(std::move(prop_msg));
    ASSERT_EQ(5, sm.raft_log_handle_.last_index());
    ASSERT_EQ(tt.wnext, sm.trk_.progress_map_view().view().at(2).next());
  }
}

/*
初始探测
   ↓
发送MsgApp1 → 进入Paused状态
   ↓
10次sendAppend()调用 → 无消息
   ↓
心跳周期 → 仅发MsgHeartbeat (保持Paused)
   ↓   ↑
   循环3轮
   ↓
收MsgHeartbeatResp → 发送MsgApp2 → 重新Paused
 */
//  验证Raft领导者对处于​​探测(probe)状态​​的follower的流量控制行为，特别是在消息发送流程暂停(MsgAppFlowPaused)状态下的正确交互
TEST_F(raft_test_suit, send_append_for_progress_probe) {
  auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2})}})));
  sm.become_candidate();
  sm.become_leader();
  sm.read_messages();
  sm.trk_.progress_map_mutable_view().mutable_view().at(2).become_probe();
  // each round is a heartbeat
  for (auto i = 0; i < 3; ++i) {
    if (i == 0) {
      // we expect that raft will only send out one msgAPP on the first
      // loop. After that, the follower is paused until a heartbeat response is
      // received.
      lepton::pb::repeated_entry entries;
      auto entry = entries.Add();
      entry->set_data("somedata");
      must_append_entry(sm, std::move(entries));
      sm.send_append(2);
      auto msgs = sm.read_messages();
      ASSERT_EQ(1, msgs.size());
      ASSERT_EQ(0, msgs[0].index());
    }

    ASSERT_TRUE(sm.trk_.progress_map_view().view().at(2).is_paused());
    for (auto j = 0; j < 10; ++j) {
      lepton::pb::repeated_entry entries;
      auto entry = entries.Add();
      entry->set_data("somedata");
      must_append_entry(sm, std::move(entries));
      sm.send_append(2);
      auto msgs = sm.read_messages();
      ASSERT_EQ(0, msgs.size());
    }

    // do a heartbeat
    for (auto j = 0; j < sm.heartbeat_timeout_; ++j) {
      sm.step(new_pb_message(1, 1, raftpb::message_type::MSG_BEAT));
    }
    ASSERT_TRUE(sm.trk_.progress_map_view().view().at(2).is_paused());

    // consume the heartbeat
    auto msgs = sm.read_messages();
    ASSERT_EQ(1, msgs.size());
    ASSERT_EQ(raftpb::message_type::MSG_HEARTBEAT, msgs[0].type());
  }

  // a heartbeat response will allow another message to be sent
  sm.step(new_pb_message(2, 1, raftpb::message_type::MSG_HEARTBEAT_RESP));
  auto msgs = sm.read_messages();
  ASSERT_EQ(1, msgs.size());
  ASSERT_EQ(0, msgs[0].index());
  ASSERT_TRUE(sm.trk_.progress_map_view().view().at(2).is_paused());
}

TEST_F(raft_test_suit, send_append_for_progress_replicate) {
  auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2})}})));
  sm.become_candidate();
  sm.become_leader();
  sm.read_messages();
  sm.trk_.progress_map_mutable_view().mutable_view().at(2).become_replicate();
  for (auto i = 0; i < 10; ++i) {
    lepton::pb::repeated_entry entries;
    auto entry = entries.Add();
    entry->set_data("somedata");
    must_append_entry(sm, std::move(entries));
    sm.send_append(2);
    auto msgs = sm.read_messages();
    ASSERT_EQ(1, msgs.size());
  }
}

TEST_F(raft_test_suit, send_append_for_progress_snapshot) {
  auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2})}})));
  sm.become_candidate();
  sm.become_leader();
  sm.read_messages();
  sm.trk_.progress_map_mutable_view().mutable_view().at(2).become_snapshot(10);
  for (auto i = 0; i < 10; ++i) {
    lepton::pb::repeated_entry entries;
    auto entry = entries.Add();
    entry->set_data("somedata");
    must_append_entry(sm, std::move(entries));
    sm.send_append(2);
    auto msgs = sm.read_messages();
    ASSERT_EQ(0, msgs.size());
  }
}

TEST_F(raft_test_suit, recv_msg_unreachable) {
  auto mm_storage = new_test_memory_storage({with_peers({1, 2})});
  ASSERT_TRUE(mm_storage.append(create_entries(1, {1, 2, 3})));
  auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(std::move(mm_storage)));
  sm.become_candidate();
  sm.become_leader();
  sm.read_messages();
  // set node 2 to state replicate
  auto &pr = sm.trk_.progress_map_mutable_view().mutable_view().at(2);
  pr.set_match(3);
  pr.become_replicate();
  pr.set_next(6);

  sm.step(new_pb_message(2, 1, raftpb::message_type::MSG_UNREACHABLE));
  ASSERT_EQ(lepton::tracker::state_type::STATE_PROBE, pr.state());
  ASSERT_EQ(pr.match() + 1, pr.next());
}

TEST_F(raft_test_suit, test_restore) {
  auto mm_storage = new_test_memory_storage({with_peers({1, 2})});
  auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(std::move(mm_storage)));
  constexpr std::uint64_t snapshot_index = 11;
  constexpr std::uint64_t snapshot_term = 11;
  const std::vector<std::uint64_t> voter_nodes = {1, 2, 3};
  ASSERT_TRUE(sm.restore(create_snapshot(snapshot_index, snapshot_term, std::vector<std::uint64_t>{voter_nodes})));

  ASSERT_EQ(snapshot_index, sm.raft_log_handle_.last_index());
  auto term = sm.raft_log_handle_.term(snapshot_index);
  ASSERT_TRUE(term.has_value());
  ASSERT_EQ(term.value(), snapshot_term);
  ASSERT_EQ(voter_nodes, sm.trk_.voter_nodes());

  ASSERT_FALSE(sm.restore(create_snapshot(snapshot_index, snapshot_term, {1, 2, 3})));
  for (auto i = 0; i < sm.randomized_election_timeout_; ++i) {
    sm.tick();
  }
  ASSERT_EQ(lepton::state_type::FOLLOWER, sm.state_type_);
}

// TestRestoreWithLearner restores a snapshot which contains learners.
TEST_F(raft_test_suit, test_restore_with_learner) {
  auto sm = new_test_learner_raft(
      3, 8, 2, pro::make_proxy<storage_builer>(new_test_memory_storage({with_peers({1, 2}), with_learners({3})})));
  constexpr std::uint64_t snapshot_index = 11;
  constexpr std::uint64_t snapshot_term = 11;
  const std::vector<std::uint64_t> voter_nodes = {1, 2};
  const std::vector<std::uint64_t> learner_nodes = {3};
  auto s = create_snapshot(snapshot_index, snapshot_term, std::vector<std::uint64_t>{voter_nodes});
  s.mutable_metadata()->mutable_conf_state()->add_learners(learner_nodes[0]);
  ASSERT_TRUE(sm.restore(raftpb::snapshot{s}));

  ASSERT_EQ(snapshot_index, sm.raft_log_handle_.last_index());
  auto term = sm.raft_log_handle_.term(snapshot_index);
  ASSERT_TRUE(term.has_value());
  ASSERT_EQ(term.value(), snapshot_term);
  ASSERT_EQ(voter_nodes, sm.trk_.voter_nodes());
  ASSERT_EQ(learner_nodes, sm.trk_.learner_nodes());
  for (auto voter : voter_nodes) {
    ASSERT_FALSE(sm.trk_.progress_map_view().view().at(voter).is_learner());
  }
  for (auto learner : learner_nodes) {
    ASSERT_TRUE(sm.trk_.progress_map_view().view().at(learner).is_learner());
  }
  ASSERT_FALSE(sm.restore(raftpb::snapshot{s}));
}

// TestRestoreWithVotersOutgoing tests if outgoing voter can receive and apply snapshot correctly.
TEST_F(raft_test_suit, test_restore_with_voters_outgoing) {
  auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({with_peers({1, 2})})));
  constexpr std::uint64_t snapshot_index = 11;
  constexpr std::uint64_t snapshot_term = 11;
  const std::vector<std::uint64_t> voter_nodes = {2, 3, 4};
  const std::vector<std::uint64_t> voter_outgoing_nodes = {1, 2, 3};
  auto s = create_snapshot(snapshot_index, snapshot_term, std::vector<std::uint64_t>{voter_nodes});
  for (auto voter : voter_outgoing_nodes) {
    s.mutable_metadata()->mutable_conf_state()->add_voters_outgoing(voter);
  }
  ASSERT_TRUE(sm.restore(raftpb::snapshot{s}));

  ASSERT_EQ(snapshot_index, sm.raft_log_handle_.last_index());
  auto term = sm.raft_log_handle_.term(snapshot_index);
  ASSERT_TRUE(term.has_value());
  ASSERT_EQ(term.value(), snapshot_term);
  std::vector<std::uint64_t> expect_voter_nodes = {1, 2, 3, 4};
  ASSERT_EQ(expect_voter_nodes, sm.trk_.voter_nodes());
  ASSERT_FALSE(sm.restore(create_snapshot(snapshot_index, snapshot_term, {1, 2, 3})));
  for (auto i = 0; i < sm.randomized_election_timeout_; ++i) {
    sm.tick();
  }
  ASSERT_EQ(lepton::state_type::FOLLOWER, sm.state_type_);
}

// TestRestoreVoterToLearner verifies that a normal peer can be downgraded to a
// learner through a snapshot. At the time of writing, we don't allow
// configuration changes to do this directly, but note that the snapshot may
// compress multiple changes to the configuration into one: the voter could have
// been removed, then readded as a learner and the snapshot reflects both
// changes. In that case, a voter receives a snapshot telling it that it is now
// a learner. In fact, the node has to accept that snapshot, or it is
// permanently cut off from the Raft log.
TEST_F(raft_test_suit, test_restore_voter_to_learner) {
  auto sm = new_test_learner_raft(3, 10, 1,
                                  pro::make_proxy<storage_builer>(new_test_memory_storage({with_peers({1, 2, 3})})));
  constexpr std::uint64_t snapshot_index = 11;
  constexpr std::uint64_t snapshot_term = 11;
  const std::vector<std::uint64_t> voter_nodes = {1, 2};
  const std::vector<std::uint64_t> learner_nodes = {3};
  auto s = create_snapshot(snapshot_index, snapshot_term, std::vector<std::uint64_t>{voter_nodes});
  s.mutable_metadata()->mutable_conf_state()->add_learners(learner_nodes[0]);
  ASSERT_FALSE(sm.is_learner_);
  ASSERT_TRUE(sm.restore(raftpb::snapshot{s}));
  ASSERT_TRUE(sm.is_learner_);
}

// TestRestoreLearnerPromotion checks that a learner can become to a follower after
// restoring snapshot.
TEST_F(raft_test_suit, test_restore_learner_promotion) {
  auto sm = new_test_learner_raft(
      3, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({with_peers({1, 2}), with_learners({3})})));
  constexpr std::uint64_t snapshot_index = 11;
  constexpr std::uint64_t snapshot_term = 11;
  const std::vector<std::uint64_t> voter_nodes = {1, 2, 3};
  auto s = create_snapshot(snapshot_index, snapshot_term, std::vector<std::uint64_t>{voter_nodes});
  ASSERT_TRUE(sm.is_learner_);
  ASSERT_TRUE(sm.restore(raftpb::snapshot{s}));
  ASSERT_FALSE(sm.is_learner_);
}

// TestLearnerReceiveSnapshot tests that a learner can receive a snpahost from leader
TEST_F(raft_test_suit, test_learner_receive_snapshot) {
  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1, 2, 3})});
  auto &mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  auto n1 = new_test_learner_raft(1, 10, 1, std::move(storage_proxy));
  auto n2 = new_test_learner_raft(
      1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({with_peers({1}), with_learners({2})})));

  constexpr std::uint64_t snapshot_index = 11;
  constexpr std::uint64_t snapshot_term = 11;
  const std::vector<std::uint64_t> voter_nodes = {1};
  const std::vector<std::uint64_t> learner_nodes = {2};
  auto s = create_snapshot(snapshot_index, snapshot_term, std::vector<std::uint64_t>{voter_nodes});
  s.mutable_metadata()->mutable_conf_state()->add_learners(learner_nodes[0]);

  ASSERT_TRUE(n1.restore(raftpb::snapshot{s}));
  ASSERT_EQ(snapshot_index, n1.raft_log_handle_.last_index());
  ASSERT_EQ(snapshot_index, n1.raft_log_handle_.committed());
  auto term = n1.raft_log_handle_.term(snapshot_index);
  ASSERT_TRUE(term.has_value());
  ASSERT_EQ(term.value(), snapshot_term);

  auto snap = n1.raft_log_handle_.next_unstable_snapshot();
  ASSERT_TRUE(snap);
  ASSERT_TRUE(mm_storage.apply_snapshot(raftpb::snapshot{*snap}));
  n1.applied_snap(*snap);

  std::vector<state_machine_builer_pair> peers;
  peers.emplace_back(state_machine_builer_pair{n1});
  peers.emplace_back(state_machine_builer_pair{n2});

  auto nt = new_network(std::move(peers));
  set_randomized_election_timeout(n1, n1.election_timeout_);
  for (int i = 0; i < n1.election_timeout_; ++i) {
    n1.tick();
  }
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_BEAT)});
  ASSERT_EQ(n1.raft_log_handle_.committed(), n2.raft_log_handle_.committed());
}

TEST_F(raft_test_suit, test_restore_ignore_snapshot) {
  constexpr std::uint64_t commit = 1;
  auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2})}})));
  sm.raft_log_handle_.append(create_entries(1, {1, 1, 1}));
  sm.raft_log_handle_.commit_to(commit);

  constexpr std::uint64_t snapshot_term = 1;
  const std::vector<std::uint64_t> voter_nodes = {1, 2};
  auto s = create_snapshot(commit, snapshot_term, std::vector<std::uint64_t>{voter_nodes});

  // ignore snapshot
  ASSERT_FALSE(sm.restore(raftpb::snapshot{s}));
  ASSERT_EQ(commit, sm.raft_log_handle_.committed());

  // ignore snapshot and fast forward commit
  s.mutable_metadata()->set_index(commit + 1);
  ASSERT_FALSE(sm.restore(raftpb::snapshot{s}));
  ASSERT_EQ(commit + 1, sm.raft_log_handle_.committed());
}

TEST_F(raft_test_suit, test_provide_snap) {
  // restore the state machine from a snapshot so it has a compacted log and a snapshot
  constexpr std::uint64_t snapshot_index = 11;
  constexpr std::uint64_t snapshot_term = 11;
  const std::vector<std::uint64_t> voter_nodes = {1, 2};
  auto s = create_snapshot(snapshot_index, snapshot_term, std::vector<std::uint64_t>{voter_nodes});

  auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1})}})));
  ASSERT_TRUE(sm.restore(raftpb::snapshot{s}));

  sm.become_candidate();
  sm.become_leader();

  // force set the next of node 2, so that node 2 needs a snapshot
  sm.trk_.progress_map_mutable_view().mutable_view().at(2).set_next(sm.raft_log_handle_.first_index());
  auto msg = convert_test_pb_message({.msg_type = raftpb::message_type::MSG_APP_RESP,
                                      .from = 2,
                                      .to = 1,
                                      .index = sm.trk_.progress_map_view().view().at(2).next() - 1});
  msg.set_reject(true);
  sm.step(std::move(msg));

  auto msgs = sm.read_messages();
  ASSERT_EQ(1, msgs.size());
  ASSERT_EQ(raftpb::message_type::MSG_SNAP, msgs[0].type());
}

TEST_F(raft_test_suit, test_ignore_provide_snap) {
  // restore the state machine from a snapshot so it has a compacted log and a snapshot
  constexpr std::uint64_t snapshot_index = 11;
  constexpr std::uint64_t snapshot_term = 11;
  const std::vector<std::uint64_t> voter_nodes = {1, 2};
  auto s = create_snapshot(snapshot_index, snapshot_term, std::vector<std::uint64_t>{voter_nodes});

  auto sm = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1})}})));
  ASSERT_TRUE(sm.restore(raftpb::snapshot{s}));

  sm.become_candidate();
  sm.become_leader();

  // force set the next of node 2, so that node 2 needs a snapshot
  // change node 2 to be inactive, expect node 1 ignore sending snapshot to 2
  sm.trk_.progress_map_mutable_view().mutable_view().at(2).set_next(sm.raft_log_handle_.first_index() - 1);
  sm.trk_.progress_map_mutable_view().mutable_view().at(2).set_recent_active(false);
  auto msg = convert_test_pb_message(
      {.msg_type = raftpb::message_type::MSG_PROP, .from = 1, .to = 1, .entries = {{.data = "somedata"}}});
  sm.step(std::move(msg));

  auto msgs = sm.read_messages();
  ASSERT_EQ(0, msgs.size());
}

TEST_F(raft_test_suit, test_restore_from_snap_msg) {
  // no use
}

TEST_F(raft_test_suit, test_slow_node_restore) {
  std::vector<state_machine_builer_pair> peers;
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  auto nt = new_network(std::move(peers));
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.isolate(3);
  for (auto j = 0; j < 100; ++j) {
    nt.send(
        {convert_test_pb_message({.msg_type = raftpb::message_type::MSG_PROP, .from = 1, .to = 1, .entries = {{}}})});
  }
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 101},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 101},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  auto &lead = *nt.peers.at(1).raft_handle;
  next_ents(lead, *nt.storage.at(1).get());
  raftpb::conf_state cs;
  auto voter_nodes = lead.trk_.voter_nodes();
  for (auto voter : voter_nodes) {
    cs.add_voters(voter);
  }
  nt.storage.at(1)->create_snapshot(lead.raft_log_handle_.applied(), std::move(cs), "");
  nt.storage.at(1)->compact(lead.raft_log_handle_.applied());

  nt.recover();
  // send heartbeats so that the leader can learn everyone is active.
  // node 3 will only be considered as active when node 1 receives a reply from it.
  do {
    nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_BEAT)});
  } while (!lead.trk_.progress_map_view().view().at(3).recent_active());
  ASSERT_TRUE(lead.trk_.progress_map_view().view().at(3).recent_active());

  // trigger a snapshot
  nt.send({convert_test_pb_message({.msg_type = raftpb::message_type::MSG_SNAP, .from = 1, .to = 1, .entries = {{}}})});
  auto &follower = *nt.peers.at(3).raft_handle;

  // trigger a commit
  ASSERT_EQ(lead.raft_log_handle_.committed(), follower.raft_log_handle_.committed());
}

// TestStepConfig tests that when raft step msgProp in EntryConfChange type,
// it appends the entry to log and sets pendingConf to be true.
TEST_F(raft_test_suit, test_step_config) {
  // a raft that cannot make progress
  auto r = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2})}})));
  r.become_candidate();
  r.become_leader();
  auto index = r.raft_log_handle_.last_index();
  auto msg = new_pb_message(1, 1, raftpb::message_type::MSG_PROP);
  auto entry = msg.add_entries();
  entry->set_type(raftpb::entry_type::ENTRY_CONF_CHANGE);
  r.step(std::move(msg));
  ASSERT_EQ(index + 1, r.raft_log_handle_.last_index());
  ASSERT_EQ(index + 1, r.pending_conf_index_);
}

// TestStepIgnoreConfig tests that if raft step the second msgProp in
// EntryConfChange type when the first one is uncommitted, the node will set
// the proposal to noop and keep its original state.
TEST_F(raft_test_suit, test_step_ignore_config) {
  // a raft that cannot make progress
  auto r = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2})}})));
  r.become_candidate();
  r.become_leader();
  auto msg = new_pb_message(1, 1, raftpb::message_type::MSG_PROP);
  auto entry = msg.add_entries();
  entry->set_type(raftpb::entry_type::ENTRY_CONF_CHANGE);
  r.step(raftpb::message{msg});
  auto index = r.raft_log_handle_.last_index();
  auto pending_conf_index = r.pending_conf_index_;
  r.step(raftpb::message{msg});

  lepton::pb::repeated_entry wents;
  auto new_entry = wents.Add();
  new_entry->set_type(raftpb::entry_type::ENTRY_NORMAL);
  new_entry->set_term(1);
  new_entry->set_index(3);

  auto ents = r.raft_log_handle_.entries(index + 1, lepton::NO_LIMIT);
  ASSERT_TRUE(ents);
  ASSERT_TRUE(compare_repeated_entry(absl::MakeSpan(wents), ents.value()));
  // ASSERT_EQ(wents, ents.value());
  ASSERT_EQ(pending_conf_index, r.pending_conf_index_);
}

// TestNewLeaderPendingConfig tests that new leader sets its pendingConfigIndex
// based on uncommitted entries.
TEST_F(raft_test_suit, test_new_leader_pending_config) {
  struct test_case {
    bool add_entry;
    uint64_t wpending_index;
  };

  std::vector<test_case> test_cases = {{false, 0}, {true, 1}};
  for (std::size_t i = 0; i < test_cases.size(); ++i) {
    auto &tt = test_cases[i];
    auto r = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2})}})));
    if (tt.add_entry) {
      lepton::pb::repeated_entry ents;
      auto new_entry = ents.Add();
      new_entry->set_type(raftpb::entry_type::ENTRY_NORMAL);
      must_append_entry(r, std::move(ents));
      r.become_candidate();
      r.become_leader();
      ASSERT_EQ(tt.wpending_index, r.pending_conf_index_);
    }
  }
}

// TestAddNode tests that addNode could update nodes correctly.
TEST_F(raft_test_suit, test_add_node) {
  auto r = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1})}})));
  r.apply_conf_change(
      lepton::pb::conf_change_as_v2(create_conf_change(2, raftpb::conf_change_type::CONF_CHANGE_ADD_NODE)));
  auto nodes = r.trk_.voter_nodes();
  std::vector<std::uint64_t> expect_voter_nodes{1, 2};
  ASSERT_EQ(expect_voter_nodes, nodes);
}

// TestAddLearner tests that addLearner could update nodes correctly.
TEST_F(raft_test_suit, test_add_learner) {
  auto r = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1})}})));
  // Add new learner peer.
  r.apply_conf_change(
      lepton::pb::conf_change_as_v2(create_conf_change(2, raftpb::conf_change_type::CONF_CHANGE_ADD_LEARNER_NODE)));
  ASSERT_FALSE(r.is_learner_);
  ASSERT_TRUE(r.trk_.progress_map_view().view().at(2).is_learner());

  // Promote peer to voter.
  r.apply_conf_change(
      lepton::pb::conf_change_as_v2(create_conf_change(2, raftpb::conf_change_type::CONF_CHANGE_ADD_NODE)));
  ASSERT_FALSE(r.is_learner_);
  ASSERT_FALSE(r.trk_.progress_map_view().view().at(2).is_learner());

  // Demote r.
  r.apply_conf_change(
      lepton::pb::conf_change_as_v2(create_conf_change(1, raftpb::conf_change_type::CONF_CHANGE_ADD_LEARNER_NODE)));
  ASSERT_TRUE(r.is_learner_);
  ASSERT_TRUE(r.trk_.progress_map_view().view().at(1).is_learner());

  // Promote r again.
  r.apply_conf_change(
      lepton::pb::conf_change_as_v2(create_conf_change(1, raftpb::conf_change_type::CONF_CHANGE_ADD_NODE)));
  ASSERT_FALSE(r.is_learner_);
  ASSERT_FALSE(r.trk_.progress_map_view().view().at(1).is_learner());
}

// TestAddNodeCheckQuorum tests that addNode does not trigger a leader election
// immediately when checkQuorum is set.
TEST_F(raft_test_suit, test_add_node_check_quorum) {
  auto sm_cfg =
      new_test_config(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1})}})));
  sm_cfg.check_quorum = true;
  auto r = new_test_raft(std::move(sm_cfg));
  ASSERT_EQ(true, r.check_quorum_);

  r.become_candidate();
  r.become_leader();
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER), magic_enum::enum_name(r.state_type_));

  for (int i = 0; i < r.election_timeout_ - 1; ++i) {
    r.tick();
  }

  r.apply_conf_change(
      lepton::pb::conf_change_as_v2(create_conf_change(2, raftpb::conf_change_type::CONF_CHANGE_ADD_NODE)));

  // This tick will reach electionTimeout, which triggers a quorum check.
  r.tick();

  // Node 1 should still be the leader after a single tick.
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER), magic_enum::enum_name(r.state_type_));

  // 新加入节点后，由于没有收到 node 2 的 response，所以 node 1 从 leader 变为 follower
  // After another electionTimeout ticks without hearing from node 2,
  // node 1 should step down.
  for (int i = 0; i < r.election_timeout_; ++i) {
    r.tick();
  }
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::FOLLOWER), magic_enum::enum_name(r.state_type_));
}

// TestRemoveNode tests that removeNode could update nodes and
// removed list correctly.
TEST_F(raft_test_suit, test_remove_node) {
  auto r = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2})}})));
  // Add new learner peer.
  r.apply_conf_change(
      lepton::pb::conf_change_as_v2(create_conf_change(2, raftpb::conf_change_type::CONF_CHANGE_REMOVE_NODE)));
  auto nodes = r.trk_.voter_nodes();
  std::vector<std::uint64_t> expect_voter_nodes{1};
  ASSERT_EQ(expect_voter_nodes, nodes);

  // Removing the remaining voter will panic.
  ASSERT_DEATH(r.apply_conf_change(lepton::pb::conf_change_as_v2(
                   create_conf_change(1, raftpb::conf_change_type::CONF_CHANGE_REMOVE_NODE))),
               "");
}

// TestRemoveLearner tests that removeNode could update nodes and
// removed list correctly.
TEST_F(raft_test_suit, test_remove_learner) {
  auto r = new_test_learner_raft(
      1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1}), with_learners({2})}})));

  r.apply_conf_change(
      lepton::pb::conf_change_as_v2(create_conf_change(2, raftpb::conf_change_type::CONF_CHANGE_REMOVE_NODE)));
  auto nodes = r.trk_.voter_nodes();
  std::vector<std::uint64_t> expect_voter_nodes{1};
  ASSERT_EQ(expect_voter_nodes, nodes);

  // Removing the remaining voter will panic.
  ASSERT_DEATH(r.apply_conf_change(lepton::pb::conf_change_as_v2(
                   create_conf_change(1, raftpb::conf_change_type::CONF_CHANGE_REMOVE_NODE))),
               "");
}

TEST_F(raft_test_suit, test_promotable) {
  // 定义测试结构体
  struct test_case {
    std::vector<uint64_t> peers;
    bool expected_result;
  };

  // 创建测试用例集合
  std::vector<test_case> tests = {{{1}, true}, {{1, 2, 3}, true}, {{}, false}, {{2, 3}, false}};

  for (const auto &tt : tests) {
    std::vector<uint64_t> peers = tt.peers;
    auto r = new_test_raft(1, 10, 1,
                           pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers(std::move(peers))}})));

    ASSERT_EQ(tt.expected_result, r.promotable());
  }
}

TEST_F(raft_test_suit, test_raft_nodes) {
  struct test_case {
    std::vector<uint64_t> ids;
    std::vector<uint64_t> wids;  // 预期结果
  };

  std::vector<test_case> tests = {
      {{1, 2, 3}, {1, 2, 3}},  // 测试用例 1
      {{3, 2, 1}, {1, 2, 3}},  // 测试用例 2 (顺序不同)
  };

  for (const auto &tt : tests) {
    std::vector<uint64_t> peers = tt.ids;
    auto r = new_test_raft(1, 10, 1,
                           pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers(std::move(peers))}})));
    auto nodes = r.trk_.voter_nodes();
    ASSERT_EQ(tt.wids, nodes);
  }
}

static void test_campaign_while_leader(bool pre_vote) {
  auto sm_cfg = new_test_config(1, 5, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1})}})));
  sm_cfg.pre_vote = pre_vote;
  auto r = new_test_raft(std::move(sm_cfg));
  ASSERT_EQ(pre_vote, r.pre_vote_);
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::FOLLOWER), magic_enum::enum_name(r.state_type_));
  // We don't call campaign() directly because it comes after the check
  // for our current state.
  r.step({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  r.advance_messages_after_append();
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER), magic_enum::enum_name(r.state_type_));

  auto term = r.term_;
  r.step({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  r.advance_messages_after_append();
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER), magic_enum::enum_name(r.state_type_));
  ASSERT_EQ(term, r.term_);
}

TEST_F(raft_test_suit, test_campaign_while_leader) { test_campaign_while_leader(false); }

TEST_F(raft_test_suit, test_pre_campaign_while_leader) { test_campaign_while_leader(true); }

// TestCommitAfterRemoveNode verifies that pending commands can become
// committed when a config change reduces the quorum requirements.
// 在Raft集群中，当一个配置变更（移除节点）被提交后，会改变集群的法定人数（quorum）要求，
// 从而使得之前由于quorum不足而未能提交的条目（entries）能够被提交。
TEST_F(raft_test_suit, test_commit_after_remove_node) {
  // Create a cluster with two nodes.
  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1, 2})});
  auto &mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  auto r = new_test_learner_raft(1, 5, 1, std::move(storage_proxy));
  r.become_candidate();
  r.become_leader();

  // Begin to remove the second node.
  auto cc = create_conf_change(2, raftpb::conf_change_type::CONF_CHANGE_REMOVE_NODE);
  {
    raftpb::message prop_msg;
    prop_msg.set_type(raftpb::message_type::MSG_PROP);
    auto entry = prop_msg.add_entries();
    entry->set_type(raftpb::entry_type::ENTRY_CONF_CHANGE);
    entry->set_data(cc.SerializeAsString());
    r.step(std::move(prop_msg));
    // Stabilize the log and make sure nothing is committed yet.
    ASSERT_TRUE(next_ents(r, mm_storage).empty());
  }

  auto cc_index = r.raft_log_handle_.last_index();

  // While the config change is pending, make another proposal.
  {
    raftpb::message prop_msg;
    prop_msg.set_type(raftpb::message_type::MSG_PROP);
    auto entry = prop_msg.add_entries();
    entry->set_type(raftpb::entry_type::ENTRY_NORMAL);
    entry->set_data("hello");
    r.step(std::move(prop_msg));
    ASSERT_TRUE(next_ents(r, mm_storage).empty());
  }

  // Node 2 acknowledges the config change, committing it.
  {
    raftpb::message msg;
    msg.set_from(2);
    msg.set_type(raftpb::message_type::MSG_APP_RESP);
    msg.set_index(cc_index);
    r.step(std::move(msg));
    auto ents = next_ents(r, mm_storage);
    ASSERT_EQ(2, ents.size());
    ASSERT_EQ(magic_enum::enum_name(raftpb::entry_type::ENTRY_NORMAL), magic_enum::enum_name(ents[0].type()));
    ASSERT_TRUE(ents[0].data().empty());
    ASSERT_EQ(magic_enum::enum_name(raftpb::entry_type::ENTRY_CONF_CHANGE), magic_enum::enum_name(ents[1].type()));
  }

  // Apply the config change. This reduces quorum requirements so the
  // pending command can now commit.
  r.apply_conf_change(lepton::pb::conf_change_as_v2(raftpb::conf_change{cc}));
  auto ents = next_ents(r, mm_storage);
  ASSERT_EQ(1, ents.size());
  ASSERT_EQ(magic_enum::enum_name(raftpb::entry_type::ENTRY_NORMAL), magic_enum::enum_name(ents[0].type()));
  ASSERT_EQ("hello", ents[0].data());
}

static void check_leader_transfer_state(lepton::raft &r, lepton::state_type state, std::uint64_t lead,
                                        std::source_location loc = std::source_location::current()) {
  ASSERT_EQ(magic_enum::enum_name(state), magic_enum::enum_name(r.state_type_))
      << fmt::format("[{}:{}][{}]\n", loc.file_name(), loc.line(), loc.function_name());
  ASSERT_EQ(lead, r.lead_) << fmt::format("[{}:{}][{}]\n", loc.file_name(), loc.line(), loc.function_name());
  ASSERT_EQ(lepton::NONE, r.leader_transferee_)
      << fmt::format("[{}:{}][{}]\n", loc.file_name(), loc.line(), loc.function_name());
}

// TestLeaderTransferToUpToDateNode verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
TEST_F(raft_test_suit, test_leader_transfer_to_up_to_date_node) {
  auto nt = init_empty_network({1, 2, 3});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  ASSERT_EQ(1, nt.peers.at(1).raft_handle->lead_);

  // Transfer leadership to 2.
  nt.send({new_pb_message(2, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
    };
    check_raft_node_after_send_msg(tests);
  }
  check_leader_transfer_state(*nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2);

  // After some log replication, transfer leadership back to 1.
  nt.send({convert_test_pb_message({.msg_type = raftpb::message_type::MSG_PROP, .from = 1, .to = 1, .entries = {{}}})});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 3},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 2, 3},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 3},
    };
    check_raft_node_after_send_msg(tests);
  }
  nt.send({new_pb_message(1, 2, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 3, 4},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 3, 4},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 3, 4},
    };
    check_raft_node_after_send_msg(tests);
  }
  check_leader_transfer_state(*nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1);
}

// TestLeaderTransferToUpToDateNodeFromFollower verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
// Not like TestLeaderTransferToUpToDateNode, where the leader transfer message
// is sent to the leader, in this test case every leader transfer message is sent
// to the follower.
TEST_F(raft_test_suit, test_leader_transfer_to_up_to_date_node_from_follower) {
  auto nt = init_empty_network({1, 2, 3});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  ASSERT_EQ(1, nt.peers.at(1).raft_handle->lead_);

  // Transfer leadership to 2.
  nt.send({new_pb_message(2, 2, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
    };
    check_raft_node_after_send_msg(tests);
  }
  check_leader_transfer_state(*nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2);

  // After some log replication, transfer leadership back to 1.
  nt.send({convert_test_pb_message({.msg_type = raftpb::message_type::MSG_PROP, .from = 1, .to = 1, .entries = {{}}})});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 3},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 2, 3},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 3},
    };
    check_raft_node_after_send_msg(tests);
  }
  nt.send({new_pb_message(1, 2, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 3, 4},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 3, 4},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 3, 4},
    };
    check_raft_node_after_send_msg(tests);
  }
  check_leader_transfer_state(*nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1);
}

// TestLeaderTransferWithCheckQuorum ensures transferring leader still works
// even the current leader is still under its leader lease
TEST_F(raft_test_suit, test_leader_transfer_with_check_quorum) {
  auto nt = init_network({1, 2, 3}, {raft_config_quorum_hook}, {raft_quorum_hook});
  for (std::uint64_t i = 1; i < 4; ++i) {
    set_randomized_election_timeout(*nt.peers.at(i).raft_handle,
                                    nt.peers.at(i).raft_handle->election_timeout_ + static_cast<int>(i));
  }

  // Letting peer 2 electionElapsed reach to timeout so that it can vote for peer 1
  auto &r2 = *nt.peers.at(2).raft_handle;
  for (int i = 0; i < r2.election_timeout_; ++i) {
    r2.tick();
  }

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // Transfer leadership to 2.
  nt.send({new_pb_message(2, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
    };
    check_raft_node_after_send_msg(tests);
  }
  check_leader_transfer_state(*nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2);

  // After some log replication, transfer leadership back to 1.
  nt.send({convert_test_pb_message({.msg_type = raftpb::message_type::MSG_PROP, .from = 1, .to = 1, .entries = {{}}})});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 3},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 2, 3},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 3},
    };
    check_raft_node_after_send_msg(tests);
  }
  nt.send({new_pb_message(1, 2, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 3, 4},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 3, 4},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 3, 4},
    };
    check_raft_node_after_send_msg(tests);
  }
  check_leader_transfer_state(*nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1);
}

TEST_F(raft_test_suit, test_leader_transfer_to_slow_follower) {
  auto nt = init_empty_network({1, 2, 3});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.isolate(3);
  nt.send({convert_test_pb_message({.msg_type = raftpb::message_type::MSG_PROP, .from = 1, .to = 1, .entries = {{}}})});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.recover();
  ASSERT_EQ(1, nt.peers.at(1).raft_handle->trk_.progress_map_view().view().at(3).match());

  // Transfer leadership to 3 when node 3 is lack of log.
  nt.send({new_pb_message(3, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 3},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 3},
        {nt.peers.at(3).raft_handle, lepton::state_type::LEADER, 2, 3},
    };
    check_raft_node_after_send_msg(tests);
  }
  check_leader_transfer_state(*nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 3);
}

TEST_F(raft_test_suit, test_leader_transfer_after_snapshot) {
  auto nt = init_empty_network({1, 2, 3});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.isolate(3);

  nt.send({convert_test_pb_message({.msg_type = raftpb::message_type::MSG_PROP, .from = 1, .to = 1, .entries = {{}}})});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  auto &lead = *nt.peers.at(1).raft_handle;
  next_ents(lead, *nt.storage.at(1));
  raftpb::conf_state cs;
  auto voter_nodes = lead.trk_.voter_nodes();
  for (auto voter : voter_nodes) {
    cs.add_voters(voter);
  }
  nt.storage.at(1)->create_snapshot(lead.raft_log_handle_.applied(), std::move(cs), "");
  nt.storage.at(1)->compact(lead.raft_log_handle_.applied());

  nt.recover();
  ASSERT_EQ(1, nt.peers.at(1).raft_handle->trk_.progress_map_view().view().at(3).match());

  raftpb::message filtered;
  // Snapshot needs to be applied before sending MsgAppResp
  nt.msg_hook = [&](const raftpb::message &m) {
    if (m.type() != raftpb::message_type::MSG_APP_RESP) {
      return true;
    }
    if (m.from() != 3) {
      return true;
    }
    if (m.reject()) {
      return true;
    }
    filtered.CopyFrom(m);
    return false;
  };
  // Transfer leadership to 3 when node 3 is lack of snapshot.
  nt.send({new_pb_message(3, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 2},
    };
    check_raft_node_after_send_msg(tests);
  }
  ASSERT_NE(raftpb::message{}.DebugString(), filtered.DebugString());

  // Apply snapshot and resume progress
  auto &follower = *nt.peers.at(3).raft_handle;
  auto snap = follower.raft_log_handle_.next_unstable_snapshot();
  ASSERT_TRUE(snap);
  nt.storage.at(3)->apply_snapshot(raftpb::snapshot{*snap});
  follower.applied_snap(*snap);
  nt.msg_hook = nullptr;
  nt.send({std::move(filtered)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 3},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 3},
        {nt.peers.at(3).raft_handle, lepton::state_type::LEADER, 2, 3},
    };
    check_raft_node_after_send_msg(tests);
  }
  check_leader_transfer_state(*nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 3);
}

TEST_F(raft_test_suit, test_leader_transfer_to_self) {
  auto nt = init_empty_network({1, 2, 3});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // Transfer leadership to self, there will be noop.
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  check_leader_transfer_state(*nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1);
}

TEST_F(raft_test_suit, test_leader_transfer_to_non_existing_node) {
  auto nt = init_empty_network({1, 2, 3});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // Transfer leadership to self, there will be noop.
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  check_leader_transfer_state(*nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1);
}

TEST_F(raft_test_suit, test_leader_transfer_timeout) {
  auto nt = init_empty_network({1, 2, 3});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.isolate(3);

  auto &lead = *nt.peers.at(1).raft_handle;

  // Transfer leadership to isolated node, wait for timeout.
  nt.send({new_pb_message(3, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  ASSERT_EQ(3, lead.leader_transferee_);

  for (auto i = 0; i < lead.heartbeat_timeout_; ++i) {
    lead.tick();
  }
  ASSERT_EQ(3, lead.leader_transferee_);

  for (auto i = 0; i < lead.election_timeout_ - lead.heartbeat_timeout_; ++i) {
    lead.tick();
  }
  check_leader_transfer_state(*nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1);
}

// 当 Leader 正在处理领导权转移（leader transfer）时，
// 新的提案（proposal）会被正确拒绝​​。
TEST_F(raft_test_suit, test_leader_transfer_ignore_proposal) {
  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1, 2, 3})});
  auto &mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  auto r = new_test_learner_raft(1, 10, 1, std::move(storage_proxy));

  std::vector<state_machine_builer_pair> peers;
  peers.emplace_back(state_machine_builer_pair{r});
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  auto nt = new_network(std::move(peers));
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.isolate(3);

  auto &lead = *nt.peers.at(1).raft_handle;
  next_ents(r, mm_storage);  // handle empty entry

  // Transfer leadership to isolated node to let transfer pending, then send proposal.
  nt.send({new_pb_message(3, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  ASSERT_EQ(3, lead.leader_transferee_);
  nt.send({convert_test_pb_message({.msg_type = raftpb::message_type::MSG_PROP, .from = 1, .to = 1, .entries = {{}}})});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  auto err_code = EC_SUCCESS;
  auto has_called_error = false;
  auto step_resilt = leaf::try_handle_some(
      [&]() -> leaf::result<void> {
        BOOST_LEAF_CHECK(lead.step(convert_test_pb_message(
            {.msg_type = raftpb::message_type::MSG_PROP, .from = 1, .to = 1, .entries = {{}}})));
        return {};
      },
      [&](const lepton_error &e) -> leaf::result<void> {
        has_called_error = true;
        err_code = e.err_code;
        return new_error(e);
      });
  ASSERT_TRUE(has_called_error);
  ASSERT_EQ(err_code, lepton::logic_error::PROPOSAL_DROPPED);
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  ASSERT_EQ(1, nt.peers.at(1).raft_handle->trk_.progress_map_view().view().at(1).match());
}

TEST_F(raft_test_suit, test_leader_transfer_receive_higher_term_vote) {
  auto nt = init_empty_network({1, 2, 3});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.isolate(3);

  auto &lead = *nt.peers.at(1).raft_handle;

  // Transfer leadership to isolated node to let transfer pending.
  nt.send({new_pb_message(3, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  ASSERT_EQ(3, lead.leader_transferee_);

  nt.send({convert_test_pb_message(
      {.msg_type = raftpb::message_type::MSG_HUP, .from = 2, .to = 2, .term = 2, .index = 1})});
  check_leader_transfer_state(*nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2);
}

TEST_F(raft_test_suit, test_leader_transfer_remove_node) {
  auto nt = init_empty_network({1, 2, 3});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.ignore(raftpb::message_type::MSG_TIMEOUT_NOW);

  auto &lead = *nt.peers.at(1).raft_handle;

  // The leadTransferee is removed when leadship transferring.
  nt.send({new_pb_message(3, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  ASSERT_EQ(3, lead.leader_transferee_);

  lead.apply_conf_change(
      lepton::pb::conf_change_as_v2(create_conf_change(3, raftpb::conf_change_type::CONF_CHANGE_REMOVE_NODE)));
  check_leader_transfer_state(lead, lepton::state_type::LEADER, 1);
}

TEST_F(raft_test_suit, test_leader_transfer_demote_node) {
  auto nt = init_empty_network({1, 2, 3});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.ignore(raftpb::message_type::MSG_TIMEOUT_NOW);

  auto &lead = *nt.peers.at(1).raft_handle;

  // The leadTransferee is removed when leadship transferring.
  nt.send({new_pb_message(3, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  ASSERT_EQ(3, lead.leader_transferee_);

  raftpb::conf_change_v2 cc;
  auto change1 = cc.add_changes();
  change1->set_node_id(3);
  change1->set_type(raftpb::conf_change_type::CONF_CHANGE_REMOVE_NODE);
  auto change2 = cc.add_changes();
  change2->set_node_id(3);
  change2->set_type(raftpb::conf_change_type::CONF_CHANGE_ADD_LEARNER_NODE);
  lead.apply_conf_change(std::move(cc));

  // Make the Raft group commit the LeaveJoint entry.
  lead.apply_conf_change(raftpb::conf_change_v2{});
  check_leader_transfer_state(lead, lepton::state_type::LEADER, 1);
}

// TestLeaderTransferBack verifies leadership can transfer back to self when last transfer is pending.
TEST_F(raft_test_suit, test_leader_transfer_back) {
  auto nt = init_empty_network({1, 2, 3});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.isolate(3);

  auto &lead = *nt.peers.at(1).raft_handle;

  nt.send({new_pb_message(3, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  ASSERT_EQ(3, lead.leader_transferee_);

  // Transfer leadership back to self.
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  check_leader_transfer_state(lead, lepton::state_type::LEADER, 1);
}

// TestLeaderTransferSecondTransferToAnotherNode verifies leader can transfer to another node
// when last transfer is pending.
TEST_F(raft_test_suit, test_leader_transfer_second_transfer_to_another_node) {
  auto nt = init_empty_network({1, 2, 3});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.isolate(3);

  auto &lead = *nt.peers.at(1).raft_handle;

  nt.send({new_pb_message(3, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  ASSERT_EQ(3, lead.leader_transferee_);

  // Transfer leadership to another node.
  nt.send({new_pb_message(2, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  check_leader_transfer_state(lead, lepton::state_type::FOLLOWER, 2);
}

// TestLeaderTransferSecondTransferToSameNode verifies second transfer leader request
// to the same node should not extend the timeout while the first one is pending.
TEST_F(raft_test_suit, test_leader_transfer_second_transfer_to_same_node) {
  auto nt = init_empty_network({1, 2, 3});
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.isolate(3);

  auto &lead = *nt.peers.at(1).raft_handle;

  nt.send({new_pb_message(3, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  ASSERT_EQ(3, lead.leader_transferee_);

  // 若在 electionTimeout 内未完成转移，Leader 终止流程继续服务，避免阻塞。
  for (auto i = 0; i < lead.heartbeat_timeout_; ++i) {
    lead.tick();
  }
  ASSERT_EQ(3, lead.leader_transferee_);

  // Second transfer leadership request to the same node.
  nt.send({new_pb_message(3, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  ASSERT_EQ(3, lead.leader_transferee_);

  for (auto i = 0; i < lead.election_timeout_ - lead.heartbeat_timeout_; ++i) {
    lead.tick();
  }
  ASSERT_EQ(NONE, lead.leader_transferee_);

  check_leader_transfer_state(*nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1);
}

// TestTransferNonMember verifies that when a MsgTimeoutNow arrives at
// a node that has been removed from the group, nothing happens.
// (previously, if the node also got votes, it would panic as it
// transitioned to StateLeader)
TEST_F(raft_test_suit, test_transfer_non_member) {
  auto r = new_test_raft(1, 5, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({2, 3, 4})}})));
  r.step({new_pb_message(2, 1, raftpb::message_type::MSG_TIMEOUT_NOW)});
  {
    std::vector<test_expected_raft_status> tests{
        {&r, lepton::state_type::FOLLOWER, 0, 0},
    };
    check_raft_node_after_send_msg(tests);
  }

  r.step({new_pb_message(2, 1, raftpb::message_type::MSG_VOTE_RESP)});
  {
    std::vector<test_expected_raft_status> tests{
        {&r, lepton::state_type::FOLLOWER, 0, 0},
    };
    check_raft_node_after_send_msg(tests);
  }

  r.step({new_pb_message(3, 1, raftpb::message_type::MSG_VOTE_RESP)});
  {
    std::vector<test_expected_raft_status> tests{
        {&r, lepton::state_type::FOLLOWER, 0, 0},
    };
    check_raft_node_after_send_msg(tests);
  }
}

// TestNodeWithSmallerTermCanCompleteElection tests the scenario where a node
// that has been partitioned away (and fallen behind) rejoins the cluster at
// about the same time the leader node gets partitioned away.
// Previously the cluster would come to a standstill when run with PreVote
// enabled.
TEST_F(raft_test_suit, test_node_with_smaller_term_can_complete_election) {
  auto nt = init_network({1, 2, 3}, {raft_config_pre_vote}, {raft_pre_vote_hook});
  nt.peers.at(1).raft_handle->become_follower(1, NONE);
  nt.peers.at(2).raft_handle->become_follower(1, NONE);
  nt.peers.at(3).raft_handle->become_follower(1, NONE);

  // cause a network partition to isolate node 3
  nt.cut(1, 3);
  nt.cut(2, 3);

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 0},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::PRE_CANDIDATE, 1, 0},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.send({new_pb_message(2, 2, raftpb::message_type::MSG_HUP)});
  // check whether the term values are expected
  // check state
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 3, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 3, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::PRE_CANDIDATE, 1, 0},
    };
    check_raft_node_after_send_msg(tests);
  }

  SPDLOG_INFO("going to bring back peer 3 and kill peer 2");
  // recover the network then immediately isolate b which is currently
  // the leader, this is to emulate the crash of b.
  nt.recover();
  nt.cut(2, 1);
  nt.cut(2, 3);

  // call for election
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 3, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 3, 2},
        // 收到高于当前term的msg后，会重置自己的term
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 3, 0},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 4, 3},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 3, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 4, 3},
    };
    check_raft_node_after_send_msg(tests);
  }
}

// TestPreVoteWithSplitVote verifies that after split vote, cluster can complete
// election in next round.
TEST_F(raft_test_suit, test_pre_vote_with_split_vote) {
  auto nt = init_network({1, 2, 3}, {raft_config_pre_vote}, {raft_pre_vote_hook});
  nt.peers.at(1).raft_handle->become_follower(1, NONE);
  nt.peers.at(2).raft_handle->become_follower(1, NONE);
  nt.peers.at(3).raft_handle->become_follower(1, NONE);

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // simulate leader down. followers start split vote.
  nt.isolate(1);
  nt.send({new_pb_message(2, 2, raftpb::message_type::MSG_HUP), new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  // check whether the term values are expected
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::CANDIDATE, 3, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::CANDIDATE, 3, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // node 2 election timeout first
  nt.send({new_pb_message(2, 2, raftpb::message_type::MSG_HUP)});

  // check whether the term values are expected
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 4, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 4, 2},
    };
    check_raft_node_after_send_msg(tests);
  }
}

// TestPreVoteWithCheckQuorum ensures that after a node become pre-candidate,
// it will checkQuorum correctly.
TEST_F(raft_test_suit, test_pre_vote_with_check_quorum) {
  auto nt =
      init_network({1, 2, 3}, {raft_config_quorum_hook, raft_config_pre_vote}, {raft_quorum_hook, raft_pre_vote_hook});
  nt.peers.at(1).raft_handle->become_follower(1, NONE);
  nt.peers.at(2).raft_handle->become_follower(1, NONE);
  nt.peers.at(3).raft_handle->become_follower(1, NONE);

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // isolate node 1. node 2 and node 3 have leader info
  nt.isolate(1);

  // node 2 will ignore node 3's PreVote
  // node 2 仍然有 leader，所以会忽略 node 3 pre_vote msg
  // 此时 node 3 的 leader 会设置为 NONE
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::PRE_CANDIDATE, 2, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  //  由于 node 3 的 leader 已经为 NONE，所以此时收到 node 2 的 pre_vote msg 会赞成
  nt.send({new_pb_message(2, 2, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 3, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 3, 2},
    };
    check_raft_node_after_send_msg(tests);
  }
}

// TestLearnerCampaign verifies that a learner won't campaign even if it receives
// a MsgHup or MsgTimeoutNow.
TEST_F(raft_test_suit, test_learner_campaign) {
  auto n1 = new_test_learner_raft(
      1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1}), with_learners({2})}})));
  auto n2 = new_test_learner_raft(
      2, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1}), with_learners({2})}})));
  n1.apply_conf_change(
      lepton::pb::conf_change_as_v2(create_conf_change(2, raftpb::conf_change_type::CONF_CHANGE_ADD_LEARNER_NODE)));
  n2.apply_conf_change(
      lepton::pb::conf_change_as_v2(create_conf_change(2, raftpb::conf_change_type::CONF_CHANGE_ADD_LEARNER_NODE)));

  std::vector<state_machine_builer_pair> peers;
  peers.emplace_back(state_machine_builer_pair{n1});
  peers.emplace_back(state_machine_builer_pair{n2});

  auto nt = new_network(std::move(peers));
  ASSERT_TRUE(nt.peers.at(2).raft_handle->is_learner_);

  nt.send({new_pb_message(2, 2, raftpb::message_type::MSG_HUP)});
  ASSERT_TRUE(nt.peers.at(2).raft_handle->is_learner_);
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 0, 0},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 0, 0},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  ASSERT_EQ(1, nt.peers.at(1).raft_handle->lead_);
  ASSERT_TRUE(nt.peers.at(2).raft_handle->is_learner_);
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // NB: TransferLeader already checks that the recipient is not a learner, but
  // the check could have happened by the time the recipient becomes a learner,
  // in which case it will receive MsgTimeoutNow as in this test case and we
  // verify that it's ignored.
  nt.send({new_pb_message(1, 2, raftpb::message_type::MSG_TIMEOUT_NOW)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
}

// simulate rolling update a cluster for Pre-Vote. cluster has 3 nodes [n1, n2, n3].
// n1 is leader with term 2
// n2 is follower with term 2
// n3 is partitioned, with term 4 and less log, state is candidate
static network new_pre_vote_migration_cluster() {
  auto raft_config_pre_vote_func = [](lepton::config &cfg) {
    if (cfg.id == 1 || cfg.id == 2) {
      cfg.pre_vote = true;
    }
  };
  auto raft_pre_vote_hook_func = [](lepton::raft &sm) {
    if (sm.id_ == 1 || sm.id_ == 2) {
      ASSERT_TRUE(sm.pre_vote_);
    }
  };
  // We intentionally do not enable PreVote for n3, this is done so in order
  // to simulate a rolling restart process where it's possible to have a mixed
  // version cluster with replicas with PreVote enabled, and replicas without.
  auto nt = init_network({1, 2, 3}, {raft_config_pre_vote_func}, {raft_pre_vote_hook_func});
  nt.peers.at(1).raft_handle->become_follower(1, NONE);
  nt.peers.at(2).raft_handle->become_follower(1, NONE);
  nt.peers.at(3).raft_handle->become_follower(1, NONE);

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // Cause a network partition to isolate n3.
  nt.isolate(3);
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "some data")});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  assert(2 == nt.peers.at(1).raft_handle->raft_log_handle_.committed());

  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::CANDIDATE, 3, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::CANDIDATE, 4, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // Enable prevote on n3, then recover the network
  nt.peers.at(3).raft_handle->pre_vote_ = true;
  nt.recover();
  return nt;
}

TEST_F(raft_test_suit, test_pre_vote_migration_can_complete_election) {
  auto nt = new_pre_vote_migration_cluster();

  // n1 is leader with term 2
  // n2 is follower with term 2
  // n3 is pre-candidate with term 4, and less log

  // simulate leader down
  nt.isolate(1);

  // Call for elections from both n2 and n3.
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::PRE_CANDIDATE, 4, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
  // node 2 收到来自 node 3 的 MSG_PRE_VOTE_RESP, term 比自己高会重置当前状态为follower ，并设置自己的 term 为 MSG的term
  nt.send({new_pb_message(2, 2, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 4, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::PRE_CANDIDATE, 4, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 4, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::PRE_CANDIDATE, 4, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  nt.send({new_pb_message(2, 2, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 5, 3},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 5, 3},
    };
    check_raft_node_after_send_msg(tests);
  }
}

TEST_F(raft_test_suit, test_pre_vote_migration_with_free_stuck_pre_candidate) {
  auto nt = new_pre_vote_migration_cluster();

  // n1 is leader with term 2
  // n2 is follower with term 2
  // n3 is pre-candidate with term 4, and less log

  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::PRE_CANDIDATE, 4, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // Pre-Vote again for safety
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::PRE_CANDIDATE, 4, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // Disrupt the leader so that the stuck peer is freed
  // node 1 收到来自 node 3 的 MSG_HEARTBEAT_RESP, term 比自己高会重置当前状态为follower ，并设置自己的 term 为
  // MSG的term
  nt.send({convert_test_pb_message({.msg_type = raftpb::message_type::MSG_HEARTBEAT,
                                    .from = 1,
                                    .to = 3,
                                    .term = nt.peers.at(1).raft_handle->term_})});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 4, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::PRE_CANDIDATE, 4, 1},
    };
    check_raft_node_after_send_msg(tests);
  }
}

static void test_conf_change_check_before_campaign(bool v2) {
  auto nt = init_empty_network({1, 2, 3});
  auto &n1 = *nt.peers.at(1).raft_handle;
  auto &n2 = *nt.peers.at(2).raft_handle;

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // Begin to remove the third node.
  raftpb::conf_change cc;
  cc.set_type(raftpb::conf_change_type::CONF_CHANGE_REMOVE_NODE);
  cc.set_node_id(2);
  std::string cc_data;
  raftpb::entry_type ty;
  if (v2) {
    auto ccv2 = lepton::pb::conf_change_as_v2(std::move(cc));
    cc_data = ccv2.SerializeAsString();
    ty = raftpb::entry_type::ENTRY_CONF_CHANGE_V2;
  } else {
    cc_data = cc.SerializeAsString();
    ty = raftpb::entry_type::ENTRY_CONF_CHANGE;
  }

  // 进行配置变更。注意：这里仅完成提交，但没有 apply conf change
  raftpb::message prop_msg = new_pb_message(1, 1, raftpb::message_type::MSG_PROP);
  auto entry = prop_msg.add_entries();
  entry->set_type(ty);
  entry->set_data(cc_data);
  nt.send({std::move(prop_msg)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 2},
    };
    check_raft_node_after_send_msg(tests);
  }

  // 由于有未应用的配置变更，所以此时即使触发超时选举逻辑，node 2 也会选举失败
  // Trigger campaign in node 2
  for (auto i = 0; i < n2.randomized_election_timeout_; ++i) {
    n2.tick();
  }
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 2},
        // It's still follower because committed conf change is not applied.
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 2},
    };
    check_raft_node_after_send_msg(tests);
  }

  // 由于有未应用的配置变更，所以此时即使触发超时选举逻辑，node 2 也会选举失败
  // Transfer leadership to peer 2.
  nt.send({new_pb_message(2, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 2},
        // It's still follower because committed conf change is not applied.
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 2},
    };
    check_raft_node_after_send_msg(tests);
  }

  // Abort transfer leader
  for (auto i = 0; i < n1.election_timeout_; ++i) {
    n1.tick();
  }

  // Advance apply
  // 在 node2 上应用提交的条目; 但是此时 node 1 并不知道 node 2 已经应用完配置变更
  next_ents(n2, *nt.storage.at(2));

  // Transfer leadership to peer 2 again.
  nt.send({new_pb_message(2, 1, raftpb::message_type::MSG_TRANSFER_LEADER)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 3},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 2, 3},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 3},
    };
    check_raft_node_after_send_msg(tests);
  }

  next_ents(n1, *nt.storage.at(1));
  // Trigger campaign in node 1
  for (auto i = 0; i < n1.randomized_election_timeout_; ++i) {
    n1.tick();
  }
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::CANDIDATE, 3, 3},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 2, 3},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 3},
    };
    check_raft_node_after_send_msg(tests);
  }
}

// TestConfChangeCheckBeforeCampaign tests if unapplied ConfChange is checked before campaign.
TEST_F(raft_test_suit, test_conf_change_check_before_campaign) { test_conf_change_check_before_campaign(false); }

// TestConfChangeV2CheckBeforeCampaign tests if unapplied ConfChangeV2 is checked before campaign.
TEST_F(raft_test_suit, test_conf_change_v2_check_before_campaign) { test_conf_change_check_before_campaign(false); }

TEST_F(raft_test_suit, test_fast_log_rejection) {
  // 测试用例结构体定义
  struct test_case {
    std::string description;                  // 测试用例的描述
    lepton::pb::repeated_entry leader_log;    // Leader 上的日志
    lepton::pb::repeated_entry follower_log;  // Follower 上的日志
    uint64_t follower_compact;                // Follower 日志被压缩的索引
    uint64_t reject_hint_term;                // 拒绝响应中包含的任期
    uint64_t reject_hint_index;               // 拒绝响应中包含的索引
    uint64_t next_append_term;                // 拒绝后 Leader 的下个追加日志任期
    uint64_t next_append_index;               // 拒绝后 Leader 的下个追加日志索引
  };

  // 测试用例数组
  std::vector<test_case> tests = {
      {.description = R"(/*
         * 测试用例1: 
         * This case tests that leader can find the conflict index quickly.
         * Firstly leader appends (type=MsgApp,index=7,logTerm=4, entries=...);
         * After rejected leader appends (type=MsgApp,index=3,logTerm=2).
         */)",
       .leader_log = create_entries(1, {1, 2, 2, 4, 4, 4, 4}),
       .follower_log = create_entries(1, {1, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3}),
       .follower_compact = 0,
       .reject_hint_term = 3,
       .reject_hint_index = 7,
       .next_append_term = 2,
       .next_append_index = 3},
      {.description = R"(/*
           * 测试用例2:
           * This case tests that leader can find the conflict index quickly.
           * Firstly leader appends (type=MsgApp,index=8,logTerm=5, entries=...);
           * After rejected leader appends (type=MsgApp,index=4,logTerm=3).
           */)",
       .leader_log = create_entries(1, {1, 2, 2, 3, 4, 4, 4, 5}),
       .follower_log = create_entries(1, {1, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3}),
       .follower_compact = 0,
       .reject_hint_term = 3,
       .reject_hint_index = 8,
       .next_append_term = 3,
       .next_append_index = 4},
      {.description = R"(/*
         * 测试用例3:
         * This case tests that follower can find the conflict index quickly.
         * Firstly leader appends (type=MsgApp,index=4,logTerm=1, entries=...);
         * After rejected leader appends (type=MsgApp,index=1,logTerm=1).
         */)",
       .leader_log = create_entries(1, {1, 1, 1, 1}),
       .follower_log = create_entries(1, {1, 2, 2, 4}),
       .follower_compact = 0,
       .reject_hint_term = 1,
       .reject_hint_index = 1,
       .next_append_term = 1,
       .next_append_index = 1},
      {.description = R"(/*
         * 测试用例4:
         * This case is similar to the previous case. However, this time, the
         * leader has a longer uncommitted log tail than the follower.
         * Firstly leader appends (type=MsgApp,index=6,logTerm=1, entries=...);
         * After rejected leader appends (type=MsgApp,index=1,logTerm=1).
         */)",
       .leader_log = create_entries(1, {1, 1, 1, 1, 1, 1}),
       .follower_log = create_entries(1, {1, 2, 2, 4}),
       .follower_compact = 0,
       .reject_hint_term = 1,
       .reject_hint_index = 1,
       .next_append_term = 1,
       .next_append_index = 1},
      {.description = R"(/*
         * 测试用例5:
         * This case is similar to the previous case. However, this time, the
         * follower has a longer uncommitted log tail than the leader.
         * Firstly leader appends (type=MsgApp,index=4,logTerm=1, entries=...);
         * After rejected leader appends (type=MsgApp,index=1,logTerm=1).
         */)",
       .leader_log = create_entries(1, {1, 1, 1, 1}),
       .follower_log = create_entries(1, {1, 2, 2, 4, 4, 4}),
       .follower_compact = 0,
       .reject_hint_term = 1,
       .reject_hint_index = 1,
       .next_append_term = 1,
       .next_append_index = 1},
      {.description = R"(/*
         * 测试用例6:
         * An normal case that there are no log conflicts.
         * Firstly leader appends (type=MsgApp,index=5,logTerm=5, entries=...);
         * After rejected leader appends (type=MsgApp,index=4,logTerm=4).
         */)",
       .leader_log = create_entries(1, {1, 1, 1, 4, 5}),
       .follower_log = create_entries(1, {1, 1, 1, 4}),
       .follower_compact = 0,
       .reject_hint_term = 4,
       .reject_hint_index = 4,
       .next_append_term = 4,
       .next_append_index = 4},
      {.description = R"(/*
         * 测试用例7:
         * Test case from example comment in stepLeader (on leader).
         * Leader terms: [2, 5, 5, 5, 5, 5, 5, 5, 5]
         * Follower terms: [2, 4, 4, 4, 4, 4]
         */)",
       .leader_log = create_entries(1, {2, 5, 5, 5, 5, 5, 5, 5, 5}),
       .follower_log = create_entries(1, {2, 4, 4, 4, 4, 4}),
       .follower_compact = 0,
       .reject_hint_term = 4,
       .reject_hint_index = 6,
       .next_append_term = 2,
       .next_append_index = 1},
      {.description = R"(/*
         * 测试用例8:
         * Test case from example comment in handleAppendEntries (on follower).
         * Leader terms: [2, 2, 2, 2, 2] (indexes 1-5)
         * Follower terms: [2, 4, 4, 4, 4, 4, 4, 4] (indexes 1-8)
         */)",
       .leader_log = create_entries(1, {2, 2, 2, 2, 2}),
       .follower_log = create_entries(1, {2, 4, 4, 4, 4, 4, 4, 4}),
       .follower_compact = 0,
       .reject_hint_term = 2,
       .reject_hint_index = 1,
       .next_append_term = 2,
       .next_append_index = 1},
      {.description = R"(/*
         * 测试用例9:
         * A case when a stale MsgApp from leader arrives after the corresponding
         * log index got compacted.
         * A stale (type=MsgApp,index=3,logTerm=3,entries=[(term=3,index=4)]) is
         * delivered to a follower who has already compacted beyond log index 3. The
         * MsgAppResp rejection will return same index=3, with logTerm=0. The leader
         * will rollback by one entry, and send MsgApp with index=2,logTerm=1.
         */)",
       .leader_log = create_entries(1, {1, 1, 3}),
       .follower_log = create_entries(1, {1, 1, 3, 3, 3}),
       .follower_compact = 5,  // 索引 <=5 的条目已被压缩
       .reject_hint_term = 0,
       .reject_hint_index = 3,
       .next_append_term = 1,
       .next_append_index = 2},
  };
  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto &tt = tests[i];

    auto s1_ptr = new_test_memory_storage_ptr({with_peers({1, 2, 3})});
    auto &s1 = *s1_ptr;
    pro::proxy<storage_builer> s1_proxy = s1_ptr.get();
    auto leader_last = tt.leader_log[tt.leader_log.size() - 1];
    raftpb::hard_state leader_hs;
    auto leader_last_term = leader_last.term();
    leader_hs.set_term(leader_last_term);
    leader_hs.set_commit(leader_last.index());
    s1.append(std::move(tt.leader_log));
    auto n1 = new_test_learner_raft(1, 10, 1, std::move(s1_proxy));
    n1.become_candidate();  // bumps Term to last.Term
    n1.become_leader();

    auto s2_ptr = new_test_memory_storage_ptr({with_peers({1, 2, 3})});
    auto &s2 = *s2_ptr;
    pro::proxy<storage_builer> s2_proxy = s2_ptr.get();
    raftpb::hard_state follower_hs;
    follower_hs.set_term(leader_last_term);
    follower_hs.set_vote(1);
    follower_hs.set_commit(0);  // 这里表示 s2 没有 commit 任何日志
    s2.append(std::move(tt.follower_log));
    auto n2 = new_test_learner_raft(2, 10, 1, std::move(s2_proxy));
    if (tt.follower_compact != 0) {
      // 在已经设置 commit 为0的情况下，仍然执行了compact
      // 调用 Compact() 会裁剪掉 MemoryStorage 中 [1, followerCompact) 范围内的日志，只保留 followerCompact 开始之后的。
      // 注意：一个节点永远不能 compact 未 commit 的日志 —— 这会造成状态机和日志不一致，是 Raft 的严重错误。
      s2.compact(tt.follower_compact);
      // NB: the state of n2 after this compaction isn't realistic because the
      // commit index is still at 0. We do this to exercise a "doesn't happen"
      // edge case behaviour, in case it still does happen in some other way.
    }

    ASSERT_TRUE(n2.step(new_pb_message(1, 2, raftpb::MSG_HEARTBEAT)));
    auto msgs = n2.read_messages();
    ASSERT_EQ(1, msgs.size());
    ASSERT_EQ(magic_enum::enum_name(raftpb::MSG_HEARTBEAT_RESP), magic_enum::enum_name(msgs[0].type()));

    // 由于 commit 为 0， leader 会给 follower 发送 MSG_APP
    ASSERT_TRUE(n1.step(raftpb::message{msgs[0]}));
    msgs = n1.read_messages();
    ASSERT_EQ(1, msgs.size());
    ASSERT_EQ(magic_enum::enum_name(raftpb::MSG_APP), magic_enum::enum_name(msgs[0].type()));

    ASSERT_TRUE(n2.step(raftpb::message{msgs[0]}));
    msgs = n2.read_messages();
    ASSERT_EQ(1, msgs.size());
    ASSERT_EQ(magic_enum::enum_name(raftpb::MSG_APP_RESP), magic_enum::enum_name(msgs[0].type()));
    // 由于storage实际已经应用了日志，所以会 reject
    ASSERT_TRUE(msgs[0].reject());
    ASSERT_EQ(tt.reject_hint_term, msgs[0].log_term());
    ASSERT_EQ(tt.reject_hint_index, msgs[0].reject_hint());

    ASSERT_TRUE(n1.step(raftpb::message{msgs[0]}));
    msgs = n1.read_messages();
    ASSERT_EQ(1, msgs.size());
    ASSERT_EQ(tt.next_append_term, msgs[0].log_term());
    ASSERT_EQ(tt.next_append_index, msgs[0].index()) << fmt::format("#{}", i);
  }
}