#include <gtest/gtest.h>
#include <proxy.h>
#include <raft.pb.h>

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "conf_change.h"
#include "config.h"
#include "error.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "magic_enum.hpp"
#include "memory_storage.h"
#include "protobuf.h"
#include "raft.h"
#include "raft_log.h"
#include "state.h"
#include "storage.h"
#include "test_diff.h"
#include "test_raft_protobuf.h"
#include "test_raft_state_machine.h"
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

// nextEnts returns the appliable entries and updates the applied index.
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

static test_memory_storage_options with_learners(lepton::pb::repeated_uint64 &&learners) {
  return [&](lepton::memory_storage &ms) -> void {
    ms.snapshot_ref().mutable_metadata()->mutable_conf_state()->mutable_learners()->Swap(&learners);
  };
}

static memory_storage new_memory_storage(std::vector<test_memory_storage_options> &&options) {
  memory_storage ms;
  for (auto &option : options) {
    option(ms);
  }
  return ms;
}

static lepton::raft new_test_raft(lepton::config &&config) {
  auto r = new_raft(std::move(config));
  assert(r);
  return std::move(r.value());
}

static lepton::raft new_test_raft(std::uint64_t id, int election_tick, int heartbeat_tick,
                                  pro::proxy<storage_builer> &&storage) {
  auto r = new_raft(new_test_config(id, election_tick, heartbeat_tick, std::move(storage)));
  assert(r);
  return std::move(r.value());
}

static lepton::raft new_test_learner_raft(std::uint64_t id, int election_tick, int heartbeat_tick,
                                          pro::proxy<storage_builer> &&storage) {
  return new_test_raft(id, election_tick, heartbeat_tick, std::move(storage));
}

static raftpb::message new_pb_message(std::uint64_t from, std::uint64_t to, raftpb::message_type type) {
  raftpb::message msg;
  msg.set_from(from);
  msg.set_to(to);
  msg.set_type(type);
  return msg;
}

static raftpb::message new_pb_message(std::uint64_t from, std::uint64_t to, raftpb::message_type type,
                                      std::string data) {
  raftpb::message msg;
  msg.set_from(from);
  msg.set_to(to);
  msg.set_type(type);
  auto entry = msg.add_entries();
  entry->set_data(data);
  return msg;
}

static raftpb::message new_pb_message(std::uint64_t from, std::uint64_t to, raftpb::message_type type,
                                      lepton::pb::repeated_entry &&entries) {
  raftpb::message msg;
  msg.set_from(from);
  msg.set_to(to);
  msg.set_type(type);
  msg.mutable_entries()->Swap(&entries);
  return msg;
}

static state_machine_builer_pair ents_with_config(std::function<void(lepton::config &)> config_func,
                                                  std::vector<std::uint64_t> &&term) {
  memory_storage ms;
  lepton::pb::repeated_entry entries;
  for (std::size_t i = 0; i < term.size(); ++i) {
    auto entry = entries.Add();
    entry->set_index(i + 1);
    entry->set_term(term[i]);
  }
  assert(ms.append(std::move(entries)));

  auto storage = pro::make_proxy<storage_builer, memory_storage>(std::move(ms));
  auto cfg = new_test_config(1, 5, 1, std::move(storage));
  if (config_func != nullptr) {
    config_func(cfg);
  }
  auto r = new_raft(std::move(cfg));
  assert(r);
  r->reset(term.back());
  auto raft_handle = std::make_unique<lepton::raft>(std::move(r.value()));
  return state_machine_builer_pair{std::move(raft_handle)};
}

// votedWithConfig creates a raft state machine with Vote and Term set
// to the given value but no log entries (indicating that it voted in
// the given term but has not received any logs).
static state_machine_builer_pair voted_with_config(std::function<void(lepton::config &)> config_func,
                                                   std::uint64_t vote, std::uint64_t term) {
  memory_storage ms;
  raftpb::hard_state hard_state;
  hard_state.set_vote(vote);
  hard_state.set_term(term);
  ms.set_hard_state(std::move(hard_state));

  auto storage = pro::make_proxy<storage_builer, memory_storage>(std::move(ms));
  auto cfg = new_test_config(1, 5, 1, std::move(storage));
  if (config_func != nullptr) {
    config_func(cfg);
  }
  auto r = new_raft(std::move(cfg));
  assert(r);
  r->reset(term);
  auto raft_handle = std::make_unique<lepton::raft>(std::move(r.value()));
  return state_machine_builer_pair{std::move(raft_handle)};
}

TEST_F(raft_test_suit, progress_leader) {
  auto storage = pro::make_proxy<storage_builer>(new_memory_storage({with_peers({1, 2})}));
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
  auto storage = pro::make_proxy<storage_builer>(new_memory_storage({with_peers({1, 2})}));
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
  auto storage = pro::make_proxy<storage_builer>(new_memory_storage({with_peers({1, 2})}));
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
  auto storage = pro::make_proxy<storage_builer>(new_memory_storage({with_peers({1, 2})}));
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

  auto storage = pro::make_proxy<storage_builer>(new_memory_storage({with_peers({1, 2, 3})}));
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

void emplace_nil_peer(std::vector<state_machine_builer_pair> &peers) {
  peers.emplace_back(state_machine_builer_pair{});
}

void emplace_nop_stepper(std::vector<state_machine_builer_pair> &peers) {
  emplace_nil_peer(peers);
  peers.back().init_black_hole_builder(pro::make_proxy<state_machine_builer, black_hole>());
}

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
      1, 10, 1, pro::make_proxy<storage_builer>(new_memory_storage({{with_peers({1}), with_learners({2})}})));
  auto n2 = new_test_learner_raft(
      2, 10, 1, pro::make_proxy<storage_builer>(new_memory_storage({{with_peers({1}), with_learners({2})}})));

  n1.become_follower(1, NONE);
  n2.become_follower(1, NONE);

  // n2 is learner. Learner should not start election even when times out.
  set_randomized_election_timeout(n2, n2.election_timeout_);
  for (int i = 0; i < n2.election_timeout_; ++i) {
    n2.tick_func_();
  }

  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::FOLLOWER), magic_enum::enum_name(n2.state_type_));
}

// TestLearnerPromotion verifies that the learner should not election until
// it is promoted to a normal peer.
TEST_F(raft_test_suit, test_learner_promotion) {
  auto n1 = new_test_learner_raft(
      1, 10, 1, pro::make_proxy<storage_builer>(new_memory_storage({{with_peers({1}), with_learners({2})}})));
  auto n2 = new_test_learner_raft(
      2, 10, 1, pro::make_proxy<storage_builer>(new_memory_storage({{with_peers({1}), with_learners({2})}})));

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
    n1.tick_func_();
  }
  n1.advance_messages_after_append();
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::LEADER), magic_enum::enum_name(n1.state_type_));
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::FOLLOWER), magic_enum::enum_name(n2.state_type_));

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_BEAT)});

  raftpb::conf_change cc1;
  cc1.set_node_id(2);
  cc1.set_type(raftpb::conf_change_type::CONF_CHANGE_ADD_NODE);
  raftpb::conf_change cc2;
  cc2.CopyFrom(cc1);
  n1.apply_conf_change(lepton::pb::conf_change_as_v2(std::move(cc1)));
  n2.apply_conf_change(lepton::pb::conf_change_as_v2(std::move(cc2)));
  ASSERT_FALSE(n2.is_learner_);

  // n2 start election, should become leader
  set_randomized_election_timeout(n2, n2.election_timeout_);
  for (int i = 0; i < n2.election_timeout_; ++i) {
    n2.tick_func_();
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
      2, 10, 1, pro::make_proxy<storage_builer>(new_memory_storage({{with_peers({1}), with_learners({2})}})));

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
    auto r = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_memory_storage({{with_peers({1, 2, 3})}})));
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
    ASSERT_EQ(magic_enum::enum_name(lepton::pb::vote_response_type(vt)), magic_enum::enum_name(resp.type()));
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
    n1.tick_func_();
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
  struct test_case {
    raft *raft_handle;
    lepton::state_type expected_state;
    std::uint64_t expected_term;
    std::uint64_t last_index;
  };

  auto a = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_memory_storage({{with_peers({1, 2, 3})}})));
  auto b = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_memory_storage({{with_peers({1, 2, 3})}})));
  auto c = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_memory_storage({{with_peers({1, 2, 3})}})));
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
    std::vector<test_case> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::CANDIDATE, 1, 0},
    };
    for (const auto &iter : tests) {
      ASSERT_EQ(magic_enum::enum_name(iter.expected_state), magic_enum::enum_name(iter.raft_handle->state_type_));
      ASSERT_EQ(iter.expected_term, iter.raft_handle->term_);
      ASSERT_EQ(iter.last_index, iter.raft_handle->raft_log_handle_.last_index());
    }
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
    std::vector<test_case> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 0},
    };
    for (const auto &iter : tests) {
      ASSERT_EQ(magic_enum::enum_name(iter.expected_state), magic_enum::enum_name(iter.raft_handle->state_type_));
      ASSERT_EQ(iter.expected_term, iter.raft_handle->term_);
      ASSERT_EQ(iter.last_index, iter.raft_handle->raft_log_handle_.last_index());
    }
  }
}

TEST_F(raft_test_suit, dueling_pre_candidates) {
  struct test_case {
    raft *raft_handle;
    lepton::state_type expected_state;
    std::uint64_t expected_term;
    std::uint64_t last_index;
  };

  auto a_cfg =
      new_test_config(1, 10, 1, pro::make_proxy<storage_builer>(new_memory_storage({{with_peers({1, 2, 3})}})));
  auto b_cfg =
      new_test_config(1, 10, 1, pro::make_proxy<storage_builer>(new_memory_storage({{with_peers({1, 2, 3})}})));
  auto c_cfg =
      new_test_config(1, 10, 1, pro::make_proxy<storage_builer>(new_memory_storage({{with_peers({1, 2, 3})}})));
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
    std::vector<test_case> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 0},
    };
    for (const auto &iter : tests) {
      ASSERT_EQ(magic_enum::enum_name(iter.expected_state), magic_enum::enum_name(iter.raft_handle->state_type_));
      ASSERT_EQ(iter.expected_term, iter.raft_handle->term_);
      ASSERT_EQ(iter.last_index, iter.raft_handle->raft_log_handle_.last_index());
    }
  }

  nt.recover();

  // Candidate 3 now increases its term and tries to vote again.
  // With PreVote, it does not disrupt the leader.
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});

  {
    std::vector<test_case> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 0},
    };
    for (const auto &iter : tests) {
      ASSERT_EQ(magic_enum::enum_name(iter.expected_state), magic_enum::enum_name(iter.raft_handle->state_type_));
      ASSERT_EQ(iter.expected_term, iter.raft_handle->term_);
      ASSERT_EQ(iter.last_index, iter.raft_handle->raft_log_handle_.last_index());
    }
  }
}

TEST_F(raft_test_suit, candidates_concede) {
  struct test_case {
    raft *raft_handle;
    lepton::state_type expected_state;
    std::uint64_t expected_term;
    std::uint64_t last_index;
  };

  std::vector<state_machine_builer_pair> peers;
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  auto nt = new_network(std::move(peers));

  nt.isolate(1);

  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_case> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::CANDIDATE, 1, 0},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::LEADER, 1, 1},
    };
    for (const auto &iter : tests) {
      ASSERT_EQ(magic_enum::enum_name(iter.expected_state), magic_enum::enum_name(iter.raft_handle->state_type_));
      ASSERT_EQ(iter.expected_term, iter.raft_handle->term_);
      ASSERT_EQ(iter.last_index, iter.raft_handle->raft_log_handle_.last_index());
    }
  }

  // heal the partition
  nt.recover();
  // send heartbeat; reset wait
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_BEAT)});
  {
    std::vector<test_case> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::LEADER, 1, 1},
    };
    for (const auto &iter : tests) {
      ASSERT_EQ(magic_enum::enum_name(iter.expected_state), magic_enum::enum_name(iter.raft_handle->state_type_));
      ASSERT_EQ(iter.expected_term, iter.raft_handle->term_);
      ASSERT_EQ(iter.last_index, iter.raft_handle->raft_log_handle_.last_index());
    }
  }

  std::string data{"force follower"};
  // send a proposal to 3 to flush out a MsgApp to 1
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_PROP, data)});
  // send heartbeat; flush out commit
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_BEAT)});
  {
    std::vector<test_case> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 1, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::LEADER, 1, 2},
    };
    for (const auto &iter : tests) {
      ASSERT_EQ(magic_enum::enum_name(iter.expected_state), magic_enum::enum_name(iter.raft_handle->state_type_));
      ASSERT_EQ(iter.expected_term, iter.raft_handle->term_);
      ASSERT_EQ(iter.last_index, iter.raft_handle->raft_log_handle_.last_index());
    }
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
  struct test_case {
    raft *raft_handle;
    lepton::state_type expected_state;
    std::uint64_t expected_term;
    std::uint64_t last_index;
  };
  std::vector<state_machine_builer_pair> peers;
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  auto nt = new_network(std::move(peers));

  // make 0 leader @ term 3
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_case> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    for (const auto &iter : tests) {
      ASSERT_EQ(magic_enum::enum_name(iter.expected_state), magic_enum::enum_name(iter.raft_handle->state_type_));
      ASSERT_EQ(iter.expected_term, iter.raft_handle->term_);
      ASSERT_EQ(iter.last_index, iter.raft_handle->raft_log_handle_.last_index());
    }
  }
  nt.send({new_pb_message(2, 2, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_case> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::LEADER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
    };
    for (const auto &iter : tests) {
      ASSERT_EQ(magic_enum::enum_name(iter.expected_state), magic_enum::enum_name(iter.raft_handle->state_type_));
      ASSERT_EQ(iter.expected_term, iter.raft_handle->term_);
      ASSERT_EQ(iter.last_index, iter.raft_handle->raft_log_handle_.last_index());
    }
  }
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_case> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 3, 3},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 3, 3},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 3, 3},
    };
    for (const auto &iter : tests) {
      ASSERT_EQ(magic_enum::enum_name(iter.expected_state), magic_enum::enum_name(iter.raft_handle->state_type_));
      ASSERT_EQ(iter.expected_term, iter.raft_handle->term_);
      ASSERT_EQ(iter.last_index, iter.raft_handle->raft_log_handle_.last_index());
    }
  }
  // pretend we're an old leader trying to make progress; this entry is expected to be ignored.
  auto msg1 = new_pb_message(2, 1, raftpb::message_type::MSG_APP);
  msg1.set_term(2);
  auto entries1 = create_entries(3, {2});
  msg1.mutable_entries()->Swap(&entries1);
  nt.send({msg1});
  {
    std::vector<test_case> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 3, 3},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 3, 3},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 3, 3},
    };
    for (const auto &iter : tests) {
      ASSERT_EQ(magic_enum::enum_name(iter.expected_state), magic_enum::enum_name(iter.raft_handle->state_type_));
      ASSERT_EQ(iter.expected_term, iter.raft_handle->term_);
      ASSERT_EQ(iter.last_index, iter.raft_handle->raft_log_handle_.last_index());
    }
  }
  // commit a new entry
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "somedata")});
  {
    std::vector<test_case> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 3, 4},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 3, 4},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 3, 4},
    };
    for (const auto &iter : tests) {
      ASSERT_EQ(magic_enum::enum_name(iter.expected_state), magic_enum::enum_name(iter.raft_handle->state_type_));
      ASSERT_EQ(iter.expected_term, iter.raft_handle->term_);
      ASSERT_EQ(iter.last_index, iter.raft_handle->raft_log_handle_.last_index());
    }
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