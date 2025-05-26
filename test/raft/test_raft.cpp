#include <gtest/gtest.h>
#include <proxy.h>
#include <raft.pb.h>

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "conf_change.h"
#include "config.h"
#include "error.h"
#include "magic_enum.hpp"
#include "memory_storage.h"
#include "protobuf.h"
#include "raft.h"
#include "state.h"
#include "storage.h"
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

static auto nop_stepper = pro::make_proxy<state_machine_builer, black_hole>();

static void test_leader_election(bool pre_vote) {
  std::function<void(lepton::config &)> config_func;
  auto cand_state = lepton::state_type::STATE_CANDIDATE;
  std::uint64_t cand_term = 1;
  if (pre_vote) {
    config_func = pre_vote_config;
    // In pre-vote mode, an election that fails to complete
    // leaves the node in pre-candidate state without advancing
    // the term.
    cand_state = lepton::state_type::STATE_PRE_CANDIDATE;
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
    peers.emplace_back(state_machine_builer_pair{});
    peers.emplace_back(state_machine_builer_pair{});
    peers.emplace_back(state_machine_builer_pair{});
    test_cases.emplace_back(new_network_with_config(config_func, std::move(peers)), lepton::state_type::STATE_LEADER,
                            1);
  }
  {
    std::vector<state_machine_builer_pair> peers;
    peers.emplace_back(state_machine_builer_pair{});
    peers.emplace_back(state_machine_builer_pair{});
    peers.emplace_back(state_machine_builer_pair{});
    peers.back().init_black_hole_builder(pro::make_proxy<state_machine_builer, black_hole>());
    test_cases.emplace_back(new_network_with_config(config_func, std::move(peers)), lepton::state_type::STATE_LEADER,
                            1);
  }
  {
    std::vector<state_machine_builer_pair> peers;
    peers.emplace_back(state_machine_builer_pair{});
    peers.emplace_back(state_machine_builer_pair{});
    peers.back().init_black_hole_builder(pro::make_proxy<state_machine_builer, black_hole>());
    peers.emplace_back(state_machine_builer_pair{});
    peers.back().init_black_hole_builder(pro::make_proxy<state_machine_builer, black_hole>());
    test_cases.emplace_back(new_network_with_config(config_func, std::move(peers)), cand_state, cand_term);
  }
  {
    std::vector<state_machine_builer_pair> peers;
    peers.emplace_back(state_machine_builer_pair{});
    peers.emplace_back(state_machine_builer_pair{});
    peers.back().init_black_hole_builder(pro::make_proxy<state_machine_builer, black_hole>());
    peers.emplace_back(state_machine_builer_pair{});
    peers.back().init_black_hole_builder(pro::make_proxy<state_machine_builer, black_hole>());
    peers.emplace_back(state_machine_builer_pair{});
    test_cases.emplace_back(new_network_with_config(config_func, std::move(peers)), cand_state, cand_term);
  }
  {
    std::vector<state_machine_builer_pair> peers;
    peers.emplace_back(state_machine_builer_pair{});
    peers.emplace_back(ents_with_config(config_func, {1}));
    peers.emplace_back(ents_with_config(config_func, {1}));
    peers.emplace_back(ents_with_config(config_func, {1}));
    test_cases.emplace_back(new_network_with_config(config_func, std::move(peers)), lepton::state_type::STATE_FOLLOWER,
                            1);
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

  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::STATE_FOLLOWER), magic_enum::enum_name(n2.state_type_));
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

  ASSERT_NE(magic_enum::enum_name(lepton::state_type::STATE_LEADER), magic_enum::enum_name(n1.state_type_));

  // n1 should become leader
  set_randomized_election_timeout(n1, n1.election_timeout_);
  for (int i = 0; i < n1.election_timeout_; ++i) {
    n1.tick_func_();
  }
  n1.advance_messages_after_append();
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::STATE_LEADER), magic_enum::enum_name(n1.state_type_));
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::STATE_FOLLOWER), magic_enum::enum_name(n2.state_type_));

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
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::STATE_FOLLOWER), magic_enum::enum_name(n1.state_type_));
  ASSERT_EQ(magic_enum::enum_name(lepton::state_type::STATE_LEADER), magic_enum::enum_name(n2.state_type_));
}