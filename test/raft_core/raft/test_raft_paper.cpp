#include <gtest/gtest.h>
#include <proxy.h>
#include <raft.pb.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdio>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/types/span.h"
#include "config.h"
#include "enum_name.h"
#include "fmt/base.h"
#include "fmt/format.h"
#include "lepton_error.h"
#include "memory_storage.h"
#include "protobuf.h"
#include "raft.h"
#include "raft_log.h"
#include "state.h"
#include "storage.h"
#include "test_diff.h"
#include "test_raft_protobuf.h"
#include "test_raft_utils.h"
#include "types.h"
using namespace lepton;

static raftpb::message accept_and_reply(const raftpb::message& m) {
  assert(m.type() == raftpb::MSG_APP);
  auto resp = new_pb_message(m.to(), m.from(), m.term(), raftpb::message_type::MSG_APP_RESP);
  resp.set_index(m.index() + static_cast<std::uint64_t>(m.entries_size()));
  return resp;
}

// raft node 成为 leader 以后，需要发一个空的MSG_APP
static void commit_noop_entry(lepton::raft& r, lepton::memory_storage& s) {
  ASSERT_EQ(lepton::state_type::LEADER, r.state_type_) << "it should only be used when it is the leader";
  r.bcast_append();
  // simulate the response of MsgApp
  auto msgs = r.read_messages();
  for (auto& msg : msgs) {
    auto unexpected_msg = "not a message to append noop entry";
    ASSERT_EQ(raftpb::message_type::MSG_APP, msg.type()) << unexpected_msg;
    ASSERT_EQ(1, msg.entries_size()) << unexpected_msg;
    ASSERT_FALSE(msg.entries(0).has_data()) << unexpected_msg;
    r.step(accept_and_reply(msg));
  }
  // ignore further messages to refresh followers' commit index
  r.read_messages();
  s.append(lepton::pb::convert_span_entry(r.raft_log_handle_.next_unstable_ents()));
  r.raft_log_handle_.applied_to(r.raft_log_handle_.committed(), 0);
  r.raft_log_handle_.stable_to(r.raft_log_handle_.last_entry_id());
}

class raft_paper_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

// testUpdateTermFromMessage tests that if one server’s current term is
// smaller than the other’s, then it updates its current term to the larger
// value. If a candidate or leader discovers that its term is out of date,
// it immediately reverts to follower state.
// Reference: section 5.1
// Raft uses the term number to detect stale messages. If a node receives a message with a term number smaller than its
// current term, it rejects that message
static void test_update_term_from_message(lepton::state_type state_type) {
  auto r = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  switch (state_type) {
    case state_type::FOLLOWER: {
      r.become_follower(1, 2);
      break;
    }
    case state_type::CANDIDATE: {
      r.become_candidate();
      break;
    }
    case state_type::LEADER: {
      r.become_candidate();
      r.become_leader();
    }
    case state_type::PRE_CANDIDATE:
      break;
  }
  raftpb::message m;
  m.set_type(::raftpb::message_type::MSG_APP);
  m.set_term(2);
  r.step(std::move(m));
  ASSERT_EQ(2, r.term_);
  ASSERT_EQ(lepton::state_type::FOLLOWER, r.state_type_);
}

TEST_F(raft_paper_test_suit, test_follower_update_term_from_message) {
  test_update_term_from_message(lepton::state_type::FOLLOWER);
}

TEST_F(raft_paper_test_suit, test_candidate_update_term_from_message) {
  test_update_term_from_message(lepton::state_type::CANDIDATE);
}

TEST_F(raft_paper_test_suit, test_leader_update_term_from_message) {
  test_update_term_from_message(lepton::state_type::LEADER);
}

// TestRejectStaleTermMessage tests that if a server receives a request with
// a stale term number, it rejects the request.
// Our implementation ignores the request instead.
// Reference: section 5.1
// Raft uses the term number to detect stale messages. If a node receives a message with a term number smaller than its
// current term, it rejects that message
TEST_F(raft_paper_test_suit, test_reject_stale_term_message) {
  auto called = false;
  auto fake_step = [&](lepton::raft& r, raftpb::message&& m) -> lepton::leaf::result<void> {
    called = true;
    return {};
  };
  auto r = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  r.step_func_ = fake_step;
  raftpb::hard_state hs;
  hs.set_term(2);
  r.load_state(hs);
  raftpb::message m;
  m.set_type(::raftpb::message_type::MSG_APP);
  m.set_term(r.term_ - 1);
  r.step(std::move(m));
  ASSERT_FALSE(called);
}

// TestStartAsFollower tests that when servers start up, they begin as followers.
// Reference: section 5.2
// When servers start up, they begin as followers.
TEST_F(raft_paper_test_suit, test_start_as_follower) {
  auto r = new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  ASSERT_EQ(lepton::state_type::FOLLOWER, r.state_type_);
}

// TestLeaderBcastBeat tests that if the leader receives a heartbeat tick,
// it will send a MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries
// as heartbeat to all followers.
// Reference: section 5.2
// 验证心跳是否广播到集群中所有其他节点（排除自身）
TEST_F(raft_paper_test_suit, test_leader_bcast_beat) {
  // heartbeat interval
  constexpr auto hi = 1;
  auto r =
      new_test_raft(1, 10, hi, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  r.become_candidate();
  r.become_leader();
  for (auto i = 0; i < 10; ++i) {
    lepton::pb::repeated_entry entries;
    auto entry = entries.Add();
    entry->set_index(static_cast<std::uint64_t>(i) + 1);
    must_append_entry(r, std::move(entries));
  }
  for (auto i = 0; i < hi; ++i) {
    r.tick();
  }

  auto msgs = r.read_messages();
  auto msg_heartbeat = new_pb_message(1, 2, 1, raftpb::message_type::MSG_HEARTBEAT);
  msg_heartbeat.set_commit(0);
  lepton::pb::repeated_message expected_msgs;
  expected_msgs.Add()->CopyFrom(msg_heartbeat);
  msg_heartbeat.set_to(3);
  expected_msgs.Add()->CopyFrom(msg_heartbeat);
  ASSERT_TRUE(compare_repeated_message(expected_msgs, msgs));
}

// testNonleaderStartElection tests that if a follower receives no communication
// over election timeout, it begins an election to choose a new leader. It
// increments its current term and transitions to candidate state. It then
// votes for itself and issues RequestVote RPCs in parallel to each of the
// other servers in the cluster.
// Reference: section 5.2
// Also if a candidate fails to obtain a majority, it will time out and
// start a new election by incrementing its term and initiating another
// round of RequestVote RPCs.
// Reference: section 5.2
static void test_nonleader_start_election(lepton::state_type state_type) {
  constexpr auto et = 10;
  auto r = new_test_raft(1, et, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  switch (state_type) {
    case state_type::FOLLOWER: {
      r.become_follower(1, 2);
      ASSERT_EQ(1, r.term_);
      break;
    }
    case state_type::CANDIDATE: {
      r.become_candidate();
      ASSERT_EQ(1, r.term_);
      break;
    }
    case state_type::LEADER:
    case state_type::PRE_CANDIDATE:
      ASSERT_TRUE(false);
      break;
  }
  for (auto i = 1; i < 2 * et; ++i) {
    r.tick();
  }
  r.advance_messages_after_append();

  ASSERT_EQ(2, r.term_) << fmt::format("randomized_election_timeout:{} election_elapsed:{}",
                                       r.randomized_election_timeout_, r.election_elapsed_);
  ASSERT_EQ(lepton::state_type::CANDIDATE, r.state_type_);
  ASSERT_TRUE(r.trk_.votes_view().at(r.id_));

  auto msgs = r.read_messages();
  auto msg_vote = new_pb_message(1, 2, 2, raftpb::message_type::MSG_VOTE);
  msg_vote.set_log_term(0);
  msg_vote.set_index(0);
  lepton::pb::repeated_message expected_msgs;
  expected_msgs.Add()->CopyFrom(msg_vote);
  msg_vote.set_to(3);
  expected_msgs.Add()->CopyFrom(msg_vote);
  ASSERT_TRUE(compare_repeated_message(expected_msgs, msgs));
}

TEST_F(raft_paper_test_suit, test_follower_start_election) {
  test_nonleader_start_election(lepton::state_type::FOLLOWER);
}

TEST_F(raft_paper_test_suit, test_candidate_start_new_election) {
  test_nonleader_start_election(lepton::state_type::CANDIDATE);
}

// TestLeaderElectionInOneRoundRPC tests all cases that may happen in
// leader election during one round of RequestVote RPC:
// a) it wins the election
// b) it loses the election
// c) it is unclear about the result
// Reference: section 5.2
TEST_F(raft_paper_test_suit, test_leader_election_in_one_round_rpc) {
  // 测试用例结构定义
  struct test_case {
    std::size_t size;                // 集群大小
    std::map<uint64_t, bool> votes;  // 收到的投票 (节点ID -> 投票结果)
    state_type expected_state;       // 期望的状态
  };

  // 测试用例集合
  std::vector<test_case> test_cases = {
      // 赢得选举（收到大部分节点的赞成票）
      // win the election when receiving votes from a majority of the servers
      {1, {}, state_type::LEADER},
      {3, {{2, true}, {3, true}}, state_type::LEADER},
      {3, {{2, true}}, state_type::LEADER},
      {5, {{2, true}, {3, true}, {4, true}, {5, true}}, state_type::LEADER},
      {5, {{2, true}, {3, true}, {4, true}}, state_type::LEADER},
      {5, {{2, true}, {3, true}}, state_type::LEADER},

      // 收到大部分反对票时回到跟随者状态
      // return to follower state if it receives vote denial from a majority
      {3, {{2, false}, {3, false}}, state_type::FOLLOWER},
      {5, {{2, false}, {3, false}, {4, false}, {5, false}}, state_type::FOLLOWER},
      {5, {{2, true}, {3, false}, {4, false}, {5, false}}, state_type::FOLLOWER},

      // 未获得多数票时保持候选人状态
      // stay in candidate if it does not obtain the majority
      {3, {}, state_type::CANDIDATE},
      {5, {{2, true}}, state_type::CANDIDATE},
      {5, {{2, false}, {3, false}}, state_type::CANDIDATE},
      {5, {}, state_type::CANDIDATE},
  };
  for (const auto& [idx, tt] : test_cases | std::views::transform([&](auto&& item) {
                                 return std::pair{&item - test_cases.data(), std::forward<decltype(item)>(item)};
                               })) {
    auto r = new_test_raft(
        1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers(ids_by_size(tt.size))}})));
    r.step(new_pb_message(1, 1, raftpb::MSG_HUP));
    r.advance_messages_after_append();
    for (auto& [id, vote] : tt.votes) {
      auto vote_resp = new_pb_message(id, 1, r.term_, raftpb::message_type::MSG_VOTE_RESP);
      vote_resp.set_reject(!vote);
      r.step(std::move(vote_resp));
    }

    ASSERT_EQ(magic_enum::enum_name(tt.expected_state), magic_enum::enum_name(r.state_type_))
        << fmt::format("#{}", idx);
    ASSERT_EQ(1, r.term_) << fmt::format("#{}", idx);
  }
}

// TestFollowerVote tests that each follower will vote for at most one
// candidate in a given term, on a first-come-first-served basis.
// Reference: section 5.2
TEST_F(raft_paper_test_suit, test_follower_vote) {
  struct test_case {
    uint64_t vote;   // 当前投票情况
    uint64_t nvote;  // 新投票请求
    bool wreject;    // 期望的拒绝结果
  };

  // 创建测试用例数组
  std::vector<test_case> tests = {
      {NONE, 2, false},  // 空投票接收 2 号投票 -> 接受
      {NONE, 3, false},  // 空投票接收 3 号投票 -> 接受
      {2, 2, false},     // 已投 2 号接收 2 号投票 -> 接受
      {3, 3, false},     // 已投 3 号接收 3 号投票 -> 接受
      {2, 3, true},      // 已投 2 号接收 3 号投票 -> 拒绝
      {3, 2, true}       // 已投 3 号接收 2 号投票 -> 拒绝
  };
  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto& tt = tests[i];
    auto r =
        new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
    raftpb::hard_state hs;
    hs.set_term(1);
    hs.set_vote(tt.vote);
    r.load_state(hs);
    r.step(new_pb_message(tt.nvote, 1, 1, raftpb::message_type::MSG_VOTE));
    auto msg_vote_resp = new_pb_message(1, tt.nvote, 1, raftpb::message_type::MSG_VOTE_RESP);
    if (tt.wreject) {
      msg_vote_resp.set_reject(tt.wreject);
    }
    lepton::pb::repeated_message expected_msgs;
    expected_msgs.Add()->CopyFrom(msg_vote_resp);
    ASSERT_TRUE(compare_repeated_message(expected_msgs, r.msgs_after_append_)) << fmt::format("#{}", i);
  }
}

// TestCandidateFallback tests that while waiting for votes,
// if a candidate receives an AppendEntries RPC from another server claiming
// to be leader whose term is at least as large as the candidate's current term,
// it recognizes the leader as legitimate and returns to follower state.
// Reference: section 5.2
TEST_F(raft_paper_test_suit, test_candidate_fallback) {
  lepton::pb::repeated_message tests;
  tests.Add(new_pb_message(2, 1, 1, raftpb::message_type::MSG_APP));
  tests.Add(new_pb_message(2, 1, 2, raftpb::message_type::MSG_APP));
  for (auto i = 0; i < tests.size(); ++i) {
    auto& tt = tests[i];

    auto r =
        new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
    r.step(new_pb_message(1, 1, raftpb::message_type::MSG_HUP));
    ASSERT_EQ(lepton::state_type::CANDIDATE, r.state_type_);

    r.step(raftpb::message{tt});

    ASSERT_EQ(lepton::state_type::FOLLOWER, r.state_type_);
    ASSERT_EQ(tt.term(), r.term_);
  }
}

// testNonleaderElectionTimeoutRandomized tests that election timeout for
// follower or candidate is randomized.
// Reference: section 5.2
static void test_nonleader_election_timeout_randomized(lepton::state_type state_type) {
  constexpr auto et = 10;
  auto r = new_test_raft(1, et, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  std::unordered_set<int> timeouts;
  for (auto round = 0; round < 50 * et; ++round) {
    switch (state_type) {
      case state_type::FOLLOWER: {
        r.become_follower(r.term_ + 1, 2);
        break;
      }
      case state_type::CANDIDATE: {
        r.become_candidate();
        break;
      }
      case state_type::LEADER:
      case state_type::PRE_CANDIDATE:
        ASSERT_TRUE(false);
        break;
    }
    auto time = 0;
    while (r.read_messages().empty()) {
      r.tick();
      time++;
    }
    timeouts.insert(time);
  }

  // 选举超时时间必须在配置的基础超时（et）和两倍超时（2*et）之间​​随机分布​
  for (auto d = et; d < 2 * et; ++d) {
    ASSERT_TRUE(timeouts.contains(d)) << fmt::format("timeout in {} ticks should happen", d);
  }
}

TEST_F(raft_paper_test_suit, test_follower_election_timeout_randomized) {
  test_nonleader_election_timeout_randomized(lepton::state_type::FOLLOWER);
}

TEST_F(raft_paper_test_suit, test_candidate_election_timeout_randomized) {
  test_nonleader_election_timeout_randomized(lepton::state_type::CANDIDATE);
}

// testNonleadersElectionTimeoutNonconflict tests that in most cases only a
// single server(follower or candidate) will time out, which reduces the
// likelihood of split vote in the new election.
// Reference: section 5.2
// 验证目标：​​选举超时随机化如何显著降低多个节点同时发起选举的概率
static void test_nonleaders_election_timeout_nonconflict(lepton::state_type state_type) {
  constexpr auto et = 10;
  std::size_t size = 5;
  std::vector<lepton::raft> rs;
  rs.reserve(size);
  auto ids = ids_by_size(size);
  for (std::size_t i = 0; i < size; ++i) {
    rs.emplace_back(new_test_raft(
        ids[i], et, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers(ids_by_size(size))}}))));
  }

  auto conflicts = 0;
  for (auto round = 0; round < 1000; ++round) {
    for (auto& r : rs) {
      switch (state_type) {
        case state_type::FOLLOWER: {
          r.become_follower(r.term_ + 1, NONE);
          break;
        }
        case state_type::CANDIDATE: {
          r.become_candidate();
          break;
        }
        case state_type::LEADER:
        case state_type::PRE_CANDIDATE:
          ASSERT_TRUE(false);
          break;
      }
    }

    auto timeout_num = 0;
    while (timeout_num == 0) {
      for (auto& r : rs) {
        r.tick();
        auto msgs = r.read_messages();
        if (!msgs.empty()) {
          timeout_num++;
        }
      }
    }

    // several rafts time out at the same tick
    if (timeout_num > 1) {
      conflicts++;
    }
  }
  SPDLOG_INFO("conflicts: {}", static_cast<double>(conflicts) / 1000);
  ASSERT_LE(static_cast<double>(conflicts) / 1000, 0.3);
}

TEST_F(raft_paper_test_suit, test_followers_election_timeout_nonconflict) {
  test_nonleaders_election_timeout_nonconflict(lepton::state_type::FOLLOWER);
}

TEST_F(raft_paper_test_suit, test_candidates_election_timeout_nonconflict) {
  test_nonleaders_election_timeout_nonconflict(lepton::state_type::CANDIDATE);
}

// TestLeaderStartReplication tests that when receiving client proposals,
// the leader appends the proposal to its log as a new entry, then issues
// AppendEntries RPCs in parallel to each of the other servers to replicate
// the entry. Also, when sending an AppendEntries RPC, the leader includes
// the index and term of the entry in its log that immediately precedes
// the new entries.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
/*
1. ​​基本提交机制验证​​
验证当条目被​​安全复制​​到多数节点后：
- 领导者能正确将条目标记为已提交 (committed)
- 条目可被应用到领导者的状态机 (nextCommittedEnts)
2. ​​提交索引传播机制验证​​
验证领导者会：
- 跟踪已知的最高提交索引
- 在后续的心跳/日志复制消息中包含此索引
- 确保其他节点最终能获知新的提交索引
*/
TEST_F(raft_paper_test_suit, test_leader_start_replication) {
  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1, 2, 3})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  auto r = new_test_raft(1, 10, 1, std::move(storage_proxy));
  r.become_candidate();
  r.become_leader();
  commit_noop_entry(r, mm_storage);
  auto li = r.raft_log_handle_.last_index();
  r.step(new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "some data"));

  auto msgs = r.read_messages();
  for (auto& msg : msgs) {
    r.step(accept_and_reply(msg));
  }

  ASSERT_EQ(li + 1, r.raft_log_handle_.committed());
  auto ents = r.raft_log_handle_.next_committed_ents(true);
  lepton::pb::repeated_entry expected_ents;
  auto entry = expected_ents.Add();
  entry->set_index(li + 1);
  entry->set_term(1);
  entry->set_data("some data");
  ASSERT_TRUE(compare_repeated_entry(expected_ents, ents));

  msgs = r.read_messages();
  std::sort(msgs.begin(), msgs.end(), [](const raftpb::message& lhs, const raftpb::message& rhs) {
    return lhs.DebugString() < rhs.DebugString();
  });
  for (auto i = 0; i < msgs.size(); ++i) {
    auto& msg = msgs[i];
    ASSERT_EQ(i + 2, msg.to());
    ASSERT_EQ(raftpb::message_type::MSG_APP, msg.type());
    ASSERT_EQ(li + 1, msg.commit());
  }
}

// TestLeaderAcknowledgeCommit tests that a log entry is committed once the
// leader that created the entry has replicated it on a majority of the servers.
// Reference: section 5.3
// 验证 MSG_APP 只有被多数派确认的清空下才会提交commit
TEST_F(raft_paper_test_suit, test_leader_acknowledge_commit) {
  struct test_case {
    std::size_t size;                               // 集群大小
    std::map<uint64_t, bool> non_leader_acceptors;  // 非领导者接受者（节点ID -> 是否接受）
    bool expected_ack;                              // 预期是否被确认 (wack)
  };

  std::vector<test_case> tests = {
      // size, acceptors, expectedAck
      {1, {}, true},   // 单节点集群应成功
      {3, {}, false},  // 3节点集群无接受者应失败

      // 3节点集群
      {3, {{2, true}}, true},             // 1个接受者成功
      {3, {{2, true}, {3, true}}, true},  // 2个接受者成功

      // 5节点集群
      {5, {}, false},                                           // 无接受者失败
      {5, {{2, true}}, false},                                  // 1个接受者不足
      {5, {{2, true}, {3, true}}, true},                        // 2个接受者成功
      {5, {{2, true}, {3, true}, {4, true}}, true},             // 3个接受者成功
      {5, {{2, true}, {3, true}, {4, true}, {5, true}}, true},  // 4个接受者成功
  };

  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto& tt = tests[i];

    auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers(ids_by_size(tt.size))});
    auto& mm_storage = *mm_storage_ptr;
    pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
    auto r = new_test_raft(1, 10, 1, std::move(storage_proxy));
    r.become_candidate();
    r.become_leader();
    commit_noop_entry(r, mm_storage);
    auto li = r.raft_log_handle_.last_index();
    r.step(new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "some data"));
    r.advance_messages_after_append();
    auto msgs = r.read_messages();
    for (auto& msg : msgs) {
      if (tt.non_leader_acceptors.contains(msg.to())) {
        r.step(accept_and_reply(msg));
      }
    }

    ASSERT_EQ(tt.expected_ack, r.raft_log_handle_.committed() > li);
  }
}

// TestLeaderCommitPrecedingEntries tests that when leader commits a log entry,
// it also commits all preceding entries in the leader’s log, including
// entries created by previous leaders.
// Also, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
TEST_F(raft_paper_test_suit, test_leader_commit_preceding_entries) {
  std::vector<lepton::pb::repeated_entry> tests = {
      {},
      create_entries({{1, 2}}),
      create_entries(1, {1, 2}),
      create_entries(1, {1}),
  };

  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto& tt = tests[i];
    const std::uint64_t li = static_cast<std::uint64_t>(tt.size());
    lepton::pb::repeated_entry expected_ents;
    for (const auto& iter_entry : tt) {
      expected_ents.Add()->CopyFrom(iter_entry);
    }
    {  // leader empty entry
      auto entry = expected_ents.Add();
      entry->set_index(li + 1);
      entry->set_term(3);
    }
    {  // MSG_PROP entry
      auto entry = expected_ents.Add();
      entry->set_index(li + 2);
      entry->set_term(3);
      entry->set_data("some data");
    }

    auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1, 2, 3})});
    auto& mm_storage = *mm_storage_ptr;
    mm_storage.append(std::move(tt));
    pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
    auto r = new_test_raft(1, 10, 1, std::move(storage_proxy));
    raftpb::hard_state hs;
    hs.set_term(2);
    r.load_state(hs);

    r.become_candidate();
    r.become_leader();
    r.step(new_pb_message(1, 1, raftpb::message_type::MSG_PROP, "some data"));
    auto msgs = r.read_messages();
    for (auto& msg : msgs) {
      r.step(accept_and_reply(msg));
    }

    auto ents = r.raft_log_handle_.next_committed_ents(true);
    ASSERT_TRUE(compare_repeated_entry(expected_ents, ents)) << fmt::format("#{}", i);
  }
}

// TestFollowerCommitEntry tests that once a follower learns that a log entry
// is committed, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
TEST_F(raft_paper_test_suit, test_follower_commit_entry) {
  struct test_case {
    lepton::pb::repeated_entry ents;
    std::uint64_t commit;
  };

  std::vector<test_case> tests = {
      {.ents = create_entries_with_entry_vec({
           create_entry(1, 1, "some data"),
       }),
       .commit = 1},
      {.ents = create_entries_with_entry_vec({
           create_entry(1, 1, "some data"),
           create_entry(2, 1, "some data2"),
       }),
       .commit = 2},
      {.ents = create_entries_with_entry_vec({
           create_entry(1, 1, "some data2"),
           create_entry(2, 1, "some data"),
       }),
       .commit = 2},
      {.ents = create_entries_with_entry_vec({
           create_entry(1, 1, "some data"),
           create_entry(2, 1, "some data2"),
       }),
       .commit = 1},
  };
  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto& tt = tests[i];
    auto r =
        new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
    r.become_follower(1, 2);
    auto msg = new_pb_message(2, 1, raftpb::message_type::MSG_APP);
    for (auto& entry : tt.ents) {
      msg.add_entries()->CopyFrom(entry);
    }
    msg.set_commit(tt.commit);
    r.step(std::move(msg));

    ASSERT_EQ(tt.commit, r.raft_log_handle_.committed());
    auto expected_entries_view = absl::MakeSpan(tt.ents.data(), tt.commit);
    auto entries = r.raft_log_handle_.next_committed_ents(true);
    ASSERT_TRUE(compare_repeated_entry(expected_entries_view, entries)) << fmt::format("#{}", i);
  }
}

// TestFollowerCheckMsgApp tests that if the follower does not find an
// entry in its log with the same index and term as the one in AppendEntries RPC,
// then it refuses the new entries. Otherwise it replies that it accepts the
// append entries.
// Reference: section 5.3
TEST_F(raft_paper_test_suit, test_follower_check_msg_app) {
  auto ents = create_entries(1, {1, 2});
  struct test_case {
    uint64_t term;         // 请求中的任期号
    uint64_t index;        // 请求中的日志索引
    uint64_t windex;       // 期望返回的索引
    bool wreject;          // 期望是否被拒绝
    uint64_t wrejectHint;  // 期望的拒绝提示索引
    uint64_t wlogterm;     // 期望的日志任期
  };

  std::vector<test_case> tests = {
      // 匹配已提交条目
      {0, 0, 1, false, 0, 0},                             // 空匹配请求
      {ents[0].term(), ents[0].index(), 1, false, 0, 0},  // 精确匹配条目1
      {ents[1].term(), ents[1].index(), 2, false, 0, 0},  // 精确匹配条目2

      // 与现有条目不匹配
      {ents[0].term(), ents[1].index(),  // 任期匹配但索引不匹配
       ents[1].index(), true, 1, 1},     // 期望拒绝提示索引1

      // 不存在的条目
      {ents[1].term() + 1, ents[1].index() + 1,  // 更高任期和索引
       ents[1].index() + 1, true, 2, 2}          // 期望拒绝提示索引2
  };

  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto& tt = tests[i];

    auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1, 2, 3})});
    auto& mm_storage = *mm_storage_ptr;
    mm_storage.append(create_entries(1, {1, 2}));
    pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
    auto r = new_test_raft(1, 10, 1, std::move(storage_proxy));
    raftpb::hard_state hs;
    hs.set_commit(1);
    r.load_state(hs);
    r.become_follower(2, 2);

    r.step(convert_test_pb_message({
        .msg_type = raftpb::message_type::MSG_APP,
        .from = 2,
        .to = 1,
        .term = 2,
        .log_term = tt.term,
        .index = tt.index,
    }));

    auto expected_msg = convert_test_pb_message({
        .msg_type = raftpb::message_type::MSG_APP_RESP,
        .from = 1,
        .to = 2,
        .term = 2,
        .log_term = tt.wlogterm,
        .index = tt.windex,
    });
    if (tt.wreject) {
      expected_msg.set_reject(tt.wreject);
    }
    if (tt.wrejectHint != 0) {
      expected_msg.set_reject_hint(tt.wrejectHint);
    }
    auto msgs = r.read_messages();
    ASSERT_EQ(expected_msg.DebugString(), msgs[0].DebugString()) << fmt::format("#{}", i);
  }
}

// TestFollowerAppendEntries tests that when AppendEntries RPC is valid,
// the follower will delete the existing conflict entry and all that follow it,
// and append any new entries not already in the log.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
TEST_F(raft_paper_test_suit, test_follower_append_entries) {
  struct test_case {
    std::uint64_t index;
    std::uint64_t term;
    lepton::pb::repeated_entry ents;
    lepton::pb::repeated_entry wents;
    lepton::pb::repeated_entry wunstable;
  };
  std::vector<test_case> tests = {
      // 用例 1：在索引2（任期2）后追加新条目
      {
          .index = 2,
          .term = 2,
          .ents = create_entries(3, {3}),         // 新条目: index=3, term=3
          .wents = create_entries(1, {1, 2, 3}),  // 最终日志: [term1@1, term2@2, term3@3]
          .wunstable = create_entries(3, {3})     // 未持久化: [term3@3]
      },

      // 用例 2：覆盖部分日志（任期冲突）
      {
          .index = 1,
          .term = 1,
          .ents = create_entries(2, {3, 4}),      // 新条目: [term3@2, term4@3]
          .wents = create_entries(1, {1, 3, 4}),  // 最终日志: [term1@1, term3@2, term4@3]
          .wunstable = create_entries(2, {3, 4})  // 未持久化: [term3@2, term4@3]
      },

      // 用例 3：从零开始追加
      {
          .index = 0,
          .term = 0,
          .ents = create_entries(1, {1}),  // 新条目: [term1@1]
          .wents = create_entries(1, {1, 2}),  // 最终日志: [term1@1, term2@2]（注意：包含隐含添加的下一条）
          .wunstable = {}                      // 全部已持久化（空）
      },

      // 用例 4：覆盖全部日志
      {
          .index = 0,
          .term = 0,
          .ents = create_entries(1, {3}),      // 新条目: [term3@1]
          .wents = create_entries(1, {3}),     // 最终日志: [term3@1]
          .wunstable = create_entries(1, {3})  // 未持久化: [term3@1]
      },
  };
  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto& tt = tests[i];

    auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1, 2, 3})});
    auto& mm_storage = *mm_storage_ptr;
    mm_storage.append(create_entries(1, {1, 2}));
    pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
    auto r = new_test_raft(1, 10, 1, std::move(storage_proxy));
    r.become_follower(2, 2);

    auto msg = convert_test_pb_message({
        .msg_type = raftpb::message_type::MSG_APP,
        .from = 2,
        .to = 1,
        .term = 2,
        .log_term = tt.term,
        .index = tt.index,
    });
    for (auto& entry : tt.ents) {
      msg.add_entries()->CopyFrom(entry);
    }
    r.step(std::move(msg));

    ASSERT_TRUE(compare_repeated_entry(tt.wents, r.raft_log_handle_.all_entries()));
    ASSERT_TRUE(compare_repeated_entry(tt.wunstable, r.raft_log_handle_.next_unstable_ents()));
  }
}

// TestLeaderSyncFollowerLog tests that the leader could bring a follower's log
// into consistency with its own.
// Reference: section 5.3, figure 7
/*
"The leader maintains ​​nextIndex​​ for each follower, which is the index of the next log entry the leader will
send to that follower. When a leader first comes to power, it initializes all nextIndex values to the index just after
the last one in its log. If a follower’s log is inconsistent with the leader’s, the ​​AppendEntries consistency
check will fail​​ in the next RPC. After a rejection, the leader ​​decrements nextIndex​​ and retries the
AppendEntries RPC. Eventually, nextIndex will reach a point where the leader and follower logs match. When this happens,
AppendEntries will succeed, which ​​removes any conflicting entries​​ in the follower’s log and ​​appends
entries from the leader’s log​​."

验证 ​​Raft 协议的日志同步机制​​：
- ​​领导者强制同步​​：领导者能够覆盖不匹配的跟随者日志
- ​​日志一致性收敛​​：最终所有节点的日志完全一致
- ​​多种冲突场景​​：处理不同形式的日志不一致情况
*/
TEST_F(raft_paper_test_suit, test_leader_sync_follower_log) {
  const auto ents = create_entries(0, {0, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6});
  constexpr std::uint64_t term = 8;
  std::vector<lepton::pb::repeated_entry> tests = {
      // 场景 1: follower 日志条目比 leader 少, 几乎相同（少最后一项）
      // 预期 1：follower 与 leader 日志条目完全一致
      create_entries(0, {0, 1, 1, 1, 4, 4, 5, 5, 6, 6}),

      // 场景 2: follower 日志条目比 leader 少（少很多）
      // 预期 2：follower 与 leader 日志条目完全一致
      create_entries(0, {0, 1, 1, 1, 4, 4}),

      // 场景 3：follower 日志条目比 leader，包含重复条目
      // 预期 3：follower 与 leader 日志条目完全一致，删除 follower 多余日志
      create_entries(0, {0, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6}),

      // 场景 4：follower 包含任期跳跃
      // 预期 4：覆盖 follower 高任期日志
      create_entries(0, {0, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7}),

      // 场景 5：follower 存在重复条目
      // 预期 5：follower 清理重复日志
      create_entries(0, {0, 1, 1, 1, 4, 4, 4, 4}),

      // 场景 6：follower 日志条目更多，但是 term 较低
      // 预期 6：覆盖 follower 高任期日志
      create_entries(0, {0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3}),
  };

  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto& tt = tests[i];

    auto leader_mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1, 2, 3})});
    auto& leader_mm_storage = *leader_mm_storage_ptr;
    leader_mm_storage.append(create_entries(0, {0, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6}));
    pro::proxy<storage_builer> leader_storage_proxy = leader_mm_storage_ptr.get();
    auto lead = new_test_raft(1, 10, 1, std::move(leader_storage_proxy));
    raftpb::hard_state lead_state;
    lead_state.set_commit(lead.raft_log_handle_.last_index());
    lead_state.set_term(term);
    lead.load_state(lead_state);

    auto follower_mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1, 2, 3})});
    auto& follower_mm_storage = *follower_mm_storage_ptr;
    follower_mm_storage.append(std::move(tt));
    pro::proxy<storage_builer> follower_storage_proxy = follower_mm_storage_ptr.get();
    auto follower = new_test_raft(2, 10, 1, std::move(follower_storage_proxy));
    raftpb::hard_state follower_state;
    follower_state.set_term(term - 1);
    follower.load_state(follower_state);

    // It is necessary to have a three-node cluster.
    // The second may have more up-to-date log than the first one, so the
    // first node needs the vote from the third node to become the leader.
    std::vector<state_machine_builer_pair> peers;
    peers.emplace_back(state_machine_builer_pair{lead});
    peers.emplace_back(state_machine_builer_pair{follower});
    emplace_nop_stepper(peers);
    auto nt = new_network(std::move(peers));
    nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});

    // The election occurs in the term after the one we loaded with
    // lead.loadState above.
    nt.send({new_pb_message(3, 1, term + 1, raftpb::message_type::MSG_VOTE_RESP)});

    // 强制触发领导者日志复制
    // 确保所有未提交日志被同步
    // 测试跨任期的日志合并
    nt.send(
        {convert_test_pb_message({.msg_type = raftpb::message_type::MSG_PROP, .from = 1, .to = 1, .entries = {{}}})});
    ASSERT_TRUE(diffu(ltoa(lead.raft_log_handle_), ltoa(follower.raft_log_handle_)).empty());
  }
}

// TestVoteRequest tests that the vote request includes information about the candidate’s log
// and are sent to all of the other nodes.
// Reference: section 5.4.1
TEST_F(raft_paper_test_suit, test_vote_request) {
  struct test_case {
    lepton::pb::repeated_entry ents;
    std::uint64_t wterm;
  };
  std::vector<test_case> tests = {
      {.ents = create_entries(1, {1}), .wterm = 2},
      {.ents = create_entries(1, {1, 2}), .wterm = 3},
  };

  for (std::size_t j = 0; j < tests.size(); ++j) {
    auto& tt = tests[j];

    auto r =
        new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
    auto msg = convert_test_pb_message({
        .msg_type = raftpb::message_type::MSG_APP,
        .from = 2,
        .to = 1,
        .term = tt.wterm - 1,
        .log_term = 0,
        .index = 0,
    });
    for (auto& entry : tt.ents) {
      msg.add_entries()->CopyFrom(entry);
    }
    r.step(std::move(msg));
    r.read_messages();

    for (auto i = 1; i < r.election_timeout_ * 2; ++i) {
      r.tick_election();
    }

    auto msgs = r.read_messages();
    std::sort(msgs.begin(), msgs.end(), [](const raftpb::message& lhs, const raftpb::message& rhs) {
      return lhs.DebugString() < rhs.DebugString();
    });
    ASSERT_EQ(2, msgs.size()) << fmt::format("#{}: expected 2 messages, got {}", j, msgs.size());
    for (auto i = 0; i < msgs.size(); ++i) {
      auto& msg = msgs[i];
      ASSERT_EQ(i + 2, msg.to());
      ASSERT_EQ(magic_enum::enum_name(raftpb::message_type::MSG_VOTE), magic_enum::enum_name(msg.type()));
      ASSERT_EQ(tt.wterm, msg.term());

      ASSERT_EQ(tt.ents[tt.ents.size() - 1].index(), msg.index());
      ASSERT_EQ(tt.ents[tt.ents.size() - 1].term(), msg.log_term());
    }
  }
}

// TestVoter tests the voter denies its vote if its own log is more up-to-date
// than that of the candidate.
// Reference: section 5.4.1
TEST_F(raft_paper_test_suit, test_voter) {
  struct test_case {
    lepton::pb::repeated_entry ents;  // 投票方日志
    uint64_t logterm;                 // 候选人的日志任期
    uint64_t index;                   // 候选人的最后日志索引
    bool wreject;                     // 预期是否拒绝
  };

  // 创建测试用例数组
  std::vector<test_case> tests = {
      // 相同日志任期 - 应接受
      {create_entries(1, {1}), 1, 1, false},  // 完全匹配
      {create_entries(1, {1}), 1, 2, false},  // 候选人日志更完整

      // 相同日志任期 - 应拒绝
      {create_entries(1, {1, 1}), 1, 1, true},  // 投票方日志更完整

      // 候选人日志任期更高 - 应接受
      {create_entries(1, {1}), 2, 1, false},  // 候选人任期更高
      {create_entries(1, {1}), 2, 2, false},  // 候选人任期更高且索引更高

      // 混合任期 - 特殊情况
      {
          create_entries({{1, 1}, {2, 1}}),  // ents
          2,                                 // logterm
          1,                                 // index
          false                              // 不拒绝（候选人任期高）
      },

      // 投票方日志任期更高 - 应拒绝
      {create_entries(1, {2}), 1, 1, true},     // 投票方任期更高
      {create_entries(1, {2}), 1, 2, true},     // 投票方任期更高
      {create_entries(1, {2, 2}), 1, 1, true},  // 投票方有更多任期高的条目
      {create_entries(1, {1, 1}), 1, 1, true}   // 投票方日志更完整
  };

  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto& tt = tests[i];

    auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1, 2})});
    auto& mm_storage = *mm_storage_ptr;
    mm_storage.append(std::move(tt.ents));
    pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
    auto r = new_test_raft(1, 10, 1, std::move(storage_proxy));

    r.step(convert_test_pb_message({
        .msg_type = raftpb::message_type::MSG_VOTE,
        .from = 2,
        .to = 1,
        .term = 3,
        .log_term = tt.logterm,
        .index = tt.index,
    }));

    auto msgs = r.read_messages();
    ASSERT_EQ(1, msgs.size());
    ASSERT_EQ(raftpb::message_type::MSG_VOTE_RESP, msgs[0].type());
    ASSERT_EQ(tt.wreject, msgs[0].reject()) << fmt::format("#{}", i);
  }
}

// TestLeaderOnlyCommitsLogFromCurrentTerm tests that only log entries from the leader’s
// current term are committed by counting replicas.
// Reference: section 5.4.2
TEST_F(raft_paper_test_suit, test_leader_only_commits_log_from_current_term) {
  struct test_case {
    uint64_t index;    // 提议的日志索引
    uint64_t wcommit;  // 预期的提交索引
  };

  std::vector<test_case> tests = {
      // 不提交前任期的日志条目
      {1, 0},  // 索引1不应提交
      {2, 0},  // 索引2不应提交

      // 提交当前任期的日志
      {3, 3}  // 索引3应提交
  };

  for (std::size_t i = 0; i < tests.size(); ++i) {
    auto& tt = tests[i];

    auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1, 2})});
    auto& mm_storage = *mm_storage_ptr;
    mm_storage.append(create_entries(1, {1, 2}));
    pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
    auto r = new_test_raft(1, 10, 1, std::move(storage_proxy));
    raftpb::hard_state hs;
    hs.set_term(2);
    r.load_state(hs);
    // become leader at term 3
    r.become_candidate();
    r.become_leader();
    r.read_messages();
    // propose a entry to current term
    r.step(convert_test_pb_message({.msg_type = raftpb::message_type::MSG_PROP, .from = 1, .to = 1, .entries = {{}}}));

    r.step(convert_test_pb_message(
        {.msg_type = raftpb::message_type::MSG_APP_RESP, .from = 2, .to = 1, .term = r.term_, .index = tt.index}));
    r.advance_messages_after_append();
    ASSERT_EQ(tt.wcommit, r.raft_log_handle_.committed()) << fmt::format("#{}", i);
  }
}