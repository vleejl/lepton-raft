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
#include "test_raft_state_machine.h"
#include "test_raft_utils.h"
#include "test_utility_data.h"
#include "types.h"
using namespace lepton;

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
TEST_F(raft_paper_test_suit, test_reject_stale_term_message) {
  auto called = false;
  auto fake_step = [&](lepton::raft &r, raftpb::message &&m) -> lepton::leaf::result<void> {
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