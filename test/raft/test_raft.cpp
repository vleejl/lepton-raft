#include <gtest/gtest.h>
#include <proxy.h>
#include <raft.pb.h>

#include <memory>
#include <vector>

#include "config.h"
#include "error.h"
#include "memory_storage.h"
#include "protobuf.h"
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
    panic("entry unexpectedly dropped");
  }
}

using test_memory_storage_options = std::function<void(lepton::memory_storage &)>;

static test_memory_storage_options with_peers(lepton::pb::repeated_peers &&peers) {
  return [&](lepton::memory_storage &ms) -> void {
    ms.snapshot_ref().mutable_metadata()->mutable_conf_state()->mutable_voters()->Swap(&peers);
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
  return lepton::config{id, election_tick, heartbeat_tick, storage};
}

static lepton::raft new_test_raft(std::uint64_t id, int election_tick, int heartbeat_tick,
                                  pro::proxy_view<storage_builer> storage) {
  auto r = new_raft(new_test_config(id, election_tick, heartbeat_tick, storage));
  assert(!r);
  return std::move(r.value());
}

TEST_F(raft_test_suit, progress_leader) {
  auto ms = new_memory_storage({with_peers({1, 2})});
  pro::proxy_view<storage_builer> storage{ms.get()};
  auto r = new_test_raft(1, 10, 1, storage);
}