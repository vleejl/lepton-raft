#include <gtest/gtest.h>
#include <proxy.h>
#include <raft.pb.h>

#include "error.h"
#include "memory_storage.h"
#include "protobuf.h"
#include "raft.h"
#include "types.h"
using namespace lepton;

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

void must_append_entry(raft &r, lepton::pb::repeated_entry &&ents) {
  if (!r.append_entry(std::move(ents))) {
    panic("entry unexpectedly dropped");
  }
}