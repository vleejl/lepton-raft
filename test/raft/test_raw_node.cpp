#include <gtest/gtest.h>
#include <proxy.h>
#include <raft.pb.h>

#include <cmath>
#include <cstddef>
#include <cstdio>
#include <string>
#include <unordered_map>
#include <vector>

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
  ready_channel_ptr ready() { return nullptr; }

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