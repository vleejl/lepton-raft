#include <cassert>
#include <cstddef>
#include <vector>

#include "interaction_env.h"
#include "leaf.h"
#include "test_utility_data.h"

namespace interaction {
std::string interaction_env::handle(const datadriven::test_data &test_data) {
  auto &cmd = test_data.cmd;
  auto _ = boost::leaf::try_handle_some(
      [&]() -> lepton::leaf::result<std::string> {
        // This is a helper case to attach a debugger to when a problem needs
        // to be investigated in a longer test file. In such a case, add the
        // following stanza immediately before the interesting behavior starts:
        //
        // _breakpoint:
        // ----
        // ok
        //
        // and set a breakpoint on the `case` above.
        if (cmd == "_breakpoint") {
          // used for debug
        } else if (cmd == "add-nodes") {
          // Example:
          //
          // add-nodes <number-of-nodes-to-add> voters=(1 2 3) learners=(4 5) index=2 content=foo
          // async-storage-writes=true
          LEPTON_LEAF_CHECK(handle_add_nodes(test_data));
        } else if (cmd == "campaign") {
          // Example:
          //
          // campaign <node-idx>
          LEPTON_LEAF_CHECK(handle_campaign(test_data));
        } else if (cmd == "compact") {
          // Example:
          //
          // compact <id> <new-first-index>
          LEPTON_LEAF_CHECK(handle_compact(test_data));
        } else if (cmd == "deliver-msgs") {
          // Deliver the messages for a given recipient.
          //
          // Example:
          //
          // deliver-msgs <idx> type=MsgApp drop=(2,3)
          LEPTON_LEAF_CHECK(handle_deliver_msgs(test_data));
        } else if (cmd == "process-ready") {
          // Example:
          //
          // process-ready 3
          LEPTON_LEAF_CHECK(handle_process_ready(test_data));
        } else if (cmd == "process-append-thread") {
          // Example:
          //
          // process-append-thread 3
          LEPTON_LEAF_CHECK(handle_process_append_thread(test_data));
        } else if (cmd == "process-apply-thread") {
          // Example:
          //
          // process-apply-thread 3
          LEPTON_LEAF_CHECK(handle_process_apply_thread(test_data));
        } else if (cmd == "log-level") {
          // Set the log level. NONE disables all output, including from the test
          // harness (except errors).
          //
          // Example:
          //
          // log-level WARN
          LEPTON_LEAF_CHECK(handle_log_level(test_data));
        } else if (cmd == "raft-log") {
          // Print the Raft log.
          //
          // Example:
          //
          // raft-log 3
          LEPTON_LEAF_CHECK(handle_raft_log(test_data));
        } else if (cmd == "raft-state") {
          // Print Raft state of all nodes (whether the node is leading,
          // following, etc.). The information for node n is based on
          // n's view.
          LEPTON_LEAF_CHECK(handle_raft_state());
        } else if (cmd == "set-randomized-election-timeout") {
          // Set the randomized election timeout for the given node. Will be reset
          // again when the node changes state.
          //
          // Example:
          //
          // set-randomized-election-timeout 1 timeout=5
          LEPTON_LEAF_CHECK(handle_set_randomized_election_timeout(test_data));
        } else {
          return fmt::format("unknown command: {}", cmd);
        }
        return {};
      },
      [&](const lepton::lepton_error &e) -> lepton::leaf::result<std::string> {
        if (this->output->quiet()) {
          return e.message;
        }
        this->output->write_string(e.message);
        return {};
      });
  if (this->output->get_builder()->str().empty()) {
    return "ok";
  }
  return this->output->get_builder()->str();
}

int first_as_int(const datadriven::test_data &test_data) {
  auto result = safe_stoull(test_data.cmd_args.front().key_);
  assert(result.has_value());
  return static_cast<int>(*result);
}

std::size_t first_as_node_idx(const datadriven::test_data &test_data) {
  auto n = first_as_int(test_data);
  assert(n > 0);
  return static_cast<std::size_t>(n - 1);
}

std::vector<std::size_t> node_idxs(const datadriven::test_data &test_data) {
  std::vector<std::size_t> ints;
  for (auto &item : test_data.cmd_args) {
    if (!item.vals_.empty()) {
      continue;
    }
    auto result = safe_stoull(item.key_);
    assert(result.has_value());
    ints.push_back(*result - 1);
  }
  return ints;
}
}  // namespace interaction
