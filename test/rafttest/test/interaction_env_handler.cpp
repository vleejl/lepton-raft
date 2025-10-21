#include <cassert>
#include <cstddef>
#include <vector>

#include "interaction_env.h"
#include "leaf.h"
#include "test_utility_data.h"

namespace interaction {
std::string interaction_env::handle(const datadriven::test_data &test_data) {
  this->output->clear();
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
        } else if (cmd == "stabilize") {
          // Deliver messages to and run process-ready on the set of IDs until
          // no more work is to be done. If no ids are given, all nodes are used.
          //
          // Example:
          //
          // stabilize 1 4
          LEPTON_LEAF_CHECK(handle_stabilize(test_data));
        } else if (cmd == "status") {
          // Print Raft status.
          //
          // Example:
          //
          // status 5
          LEPTON_LEAF_CHECK(handle_status(test_data));
        } else if (cmd == "tick-election") {
          // Tick an election timeout interval for the given node (but beware the
          // randomized timeout).
          //
          // Example:
          //
          // tick-election 3
          LEPTON_LEAF_CHECK(handle_tick_election(test_data));
        } else if (cmd == "tick-heartbeat") {
          // Tick a heartbeat interval.
          //
          // Example:
          //
          // tick-heartbeat 3
          LEPTON_LEAF_CHECK(handle_tick_heartbeat(test_data));
        } else if (cmd == "transfer-leadership") {
          // Transfer the Raft leader.
          //
          // Example:
          //
          // transfer-leadership from=1 to=4
          LEPTON_LEAF_CHECK(handle_transfer_leadership(test_data));
        } else if (cmd == "forget-leader") {
          // Forgets the current leader of the given node.
          //
          // Example:
          //
          // forget-leader 1
          LEPTON_LEAF_CHECK(handle_forget_leader(test_data));
        } else if (cmd == "send-snapshot") {
          // Sends a snapshot to a node. Takes the source and destination node.
          // The message will be queued, but not delivered automatically.
          //
          // Example: send-snapshot 1 3
          LEPTON_LEAF_CHECK(handle_send_snapshot(test_data));
        } else if (cmd == "propose") {
          // Propose an entry.
          //
          // Example:
          //
          // propose 1 foo
          LEPTON_LEAF_CHECK(handle_propose(test_data));
        } else if (cmd == "propose-conf-change") {
          // Propose a configuration change, or transition out of a previously
          // proposed joint configuration change that requested explicit
          // transitions. When adding nodes, this command can be used to
          // logically add nodes to the configuration, but add-nodes is needed
          // to "create" the nodes.
          //
          // propose-conf-change node_id [v1=<bool>] [transition=<string>]
          // command string
          // See ConfChangesFromString for command string format.
          // Arguments are:
          //    node_id - the node proposing the configuration change.
          //    v1 - make one change at a time, false by default.
          //    transition - "auto" (the default), "explicit" or "implicit".
          // Example:
          //
          // propose-conf-change 1 transition=explicit
          // v1 v3 l4 r5
          //
          // Example:
          //
          // propose-conf-change 2 v1=true
          // v5
          LEPTON_LEAF_CHECK(handle_propose_conf_change(test_data));
        } else if (cmd == "report-unreachable") {
          // Calls <1st>.ReportUnreachable(<2nd>).
          //
          // Example:
          // report-unreachable 1 2
          LEPTON_LEAF_CHECK(handle_report_unreachable(test_data));
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
