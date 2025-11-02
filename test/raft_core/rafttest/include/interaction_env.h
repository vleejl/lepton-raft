#ifndef _LEPTON_INTERACTION_ENV_H_
#define _LEPTON_INTERACTION_ENV_H_
#include <cstddef>
#include <functional>
#include <memory>
#include <vector>

#include "config.h"
#include "data_driven.h"
#include "interaction_env_logger.h"
#include "leaf.h"
#include "raft.pb.h"
#include "raw_node.h"
#include "storage.h"
#include "types.h"

namespace interaction {

PRO_DEF_MEM_DISPATCH(storage_set_hard_state, set_hard_state);

PRO_DEF_MEM_DISPATCH(storage_apply_snapshot, apply_snapshot);

PRO_DEF_MEM_DISPATCH(storage_compact, compact);

PRO_DEF_MEM_DISPATCH(storage_append, append);

// Storage is the interface used by InteractionEnv. It is comprised of raft's
// Storage interface plus access to operations that maintain the log and drive
// the Ready handling loop.
// clang-format off
struct storage_builer : pro::facade_builder 
  ::add_convention<lepton::storage_initial_state, lepton::leaf::result<std::tuple<raftpb::hard_state, raftpb::conf_state>>() const> 
  ::add_convention<lepton::storage_entries, lepton::leaf::result<lepton::pb::repeated_entry>(std::uint64_t lo, std::uint64_t hi, std::uint64_t max_size) const> 
  ::add_convention<lepton::storage_entries_view, lepton::leaf::result<lepton::pb::span_entry>(std::uint64_t lo, std::uint64_t hi, std::uint64_t max_size) const> 
  ::add_convention<lepton::storage_term, lepton::leaf::result<std::uint64_t>(std::uint64_t i) const> 
  ::add_convention<lepton::storage_last_index, lepton::leaf::result<std::uint64_t>() const> 
  ::add_convention<lepton::storage_first_index, lepton::leaf::result<std::uint64_t>() const> 
  ::add_convention<lepton::storage_snapshot, lepton::leaf::result<raftpb::snapshot>() const>
  ::add_convention<storage_set_hard_state, lepton::leaf::result<void>(raftpb::hard_state&& hard_state)>
  ::add_convention<storage_apply_snapshot, lepton::leaf::result<void>(raftpb::snapshot &&snapshot)>
  ::add_convention<storage_compact, lepton::leaf::result<void>(std::uint64_t compact_index)>
  ::add_convention<storage_append, lepton::leaf::result<void>(lepton::pb::repeated_entry&& entries)>
  ::add_skill<pro::skills::as_view>
  ::build{};
// clang-format on

struct snap_override_storage {
  pro::proxy<storage_builer> storage;
  std::function<lepton::leaf::result<raftpb::snapshot>()> snap_override_func;

  lepton::leaf::result<std::tuple<raftpb::hard_state, raftpb::conf_state>> initial_state() const {
    return storage->initial_state();
  }

  lepton::leaf::result<lepton::pb::repeated_entry> entries(std::uint64_t lo, std::uint64_t hi,
                                                           std::uint64_t max_size) const {
    return storage->entries(lo, hi, max_size);
  }

  lepton::leaf::result<lepton::pb::span_entry> entries_view(std::uint64_t lo, std::uint64_t hi,
                                                            std::uint64_t max_size) const {
    return storage->entries_view(lo, hi, max_size);
  }

  lepton::leaf::result<std::uint64_t> term(std::uint64_t i) const { return storage->term(i); }

  lepton::leaf::result<std::uint64_t> last_index() const { return storage->last_index(); }

  lepton::leaf::result<std::uint64_t> first_index() const { return storage->first_index(); }

  lepton::leaf::result<raftpb::snapshot> snapshot() const {
    if (snap_override_func != nullptr) {
      return snap_override_func();
    }
    return storage->snapshot();
  }

  lepton::leaf::result<void> set_hard_state(raftpb::hard_state &&hard_state) {
    LEPTON_LEAF_CHECK(storage->set_hard_state(std::move(hard_state)));
    return {};
  }

  lepton::leaf::result<void> apply_snapshot(raftpb::snapshot &&snapshot) {
    return storage->apply_snapshot(std::move(snapshot));
  }

  lepton::leaf::result<void> compact(std::uint64_t compact_index) { return storage->compact(compact_index); }

  lepton::leaf::result<void> append(lepton::pb::repeated_entry &&entries) {
    return storage->append(std::move(entries));
  }
};

// InteractionOpts groups the options for an InteractionEnv.
struct interaction_opts {
  std::function<void(lepton::config &)> on_confg = nullptr;

  // SetRandomizedElectionTimeout is used to plumb this function down from the
  // raft test package.
  std::function<void(lepton::raw_node &, std::size_t timeout)> set_randomized_election_timeout = nullptr;
};

struct node {
  lepton::raw_node raw_node;
  pro::proxy<storage_builer> storage;

  lepton::config config;
  // []MsgStorageAppend
  lepton::pb::repeated_message append_work;
  // []MsgStorageApply
  lepton::pb::repeated_message apply_work;
  lepton::pb::repeated_snapshot history;
};

struct recipient {
  std::uint64_t id = 0;
  bool drop = false;
};

// InteractionEnv facilitates testing of complex interactions between the
// members of a raft group.
struct interaction_env {
  interaction_env() : output(std::make_shared<redirect_logger>(redirect_logger::debug, &logger_buffer)) {}

  explicit interaction_env(interaction_opts &&opts)
      : options(std::move(opts)), output(std::make_shared<redirect_logger>(redirect_logger::debug, &logger_buffer)) {}

  // Handle is the entrypoint for data-driven interaction testing. Commands and
  // parameters are parsed from the supplied TestData. Errors during data parsing
  // are reported via the supplied *testing.T; errors from the raft nodes and the
  // storage engine are reported to the output buffer.
  std::string handle(const datadriven::test_data &test_data);

  // AddNodes adds n new nodes initialized from the given snapshot (which may be
  // empty), and using the cfg as template. They will be assigned consecutive IDs.
  lepton::leaf::result<void> add_nodes(std::size_t, const lepton::config &config, raftpb::snapshot &snap);
  lepton::leaf::result<void> handle_add_nodes(const datadriven::test_data &test_data);

  lepton::leaf::result<void> handle_campaign(const datadriven::test_data &test_data);
  // Campaign the node at the given index.
  lepton::leaf::result<void> campaign(std::size_t node_idx);

  lepton::leaf::result<void> handle_compact(const datadriven::test_data &test_data);
  // Compact truncates the log on the node at index idx so that the supplied new
  // first index results.
  lepton::leaf::result<void> compact(std::size_t node_idx, std::uint64_t new_first_index);

  lepton::leaf::result<void> handle_deliver_msgs(const datadriven::test_data &test_data);
  // DeliverMsgs goes through env.Messages and, depending on the Drop flag,
  // delivers or drops messages to the specified Recipients. Only messages of type
  // typ are delivered (-1 for all types). Returns the number of messages handled
  // (i.e. delivered or dropped). A handled message is removed from env.Messages.
  int deliver_msgs(int raftpb_message_type, const std::vector<recipient> &rs);

  lepton::leaf::result<void> handle_process_ready(const datadriven::test_data &test_data);
  // ProcessReady runs Ready handling on the node with the given index.
  lepton::leaf::result<void> process_ready(std::size_t node_idx);

  lepton::leaf::result<void> handle_process_append_thread(const datadriven::test_data &test_data);
  // ProcessAppendThread runs processes a single message on the "append" thread of
  // the node with the given index.
  lepton::leaf::result<void> process_append_thread(std::size_t node_idx);

  lepton::leaf::result<void> handle_process_apply_thread(const datadriven::test_data &test_data);
  // ProcessApplyThread runs processes a single message on the "apply" thread of
  // the node with the given index.
  lepton::leaf::result<void> process_apply_thread(std::size_t node_idx);

  lepton::leaf::result<void> handle_log_level(const datadriven::test_data &test_data);

  lepton::leaf::result<void> log_level(const std::string &name);

  lepton::leaf::result<void> handle_raft_log(const datadriven::test_data &test_data);
  // RaftLog pretty prints the raft log to the output buffer.
  lepton::leaf::result<void> raft_log(std::size_t node_idx);

  // handleRaftState pretty-prints the raft state for all nodes to the output buffer.
  // For each node, the information is based on its own configuration view.
  lepton::leaf::result<void> handle_raft_state();

  lepton::leaf::result<void> handle_set_randomized_election_timeout(const datadriven::test_data &test_data);

  lepton::leaf::result<void> handle_stabilize(const datadriven::test_data &test_data);
  // Stabilize repeatedly runs Ready handling on and message delivery to the set
  // of nodes specified via the idxs slice until reaching a fixed point.
  lepton::leaf::result<void> stabilize(const std::vector<std::size_t> &idxs);

  lepton::leaf::result<void> handle_status(const datadriven::test_data &test_data);
  // Status pretty-prints the raft status for the node at the given index to the output
  // buffer.
  lepton::leaf::result<void> status(std::size_t node_idx);

  lepton::leaf::result<void> handle_tick_election(const datadriven::test_data &test_data);

  lepton::leaf::result<void> handle_tick_heartbeat(const datadriven::test_data &test_data);

  // Tick the node at the given index the given number of times.
  lepton::leaf::result<void> tick(std::size_t node_idx, int count);

  lepton::leaf::result<void> handle_transfer_leadership(const datadriven::test_data &test_data);

  // Initiate leadership transfer.
  lepton::leaf::result<void> transfer_leadership(std::size_t from_idx, std::size_t to_idx);

  lepton::leaf::result<void> handle_forget_leader(const datadriven::test_data &test_data);
  // ForgetLeader makes the follower at the given index forget its leader.
  lepton::leaf::result<void> forget_leader(std::size_t node_idx);

  lepton::leaf::result<void> handle_send_snapshot(const datadriven::test_data &test_data);

  // SendSnapshot sends a snapshot.
  lepton::leaf::result<void> send_snapshot(std::size_t from_idx, std::size_t to_idx);

  lepton::leaf::result<void> handle_propose(const datadriven::test_data &test_data);

  // Propose a regular entry.
  lepton::leaf::result<void> propose(std::size_t node_idx, const std::string &data);

  lepton::leaf::result<void> handle_propose_conf_change(const datadriven::test_data &test_data);

  // ProposeConfChange proposes a configuration change on the node with the given index.
  lepton::leaf::result<void> propose_conf_change(std::size_t node_idx, const lepton::pb::conf_change_var &cc);

  lepton::leaf::result<void> handle_report_unreachable(const datadriven::test_data &test_data);

  void with_indent(std::function<void()> f) {
    // 保存原始 builder
    std::ostringstream *orig = output->get_builder();

    // 临时 builder
    std::ostringstream temp;
    output->set_builder(&temp);

    // 执行回调，日志写入 temp
    f();

    // 把 temp 逐行缩进写回 orig
    std::istringstream iss(temp.str());
    std::string line;
    while (std::getline(iss, line)) {
      (*orig) << "  " << line << "\n";
    }

    // 恢复 builder
    output->set_builder(orig);
  }

  interaction_opts options;
  std::vector<std::unique_ptr<snap_override_storage>> storage_handles;
  std::vector<node> nodes;
  lepton::pb::repeated_message messages;  // in-flight messages

  std::ostringstream logger_buffer;
  std::shared_ptr<redirect_logger> output;
};

// raftConfigStub sets up a raft.Config stub with reasonable testing defaults.
// In particular, no limits are set. It is not a complete config: ID and Storage
// must be set for each node using the stub as a template.
inline lepton::config raft_config_stub() {
  lepton::config config;
  config.election_tick = 3;
  config.heartbeat_tick = 1;
  config.max_size_per_msg = lepton::NO_LIMIT;
  config.max_inflight_msgs = std::numeric_limits<std::size_t>::max();
  return config;
}

int first_as_int(const datadriven::test_data &test_data);

std::size_t first_as_node_idx(const datadriven::test_data &test_data);

std::vector<std::size_t> node_idxs(const datadriven::test_data &test_data);

std::tuple<lepton::pb::repeated_message, lepton::pb::repeated_message> split_msgs(
    const lepton::pb::repeated_message &msgs, std::uint64_t to, int raftpb_message_type, bool drop);

lepton::leaf::result<void> process_append(node &n, raftpb::hard_state &&hard_state, lepton::pb::repeated_entry &&ents,
                                          raftpb::snapshot &&snap);

lepton::leaf::result<void> process_apply(node &n, const lepton::pb::repeated_entry &ents);

}  // namespace interaction

#endif  // _LEPTON_INTERACTION_ENV_H_
