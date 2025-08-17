#ifndef _LEPTON_INTERACTION_ENV_H_
#define _LEPTON_INTERACTION_ENV_H_
#include <functional>
#include <vector>

#include "config.h"
#include "raw_node.h"
#include "redirect_logger.h"
#include "storage.h"
#include "types.h"

namespace rafttest {

PRO_DEF_MEM_DISPATCH(storage_set_hard_state, set_hard_state);

PRO_DEF_MEM_DISPATCH(storage_apply_snapshot, apply_snapshot);

PRO_DEF_MEM_DISPATCH(storage_compact, compact);

PRO_DEF_MEM_DISPATCH(storage_append, append);

// Storage is the interface used by InteractionEnv. It is comprised of raft's
// Storage interface plus access to operations that maintain the log and drive
// the Ready handling loop.
// clang-format off
struct storage_builer : pro::facade_builder 
  ::add_convention<lepton::storage_initial_state, lepton::leaf::result<std::tuple<raftpb::hard_state, raftpb::conf_state>>()> 
  ::add_convention<lepton::storage_entries, lepton::leaf::result<lepton::pb::repeated_entry>(std::uint64_t lo, std::uint64_t hi, std::uint64_t max_size) const> 
  ::add_convention<lepton::storage_term, lepton::leaf::result<std::uint64_t>(std::uint64_t i) const> 
  ::add_convention<lepton::storage_last_index, lepton::leaf::result<std::uint64_t>() const> 
  ::add_convention<lepton::storage_first_index, lepton::leaf::result<std::uint64_t>() const> 
  ::add_convention<lepton::storage_snapshot, lepton::leaf::result<raftpb::snapshot>() const>
  ::add_convention<storage_set_hard_state, void(raftpb::hard_state&& hard_state)>
  ::add_convention<storage_apply_snapshot, lepton::leaf::result<void>(raftpb::snapshot &&snapshot)>
  ::add_convention<storage_compact, lepton::leaf::result<void>(std::uint64_t compact_index)>
  ::add_convention<storage_append, lepton::leaf::result<void>(lepton::pb::repeated_entry&& entries)>
  ::support_view
  ::build{};
// clang-format on

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

// InteractionEnv facilitates testing of complex interactions between the
// members of a raft group.
struct interaction_env {
  interaction_env() : output(redirect_logger::DEBUG, &logger_buffer) {}

  explicit interaction_env(interaction_opts &&opts)
      : options(std::move(opts)), output(redirect_logger::DEBUG, &logger_buffer) {}

  void with_indent(std::function<void()> f) {
    // 保存原始 builder
    std::ostringstream *orig = output.get_builder();

    // 临时 builder
    std::ostringstream temp;
    output.set_builder(&temp);

    // 执行回调，日志写入 temp
    f();

    // 把 temp 逐行缩进写回 orig
    std::istringstream iss(temp.str());
    std::string line;
    while (std::getline(iss, line)) {
      (*orig) << "  " << line << "\n";
    }

    // 恢复 builder
    output.set_builder(orig);
  }

  interaction_opts options;
  std::vector<node> nodes;
  lepton::pb::repeated_message messages;  // in-flight messages

  std::ostringstream logger_buffer;
  redirect_logger output;
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

}  // namespace rafttest

#endif  // _LEPTON_INTERACTION_ENV_H_
