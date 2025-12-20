#pragma once
#ifndef _LEPTON_STATUS_H_
#define _LEPTON_STATUS_H_
#include <raft.pb.h>

#include <cstdint>
#include <memory>

#include "coroutine/channel_endpoint.h"
#include "raft_core/tracker/tracker.h"
namespace lepton::core {

enum class state_type : std::uint64_t { FOLLOWER, CANDIDATE, LEADER, PRE_CANDIDATE };

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
struct soft_state {
  // must use atomic operations to access; keep 64-bit aligned.
  std::uint64_t leader_id;
  state_type raft_state;

  auto operator<=>(const soft_state &) const = default;
};

// BasicStatus contains basic information about the Raft peer. It does not
// allocate.
struct basic_status {
  std::uint64_t id;

  raftpb::hard_state hard_state;
  lepton::core::soft_state soft_state;

  std::uint64_t applied;

  std::uint64_t lead_transferee;
};

// Status contains information about this Raft peer and its view of the system.
// The Progress is only populated on the leader.
struct status {
  lepton::core::basic_status basic_status;
  tracker::config config;
  tracker::progress_map progress;
};

using status_channel = coro::channel_endpoint<std::weak_ptr<coro::channel_endpoint<status>>>;

}  // namespace lepton::core

#endif  // _LEPTON_STATUS_H_
