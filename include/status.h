#ifndef _LEPTON_STATUS_H_
#define _LEPTON_STATUS_H_
#include <raft.pb.h>

#include <cstdint>

#include "tracker.h"
namespace lepton {

enum state_type : std::uint64_t {
  STATE_FOLLOWER,
  STATE_CANDIDATE,
  STATE_LEADER,
  STATE_PRE_CANDIDATE
};

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
struct soft_state {
  // must use atomic operations to access; keep 64-bit aligned.
  std::uint64_t leader_id;
  state_type raft_state;
};

// BasicStatus contains basic information about the Raft peer. It does not
// allocate.
struct basic_status {
  std::uint64_t id;

  raftpb::hard_state hard_state;
  soft_state current_soft_state;

  std::uint64_t applied;

  std::uint64_t lead_transferee;
};

// Status contains information about this Raft peer and its view of the system.
// The Progress is only populated on the leader.
struct raft_status {
  basic_status current_basic_status;
  tracker::config config;
  tracker::progress_tracker progress_tracker;
};

}  // namespace lepton

#endif  // _LEPTON_STATUS_H_
