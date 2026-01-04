#include "raft_core/state_trace.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"

namespace lepton::core {

void trace_init_state(raft& r) {}

void trace_ready(raft& r) {}

void trace_change_conf_event(raftpb::ConfChangeV2& cc, raft& r) {}

void trace_replicate(raft& r, const pb::repeated_entry& entries) {}

void trace_become_follower(raft& r) {}

void trace_become_candidate(raft& r) {}

void trace_become_leader(raft& r) {}

void trace_conf_change_event(tracker::config& c, raft& r) {}

void trace_send_message(raft& r, const raftpb::Message& m) {}

void trace_receive_message(raft& r, const raftpb::Message& m) {}

}  // namespace lepton::core

#pragma GCC diagnostic pop
