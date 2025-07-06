#include "state_trace.h"

namespace lepton {

void trace_init_state(raft& r) {}

void trace_ready(raft& r) {}

void trace_change_conf_event(raftpb::conf_change_v2& cc, raft& r) {}

void trace_replicate(raft& r, const pb::repeated_entry& entries) {}

void trace_become_candidate(raft& r) {}

void trace_become_leader(raft& r) {}

void trace_conf_change_event(tracker::config& c, raft& r) {}

void trace_send_message(raft& r, const raftpb::message& m) {}

void trace_receive_message(raft& r, const raftpb::message& m) {}

}  // namespace lepton
