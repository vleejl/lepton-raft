#ifndef _LEPTON_STATE_TRACE_H_
#define _LEPTON_STATE_TRACE_H_

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"

#include <raft.pb.h>

#include "tracker.h"
#include "types.h"
namespace lepton {

class raft;

void trace_init_state(raft& r);

void trace_ready(raft& r);

void trace_change_conf_event(raftpb::conf_change_v2& cc, raft& r);

void trace_replicate(raft& r, const pb::repeated_entry& entries);

void trace_become_candidate(raft& r);

void trace_become_leader(raft& r);

void trace_conf_change_event(tracker::config& c, raft& r);

void trace_send_message(raft& r, const raftpb::message& m);

void trace_receive_message(raft& r, const raftpb::message& m);

}  // namespace lepton

#pragma GCC diagnostic pop

#endif  // _LEPTON_STATE_TRACE_H_
