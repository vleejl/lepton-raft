#ifndef _LEPTON_STATE_TRACE_H_
#define _LEPTON_STATE_TRACE_H_
#include <raft.pb.h>

namespace lepton {

class raft;
void trace_change_conf_event(raftpb::conf_change_v2& cc, raft& r);

}  // namespace lepton

#endif  // _LEPTON_STATE_TRACE_H_
