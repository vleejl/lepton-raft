#ifndef _LEPTON_CONF_CHANGE_
#define _LEPTON_CONF_CHANGE_
#include <raft.pb.h>
namespace lepton {
namespace pb {
raftpb::conf_change_v2 convert_conf_change_v2(raftpb::conf_change&& cc);
}  // namespace pb
}  // namespace lepton

#endif  // _LEPTON_CONF_CHANGE_