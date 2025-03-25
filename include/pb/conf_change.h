#ifndef _LEPTON_CONF_CHANGE_
#define _LEPTON_CONF_CHANGE_
#include <raft.pb.h>

#include "error.h"
#include "proxy.h"
#include "utility_macros.h"
namespace lepton {
namespace pb {

raftpb::conf_change_v2 convert_conf_change_v2(raftpb::conf_change&& cc);

PRO_DEF_MEM_DISPATCH(conf_change_as_v2, as_v2);
PRO_DEF_MEM_DISPATCH(conf_change_as_v1, as_v1);
// clang-format off
struct conf_change_builder : pro::facade_builder 
  ::add_convention<conf_change_as_v2, raftpb::conf_change_v2()> 
  ::add_convention<conf_change_as_v1, leaf::result<raftpb::conf_change>()> 
  ::build{};
// clang-format on

struct conf_change {
  // AsV2 returns a V2 configuration change carrying out the same operation.
  raftpb::conf_change_v2 as_v2() {
    raftpb::conf_change_v2 obj;
    auto changes = obj.add_changes();
    changes->set_type(data.type());
    changes->set_node_id(data.node_id());
    obj.set_allocated_context(data.release_context());
    return obj;
  }

  // AsV1 returns the ConfChange and true.
  leaf::result<raftpb::conf_change> as_v1() { return data; }
  raftpb::conf_change data;
};
}  // namespace pb
}  // namespace lepton

#endif  // _LEPTON_CONF_CHANGE_