#ifndef _LEPTON_CONF_CHANGE_
#define _LEPTON_CONF_CHANGE_
#include "error.h"
#include "proxy.h"
#include "raft.pb.h"
namespace lepton {
using conf_change_v2 = raftpb::conf_change_v2;
using conf_change = raftpb::conf_change;

PRO_DEF_MEM_DISPATCH(conf_change_as_v2, as_v2);
PRO_DEF_MEM_DISPATCH(conf_change_as_v1, as_v1);
// clang-format off
struct conf_change_builder : pro::facade_builder 
  ::add_convention<conf_change_as_v2, leaf::result<conf_change_v2*>()> 
  ::add_convention<conf_change_as_v1, leaf::result<conf_change*>()> 
  ::build{};
// clang-format on

}  // namespace lepton

#endif  // _LEPTON_CONF_CHANGE_