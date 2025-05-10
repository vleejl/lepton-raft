#ifndef _LEPTON_CONF_CHANGE_
#define _LEPTON_CONF_CHANGE_
#include <raft.pb.h>

#include <tuple>

#include "error.h"
namespace lepton {
namespace pb {

using conf_change_var = std::variant<std::monostate, raftpb::conf_change, raftpb::conf_change_v2>;

leaf::result<std::tuple<raftpb::entry_type, std::string>> serialize_conf_change(const conf_change_var& cc);
leaf::result<raftpb::message> conf_change_to_message(const conf_change_var& cc);

std::tuple<raftpb::conf_change, bool> conf_change_as_v1(raftpb::conf_change&& cc);
raftpb::conf_change_v2 conf_change_as_v2(raftpb::conf_change&& cc);

std::tuple<raftpb::conf_change, bool> conf_change_as_v1(raftpb::conf_change_v2&& _);
raftpb::conf_change_v2 conf_change_as_v2(raftpb::conf_change_v2&& cc);

}  // namespace pb
}  // namespace lepton

#endif  // _LEPTON_CONF_CHANGE_