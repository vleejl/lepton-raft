#pragma once
#ifndef _LEPTON_CONF_CHANGE_
#define _LEPTON_CONF_CHANGE_
#include <raft.pb.h>

#include <tuple>

#include "error/error.h"
#include "error/leaf.h"
#include "raft_core/pb/types.h"
namespace lepton::core {
namespace pb {

// replace etcd raft pb.ConfChangeI
using conf_change_var = std::variant<std::monostate, raftpb::ConfChange, raftpb::ConfChangeV2>;
leaf::result<raftpb::ConfChange> conf_change_var_as_v1(conf_change_var&& cc);
raftpb::ConfChangeV2 conf_change_var_as_v2(conf_change_var&& cc);

leaf::result<std::tuple<raftpb::EntryType, std::string>> serialize_conf_change(const conf_change_var& cc);
leaf::result<raftpb::Message> conf_change_to_message(const conf_change_var& cc);

// std::tuple<raftpb::ConfChange, bool> conf_change_as_v1(raftpb::ConfChange&& cc);
raftpb::ConfChangeV2 conf_change_as_v2(raftpb::ConfChange&& cc);

// std::tuple<raftpb::ConfChange, bool> conf_change_as_v1(raftpb::ConfChangeV2&& _);
// raftpb::ConfChangeV2 conf_change_as_v2(raftpb::ConfChangeV2&& cc);
// EnterJoint returns two bools. The second bool is true if and only if this
// config change will use Joint Consensus, which is the case if it contains more
// than one change or if the use of Joint Consensus was requested explicitly.
// The first bool can only be true if second one is, and indicates whether the
// Joint State will be left automatically.
std::tuple<bool, bool> enter_joint(const raftpb::ConfChangeV2& c);
// LeaveJoint is true if the configuration change leaves a joint configuration.
// This is the case if the ConfChangeV2 is zero, with the possible exception of
// the Context field.
bool leave_joint(raftpb::ConfChangeV2& c);

// ConfChangesToString is the inverse to ConfChangesFromString.
std::string conf_changes_to_string(const repeated_conf_change& ccs);

// ConfChangesFromString parses a Space-delimited sequence of operations into a
// slice of ConfChangeSingle. The supported operations are:
// - vn: make n a voter,
// - ln: make n a learner,
// - rn: remove n, and
// - un: update n.
leaf::result<repeated_conf_change> conf_changes_from_string(std::string_view s);

std::string describe_conf_change_v2(const raftpb::ConfChangeV2& cc);

}  // namespace pb
}  // namespace lepton::core

#endif  // _LEPTON_CONF_CHANGE_