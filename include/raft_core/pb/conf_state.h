#pragma once
#ifndef _LEPTON_CONF_STATE_H_
#define _LEPTON_CONF_STATE_H_
#include <raft.pb.h>

#include "error/lepton_error.h"

namespace lepton::core {

namespace pb {
// Equivalent returns a nil error if the inputs describe the same configuration.
// On mismatch, returns a descriptive error showing the differences.
leaf::result<void> conf_state_equivalent(const raftpb::ConfState &lhs, const raftpb::ConfState &rhs);
}  // namespace pb
}  // namespace lepton::core

#endif  // _LEPTON_CONF_STATE_H_
