#ifndef _LEPTON_CONF_STATE_H_
#define _LEPTON_CONF_STATE_H_
#include <raft.pb.h>

#include "error.h"

namespace lepton {

namespace pb {
// Equivalent returns a nil error if the inputs describe the same configuration.
// On mismatch, returns a descriptive error showing the differences.
leaf::result<void> conf_state_equivalent(const raftpb::conf_state &lhs, const raftpb::conf_state &rhs);
}  // namespace pb
}  // namespace lepton

#endif  // _LEPTON_CONF_STATE_H_
