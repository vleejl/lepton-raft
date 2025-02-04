#ifndef _LEPTON_RESTORE_H_
#define _LEPTON_RESTORE_H_

#include "confchange.h"

namespace lepton {
namespace confchange {
// Restore takes a Changer (which must represent an empty configuration), and
// runs a sequence of changes enacting the configuration described in the
// ConfState.
//
// TODO(tbg) it's silly that this takes a Changer. Unravel this by making sure
// the Changer only needs a ProgressMap (not a whole Tracker) at which point
// this can just take LastIndex and MaxInflight directly instead and cook up
// the results from that alone.
changer::result restor(const raftpb::conf_state &cs, changer &&chg);
}  // namespace confchange
}  // namespace lepton

#endif  // _LEPTON_RESTORE_H_
