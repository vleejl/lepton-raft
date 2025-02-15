#include "protobuf.h"

namespace lepton {

namespace pb {
bool is_empty_snap(const raftpb::snapshot& snap) {
  if (!snap.has_metadata()) {
    return false;
  }
  if (!snap.metadata().has_index()) {
    return false;
  }
  return snap.metadata().index() != 0;
}
}  // namespace pb

}  // namespace lepton
