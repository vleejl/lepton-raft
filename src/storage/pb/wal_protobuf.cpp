#include "storage/pb/wal_protobuf.h"

#include "error/logic_error.h"

namespace lepton::storage::pb {

expected<void> validate_snapshot_for_write(const snapshot& s) {
  if ((!s.has_conf_state()) && (s.has_index() && s.index() > 0)) {
    return tl::unexpected(logic_error::INVALID_PARAM);
  }
  return {};
}

}  // namespace lepton::storage::pb
