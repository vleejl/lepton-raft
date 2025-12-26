#include "storage/pb/wal_protobuf.h"

#include <cstdint>

#include "error/lepton_error.h"
#include "error/logic_error.h"
#include "error/storage_error.h"

namespace lepton::storage::pb {

expected<void> validate_rec_crc(const walpb::record& rec, absl::crc32c_t expected_crc) {
  auto crc = static_cast<std::uint32_t>(expected_crc);
  if (rec.has_crc() && rec.crc() == crc) {
    return {};
  }
  return tl::unexpected(storage_error::ERR_CRC_MISMATCH);
}

expected<void> validate_snapshot_for_write(const snapshot& s) {
  if ((!s.has_conf_state()) && (s.has_index() && s.index() > 0)) {
    return tl::unexpected(logic_error::INVALID_PARAM);
  }
  return {};
}

}  // namespace lepton::storage::pb
