#include "storage/pb/wal_protobuf.h"

#include <cstdint>

#include "error/storage_error.h"

namespace lepton::storage::pb {

expected<void> validate_rec_crc(const walpb::record& rec, absl::crc32c_t expected_crc) {
  auto crc = static_cast<std::uint32_t>(expected_crc);
  if (rec.has_crc() && rec.crc() == crc) {
    return {};
  }
  return tl::unexpected(storage_error::ERR_CRC_MISMATCH);
}

}  // namespace lepton::storage::pb
