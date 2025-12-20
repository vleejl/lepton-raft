#pragma once
#ifndef _LEPTON_STORAGE_ERROR_H_
#define _LEPTON_STORAGE_ERROR_H_
#include <cassert>
#include <string>
#include <system_error>

#include "error/base_error_category.h"

namespace lepton {

enum class storage_error {
  // ErrCompacted is returned by Storage.Entries/Compact when a requested
  // index is unavailable because it predates the last snapshot.
  COMPACTED = 1,
  // ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
  // index is older than the existing snapshot.
  SNAP_OUT_OF_DATE,
  // ErrUnavailable is returned by Storage interface when the requested log
  // entries
  // are unavailable.
  UNAVAILABLE,
  // ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when
  // the required
  // snapshot is temporarily unavailable.
  SNAPSHOT_TEMPORARILY_UNAVAILABLE,
  // ErrCRCMismatch is returned when there is a crc mismatch detected
  ERR_CRC_MISMATCH,
};

// storage_error_category
class storage_error_category : public base_error_category {
 public:
  const char* name() const noexcept override { return "storage_error"; }

  std::string message(int ev) const override {
    switch (static_cast<storage_error>(ev)) {
      case storage_error::COMPACTED:
        return "requested index is unavailable due to compaction";
      case storage_error::SNAP_OUT_OF_DATE:
        return "requested index is older than the existing snapshot";
      case storage_error::UNAVAILABLE:
        return "requested entry at index is unavailable";
      case storage_error::SNAPSHOT_TEMPORARILY_UNAVAILABLE:
        return "snapshot is temporarily unavailable";
      case storage_error::ERR_CRC_MISMATCH:
        return "walpb: crc mismatch";
      default:
        assert(false);
        return "Unrecognized storage error";
    }
  }
};

inline const storage_error_category& get_storage_error_category() {
  static storage_error_category instance;
  return instance;
}

inline std::error_code make_error_code(storage_error e) { return {static_cast<int>(e), get_storage_error_category()}; }

}  // namespace lepton

namespace std {

template <>
struct is_error_code_enum<lepton::storage_error> : true_type {};
}  // namespace std

#endif  // _LEPTON_STORAGE_ERROR_H_
