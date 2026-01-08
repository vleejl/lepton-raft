#pragma once
#ifndef _LEPTON_WAL_ERROR_H_
#define _LEPTON_WAL_ERROR_H_

#include <cassert>
#include <string>
#include <system_error>

#include "error/base_error_category.h"

namespace lepton {

enum class wal_error {
  ERR_METADATA_CONFLICT = 1,
  ERR_CRC_MISMATCH,
  ERR_FILE_NOT_FOUND,
  ERR_SNAPSHOT_MISMATCH,
  ERR_SNAPSHOT_NOT_FOUND,
  ERR_SLICE_OUT_OF_RANGE,
  ERR_DECODER_NOT_FOUND,
  ERR_REC_TYPE_INVALID,
  ERR_NO_MATCHING_SEGMENT,
  ERR_INCONTINUOUS_SEQUENCE,
};

// wal_error_category
class wal_error_category : public base_error_category {
 public:
  const char* name() const noexcept override { return "wal_error"; }

  std::string message(int ev) const override {
    switch (static_cast<wal_error>(ev)) {
      case wal_error::ERR_METADATA_CONFLICT:
        return "wal: conflicting metadata found";
      case wal_error::ERR_CRC_MISMATCH:
        return "wal: crc mismatch";
      case wal_error::ERR_FILE_NOT_FOUND:
        return "wal: file not found";
      case wal_error::ERR_SNAPSHOT_MISMATCH:
        return "wal: snapshot mismatch";
      case wal_error::ERR_SNAPSHOT_NOT_FOUND:
        return "wal: snapshot not found";
      case wal_error::ERR_SLICE_OUT_OF_RANGE:
        return "wal: slice bounds out of range";
      case wal_error::ERR_DECODER_NOT_FOUND:
        return "wal: decoder not found";
      case wal_error::ERR_REC_TYPE_INVALID:
        return "wal: record type invalid";
      case wal_error::ERR_NO_MATCHING_SEGMENT:
        return "wal: no matching segment";
      case wal_error::ERR_INCONTINUOUS_SEQUENCE:
        return "wal: file sequence numbers do not increase continuously";
      default:
        assert(false);
        return "wal: unrecognized wal error";
    }
  }
};

inline const wal_error_category& get_wal_error_category() {
  static wal_error_category instance;
  return instance;
}

inline std::error_code make_error_code(wal_error e) { return {static_cast<int>(e), get_wal_error_category()}; }

}  // namespace lepton

namespace std {

template <>
struct is_error_code_enum<lepton::wal_error> : true_type {};
}  // namespace std

#endif  // _LEPTON_WAL_ERROR_H_
