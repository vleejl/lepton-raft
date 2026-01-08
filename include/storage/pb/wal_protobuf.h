#pragma once
#ifndef _LEPTON_PROTOBUF_H_
#define _LEPTON_PROTOBUF_H_
#include <absl/crc/crc32c.h>
#include <raft.pb.h>
#include <wal.pb.h>

#include <cassert>
#include <concepts>
#include <cstdint>

#include "basic/logger.h"
#include "error/expected.h"
#include "error/wal_error.h"
#include "types.h"

namespace lepton::storage::pb {

template <typename T>
concept crc_type = std::convertible_to<T, std::uint32_t> || requires(T t) { static_cast<std::uint32_t>(t); };

/**
 * @brief 校验 Record 的 CRC (C++20 Concept 版本)
 */
template <crc_type T>
expected<void> validate_rec_crc(const walpb::Record& rec, T expected_crc) {
  // 统一转换
  const uint32_t target_crc = static_cast<uint32_t>(expected_crc);
  if (!rec.has_crc()) {
    assert(false);
    LOG_TRACE("record lack crc field");
    return tl::unexpected(wal_error::ERR_CRC_MISMATCH);
  }
  if (rec.crc() != target_crc) {
    LOG_TRACE("crc value mismatch, record crc:{}, expected crc:{}", rec.crc(), target_crc);
    return tl::unexpected(wal_error::ERR_CRC_MISMATCH);
  }

  return {};
}

// ValidateSnapshotForWrite ensures the Snapshot the newly written snapshot is valid.
//
// There might exist log-entries written by old etcd versions that does not conform
// to the requirements.
expected<void> validate_snapshot_for_write(const snapshot& s);
}  // namespace lepton::storage::pb

#endif  // _LEPTON_PROTOBUF_H_
