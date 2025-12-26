#pragma once
#ifndef _LEPTON_PROTOBUF_H_
#define _LEPTON_PROTOBUF_H_
#include <absl/crc/crc32c.h>
#include <raft.pb.h>
#include <wal.pb.h>

#include "error/expected.h"
#include "error/leaf.h"
#include "types.h"
namespace lepton::storage::pb {
expected<void> validate_rec_crc(const walpb::record& rec, absl::crc32c_t expected_crc);

// ValidateSnapshotForWrite ensures the Snapshot the newly written snapshot is valid.
//
// There might exist log-entries written by old etcd versions that does not conform
// to the requirements.
expected<void> validate_snapshot_for_write(const snapshot& s);
}  // namespace lepton::storage::pb

#endif  // _LEPTON_PROTOBUF_H_
