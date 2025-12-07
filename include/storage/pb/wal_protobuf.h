#ifndef _LEPTON_PROTOBUF_H_
#define _LEPTON_PROTOBUF_H_
#include <absl/crc/crc32c.h>
#include <raft.pb.h>
#include <wal.pb.h>

#include "expected.h"
namespace lepton::storage::pb {
using snapshot = raftpb::snapshot_metadata;
expected<void> validate_rec_crc(const walpb::record& rec, absl::crc32c_t expected_crc);
}  // namespace lepton::storage::pb

#endif  // _LEPTON_PROTOBUF_H_
