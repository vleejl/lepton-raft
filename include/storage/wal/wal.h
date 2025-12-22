#pragma once

#ifndef _LEPTON_WAL_H_
#define _LEPTON_WAL_H_
#include <raft.pb.h>

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "basic/logger.h"
#include "error/leaf.h"
#include "storage//fileutil/env_file_endpoint.h"
#include "storage/pb/types.h"
#include "storage/wal/decoder.h"
#include "storage/wal/encoder.h"
#include "storage/wal/file_pipeline.h"
namespace lepton::storage::wal {
// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
class wal {
 private:
  std::shared_ptr<lepton::logger_interface> logger_;

  // the living directory of the underlay files
  std::string dir_;

  // dirFile is a fd for the wal directory for syncing on Rename
  fileutil::env_file_endpoint dir_file_;

  // metadata recorded at the head of each WAL
  std::string metadata_;
  // hardstate recorded at the head of WAL
  raftpb::hard_state state_;
  // snapshot to start reading
  pb::snapshot start_;
  // decoder to Decode records
  decoder decoder_;
  // closer for Decode reader
  std::function<leaf::result<void>()> read_close_;
  // if set, do not fsync
  const bool unsafe_no_sync_;

  std::mutex mutex_;
  // index of the last entry saved to the wal
  std::uint64_t entry_index_;
  // encoder to encode records
  encoder encoder_;
  // the locked files the WAL holds (the name is increasing)
  std::vector<fileutil::env_file_endpoint> lock_files_;
  file_pipeline file_pipeline_;
};
}  // namespace lepton::storage::wal

#endif  // _LEPTON_WAL_H_
