#pragma once

#include <cassert>
#include <memory>

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
#include "storage/fileutil/directory.h"
#include "storage/pb/types.h"
#include "storage/wal/decoder.h"
#include "storage/wal/encoder.h"
#include "storage/wal/file_pipeline.h"
namespace lepton::storage::wal {

// SegmentSizeBytes is the preallocated size of each wal segment file.
// The actual size might be larger than this. In general, the default
// value should be used, but this is defined as an exported variable
// so that tests can set a different segment size.
constexpr static std::uint64_t SEGMENT_SIZE_BYTES = 64 * 1000 * 1000;  // 64MB

// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
class wal {
  NOT_COPYABLE(wal)
 public:
  wal(asio::any_io_executor executor, rocksdb::Env *env, std::unique_ptr<encoder> &&encoder,
      const std::string &dir_path, const std::string &metadata, std::shared_ptr<lepton::logger_interface> logger)
      : executor_(executor),
        env_(env),
        dir_(dir_path),
        metadata_(metadata),
        unsafe_no_sync_(false),
        entry_index_(0),
        encoder_(std::move(encoder)),
        logger_(std::move(logger)) {}

  void append_lock_file(fileutil::env_file_handle &&file_handle) { lock_files_.emplace_back(std::move(file_handle)); }

  asio::awaitable<expected<void>> save_crc(std::uint32_t prev_crc);

  asio::awaitable<expected<void>> encode(walpb::record &r) {
    assert(encoder_);
    co_return co_await encoder_->encode(r);
  }

  asio::awaitable<expected<void>> save_snapshot(const pb::snapshot &snapshot);

  asio::awaitable<expected<void>> sync();

  leaf::result<void> rename_wal(const std::string &tmp_dir_path);

  const std::string &dir() const { return dir_; }

  void cleanup_wal();

  // Close closes the current WAL file and directory.
  asio::awaitable<expected<void>> close();

  fileutil::env_file_endpoint *tail() { return lock_files_.empty() ? nullptr : lock_files_.back().get(); }

 private:
  asio::any_io_executor executor_;

  rocksdb::Env *env_;

  // the living directory of the underlay files
  std::string dir_;

  // dirFile is a fd for the wal directory for syncing on Rename
  fileutil::directory dir_file_;

  // metadata recorded at the head of each WAL
  std::string metadata_;
  // hardstate recorded at the head of WAL
  raftpb::hard_state state_;
  // snapshot to start reading
  pb::snapshot start_;
  // decoder to Decode records
  std::unique_ptr<decoder> decoder_;
  // closer for Decode reader
  std::function<leaf::result<void>()> read_close_;
  // if set, do not fsync
  bool unsafe_no_sync_;

  std::mutex mutex_;
  // index of the last entry saved to the wal
  std::uint64_t entry_index_;
  // encoder to encode records
  std::unique_ptr<encoder> encoder_;
  // the locked files the WAL holds (the name is increasing)
  std::vector<fileutil::env_file_handle> lock_files_;
  file_pipeline_handle file_pipeline_;

  std::shared_ptr<lepton::logger_interface> logger_;
};

using wal_handle = std::unique_ptr<wal>;

asio::awaitable<expected<wal_handle>> create_wal(rocksdb::Env *env, asio::any_io_executor executor,
                                                 const std::string &dirpath, const std::string &metadata,
                                                 std::shared_ptr<lepton::logger_interface> logger);
}  // namespace lepton::storage::wal

#endif  // _LEPTON_WAL_H_
