#pragma once

#ifndef _LEPTON_WAL_H_
#define _LEPTON_WAL_H_
#include <raft.pb.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "basic/logger.h"
#include "error/leaf.h"
#include "raft_core/pb/types.h"
#include "storage//fileutil/locked_file_endpoint.h"
#include "storage/fileutil/directory.h"
#include "storage/fileutil/file_reader.h"
#include "storage/pb/types.h"
#include "storage/wal/decoder.h"
#include "storage/wal/encoder.h"
#include "storage/wal/file_pipeline.h"
namespace lepton::storage::wal {

// SegmentSizeBytes is the preallocated size of each wal segment file.
// The actual size might be larger than this. In general, the default
// value should be used, but this is defined as an exported variable
// so that tests can set a different segment size.
constexpr std::size_t SEGMENT_SIZE_BYTES = 64 * 1024 * 1024;  // 64MB

using wal_segment = std::variant<fileutil::file_reader_handle, fileutil::locked_file_endpoint_handle>;
std::vector<pro::proxy_view<ioutil::reader>> to_reader_views(std::vector<wal_segment> &files);

// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
class wal : public std::enable_shared_from_this<wal> {
  NOT_COPYABLE(wal)
 public:
#ifdef LEPTON_TEST
  wal(asio::any_io_executor executor, rocksdb::Env *env, const std::size_t segment_size_bytes,
      std::shared_ptr<lepton::logger_interface> logger)
      : executor_(executor),
        strand_(executor),
        env_(env),
        segment_size_bytes_(segment_size_bytes),
        logger_(std::move(logger)) {}
#endif
  wal(asio::any_io_executor executor, rocksdb::Env *env, std::unique_ptr<encoder> &&encoder,
      const std::string &dir_path, const std::string &metadata, const std::size_t segment_size_bytes,
      std::shared_ptr<lepton::logger_interface> logger)
      : executor_(executor),
        strand_(executor),
        env_(env),
        dir_(dir_path),
        metadata_(metadata),
        segment_size_bytes_(segment_size_bytes),
        encoder_(std::move(encoder)),
        logger_(std::move(logger)) {}

  wal(rocksdb::Env *env, asio::any_io_executor executor, const std::string &dir_path,
      const std::size_t segment_size_bytes, pb::snapshot &&snap, std::vector<wal_segment> &&segments,
      file_pipeline_handle &&file_pipeline, std::shared_ptr<lepton::logger_interface> logger);

  void append_lock_file(fileutil::locked_file_endpoint_handle &&file_handle) {
    segments_.emplace_back(std::move(file_handle));
  }

  void set_directory(fileutil::directory &&dir) { dir_file_ = std::move(dir); }

  struct read_result {
    std::string metadata;
    raftpb::HardState state;
    core::pb::repeated_entry entries;
    std::error_code ec;
  };

  // ReadAll reads out records of the current WAL.
  // If opened in write mode, it must read out all records until EOF. Or an error
  // will be returned.
  // If opened in read mode, it will try to read all records if possible.
  // If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
  // If loaded snap doesn't match with the expected one, it will return
  // all the records and error ErrSnapshotMismatch.
  // TODO: detect not-last-snap error.
  // TODO: maybe loose the checking of match.
  // After ReadAll, the WAL will be ready for appending new records.
  //
  // ReadAll suppresses WAL entries that got overridden (i.e. a newer entry with the same index
  // exists in the log). Such a situation can happen in cases described in figure 7. of the
  // RAFT paper (http://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14.pdf).
  //
  // ReadAll may return uncommitted yet entries, that are subject to be overridden.
  // Do not apply entries that have index > state.commit, as they are subject to change.
  asio::awaitable<expected<read_result>> read_all();

  asio::awaitable<expected<void>> encode(walpb::Record &r) {
    assert(encoder_);
    co_return co_await encoder_->encode(r);
  }

  asio::awaitable<expected<void>> save_snapshot(const pb::snapshot &snapshot);

  asio::awaitable<expected<void>> save(const core::pb::repeated_entry &entries, raftpb::HardState &&state);

  asio::awaitable<expected<void>> sync();

  // ReleaseLockTo releases the locks, which has smaller index than the given index
  // except the largest one among them.
  // For example, if WAL is holding lock 1,2,3,4,5,6, ReleaseLockTo(4) will release
  // lock 1,2 but keep 3. ReleaseLockTo(5) will release 1,2,3 but keep 4.
  asio::awaitable<expected<void>> release_lock_to(std::uint64_t index);

  const std::string &dir() const { return dir_; }

  // Close closes the current WAL file and directory.
  asio::awaitable<expected<void>> close();

  std::uint64_t seq();

#ifdef LEPTON_TEST
 public:
#else
 private:
#endif
  asio::awaitable<expected<void>> save_crc(std::uint32_t prev_crc);

  asio::awaitable<expected<void>> save_entry(const raftpb::Entry &entry);

  asio::awaitable<expected<void>> save_state(raftpb::HardState &&state);

  // cut closes current file written and creates a new one ready to append.
  // cut first creates a temp wal file and writes necessary headers into it.
  // Then cut atomically rename temp wal file to a wal file.
  asio::awaitable<expected<void>> cut();

  leaf::result<void> rename_wal(const std::string &tmp_dir_path);

  void cleanup_wal();

  fileutil::locked_file_endpoint *tail();

#ifdef LEPTON_TEST
 public:
#else
 private:
#endif
  asio::any_io_executor executor_;
  asio::strand<asio::any_io_executor> strand_;

  rocksdb::Env *env_ = nullptr;

  // the living directory of the underlay files
  std::string dir_;

  // dirFile is a fd for the wal directory for syncing on Rename
  std::optional<fileutil::directory> dir_file_;
  // metadata recorded at the head of each WAL
  std::string metadata_;
  // segment size in bytes
#ifdef LEPTON_TEST
  std::size_t segment_size_bytes_ = SEGMENT_SIZE_BYTES;
#else
  const std::size_t segment_size_bytes_ = SEGMENT_SIZE_BYTES;
#endif

  // hardstate recorded at the head of WAL
  raftpb::HardState state_;
  // snapshot to start reading
  pb::snapshot start_;
  // decoder to Decode records
  std::unique_ptr<decoder> decoder_ = nullptr;

  // in C++ no use
  // closer for Decode reader.
  // std::function<leaf::result<void>()> read_close_;

  // if set, do not fsync
  bool unsafe_no_sync_ = false;

  bool is_closed_ = false;
  // index of the last entry saved to the wal
  std::uint64_t entry_index_ = 0;
  // encoder to encode records
  std::unique_ptr<encoder> encoder_ = nullptr;
  // the locked files the WAL holds (the name is increasing)
  std::vector<wal_segment> segments_;
  file_pipeline_handle file_pipeline_ = nullptr;

  std::shared_ptr<lepton::logger_interface> logger_ = nullptr;
};

using wal_handle = std::shared_ptr<wal>;

// Exist returns true if there are any files in a given directory.
bool exist_wal(const std::string &dir);

leaf::result<std::pair<std::vector<std::string>, uint64_t>> select_wal_files(const std::string &dir,
                                                                             std::uint64_t snap_index);
// Open opens the WAL at the given snap.
// The snap SHOULD have been previously saved to the WAL, or the following
// ReadAll will fail.
// The returned WAL is ready to read and the first record will be the one after
// the given snap. The WAL cannot be appended to before reading out all of its
// previous records.
leaf::result<wal_handle> open(rocksdb::Env *env, asio::any_io_executor executor, const std::string &dir,
                              std::size_t segment_size_bytes, pb::snapshot &&snap,
                              std::shared_ptr<lepton::logger_interface> logger);

// OpenForRead only opens the wal files for read.
// Write on a read only wal panics.
leaf::result<wal_handle> open_for_read(rocksdb::Env *env, asio::any_io_executor executor, const std::string &dir,
                                       std::size_t segment_size_bytes, pb::snapshot &&snap,
                                       std::shared_ptr<lepton::logger_interface> logger);

asio::awaitable<expected<wal_handle>> create_wal(rocksdb::Env *env, asio::any_io_executor executor,
                                                 const std::string &dirpath, const std::string &metadata,
                                                 const std::size_t segment_size_bytes,
                                                 std::shared_ptr<lepton::logger_interface> logger);

/**
 * 解析 WAL 文件名
 * 格式示例: 0000000000000001-0000000000000005.wal
 */
leaf::result<std::pair<std::uint64_t, std::uint64_t>> parse_wal_name(std::string_view str);

// search_index returns the last array index of names whose raft index section is
// equal to or smaller than the given index.
// The given names MUST be sorted.
leaf::result<int> search_index(const std::vector<std::string> &names, uint64_t index);

// Verify reads through the given WAL and verifies that it is not corrupted.
// It creates a new decoder to read through the records of the given WAL.
// It does not conflict with any open WAL, but it is recommended not to
// call this function after opening the WAL for writing.
// If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
// If the loaded snap doesn't match with the expected one, it will
// return error ErrSnapshotMismatch.
asio::awaitable<expected<raftpb::HardState>> verify(asio::any_io_executor executor, const std::string &dir,
                                                    const pb::snapshot &snap,
                                                    std::shared_ptr<lepton::logger_interface> logger);

// ValidSnapshotEntries returns all the valid snapshot entries in the wal logs in the given directory.
// Snapshot entries are valid if their index is less than or equal to the most recent committed hardstate.
asio::awaitable<expected<pb::repeated_snapshot>> valid_snapshot_entries(
    asio::any_io_executor executor, const std::string &wal_dir, std::shared_ptr<lepton::logger_interface> logger);
}  // namespace lepton::storage::wal

#endif  // _LEPTON_WAL_H_
