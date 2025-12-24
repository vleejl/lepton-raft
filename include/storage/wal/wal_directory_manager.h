#pragma once
#ifndef _LEPTON_WAL_DIRECTORY_MANAGER_H_
#define _LEPTON_WAL_DIRECTORY_MANAGER_H_
#include <fmt/format.h>
#include <rocksdb/env.h>

#include <string>

#include "basic/logger.h"
#include "error/lepton_error.h"
#include "wal.h"
namespace lepton::storage::wal {

// SegmentSizeBytes is the preallocated size of each wal segment file.
// The actual size might be larger than this. In general, the default
// value should be used, but this is defined as an exported variable
// so that tests can set a different segment size.
constexpr static std::uint64_t SEGMENT_SIZE_BYTES = 64 * 1000 * 1000;  // 64MB

class wal_directory_manager {
 public:
  bool file_exist(const std::string& file_name) { return env_->FileExists(file_name).ok(); }

  // IsDirWriteable checks if dir is writable by writing and removing a file
  // to dir. It returns nil if dir is writable.
  leaf::result<void> is_dir_writeable(const std::string& dir_name);

  leaf::result<void> ensure_directory_writable(const std::string& dir_name);

  leaf::result<void> create_dir_all(const std::string& dir_name);

  // Create creates a WAL ready for appending records. The given metadata is
  // recorded at the head of each WAL file, and can be retrieved with ReadAll
  // after the file is Open.
  asio::awaitable<expected<wal>> create_wal(const std::string& dirpath, const std::string& metadata,
                                            std::shared_ptr<lepton::logger_interface> logger);

 private:
  rocksdb::Env* env_;
  asio::any_io_executor executor_;
};
}  // namespace lepton::storage::wal

#endif  // _LEPTON_WAL_DIRECTORY_MANAGER_H_
