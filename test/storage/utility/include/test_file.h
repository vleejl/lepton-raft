#ifndef _LEPTON_TEST_FILE_H_
#define _LEPTON_TEST_FILE_H_
#include <fmt/format.h>
#include <rocksdb/env.h>
#include <rocksdb/status.h>

#include <cstdint>
#include <filesystem>
#include <string>

#include "asio/error_code.hpp"
#include "defer.h"
#include "leaf.h"
#include "leaf.hpp"
#include "lepton_error.h"
#include "path.h"
#include "preallocate.h"
#include "read_dir.h"
#include "wal_file.h"
namespace fs = std::filesystem;

void delete_if_exists(const fs::path& file_path);

namespace lepton {

// SegmentSizeBytes is the preallocated size of each wal segment file.
// The actual size might be larger than this. In general, the default
// value should be used, but this is defined as an exported variable
// so that tests can set a different segment size.
constexpr std::uint64_t SEGMENT_SIZE_BYTES = 64 * 1000 * 1000;  // 64MB

// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
class wal {
 private:
  // the living directory of the underlay files
  std::string dir_;

  std::string metadata_;
  wal_file file_handle_;
};

class file_helper {
 public:
  bool file_exist(const std::string& file_name) { return env_->FileExists(file_name).ok(); }

  // IsDirWriteable checks if dir is writable by writing and removing a file
  // to dir. It returns nil if dir is writable.
  leaf::result<void> is_dir_writeable(const std::string& dir_name) {
    std::filesystem::path dir_path(dir_name);
    std::filesystem::path touch_file_path = dir_path / ".touch";

    rocksdb::EnvOptions env_opts;
    std::unique_ptr<rocksdb::WritableFile> touch_file_handle;
    auto s = env_->NewWritableFile(touch_file_path, &touch_file_handle, env_opts);
    if (!s.ok()) {
      return new_error(s, fmt::format("Directory {} is not writable: {}", dir_name, s.ToString()));
    }
    s = env_->DeleteFile(touch_file_path);
    if (!s.ok()) {
      return new_error(s, fmt::format("Failed to delete test file {}: {}", touch_file_path.string(), s.ToString()));
    }
    return {};
  }

  leaf::result<void> touch_dir(const std::string& dir_name) {
    auto s = env_->CreateDirIfMissing(dir_name);
    if (!s.ok()) {
      return new_error(s, fmt::format("Failed to create directory:{}, error:{}", dir_name, s.ToString()));
    }
    return is_dir_writeable(dir_name);
  }

  leaf::result<void> create_dir_all(const std::string& dir_name) {
    LEPTON_LEAF_CHECK(touch_dir(dir_name));
    BOOST_LEAF_AUTO(files, read_dir(dir_name));
    if (!files.empty()) {
      return new_error(std::make_error_code(std::errc::directory_not_empty),
                       fmt::format("Directory {} is not empty", dir_name));
    }
    return {};
  }

  // createNewWALFile creates a WAL file.
  // To create a locked file, use *fileutil.LockedFile type parameter.
  // To create a standard file, use *os.File type parameter.
  // If forceNew is true, the file will be truncated if it already exists.
  leaf::result<wal_file> create_new_wal_file(const std::string& filename, bool force_new) {
    asio::stream_file stream_file(executor_);
    asio::file_base::flags open_flags = asio::random_access_file::read_write | asio::random_access_file::create;
    if (force_new) {
      open_flags |= asio::random_access_file::truncate;
    }
    asio::error_code ec;
    stream_file.open(filename, open_flags, ec);  // NOLINT(bugprone-unused-return-value)
    if (ec) {
      return new_error(ec, fmt::format("Failed to create WAL file {}: {}", filename, ec.message()));
    }

    rocksdb::FileLock* lock;
    if (auto s = env_->LockFile(filename, &lock); !s.ok()) {
      return new_error(s, fmt::format("Failed to lock WAL file {}: {}", filename, s.ToString()));
    }

    return wal_file{std::move(stream_file), env_, lock};
  }

  leaf::result<void> create_wal(const std::string& dirpath) {
    if (file_exist(dirpath)) {
      return new_error(std::make_error_code(std::errc::file_exists), fmt::format("dirpath {} already exists", dirpath));
    }

    // keep temporary wal directory so WAL initialization appears atomic
    auto temp_dir_path = std::filesystem::path{dirpath} / ".tmp";
    const auto temp_dir_path_str = temp_dir_path.string();
    if (file_exist(temp_dir_path_str)) {
      if (auto s = env_->DeleteDir(temp_dir_path_str); !s.ok()) {
        return new_error(s, fmt::format("Failed to delete existing temp dir {}: {}", temp_dir_path_str, s.ToString()));
      }
    }
    DEFER({ remove_all(temp_dir_path_str); });

    LEPTON_LEAF_CHECK(create_dir_all(temp_dir_path_str));

    auto wal_file_path = temp_dir_path / wal_file::file_name(0, 0);
    BOOST_LEAF_AUTO(file_handle, create_new_wal_file(wal_file_path.string(), false));
    LEPTON_LEAF_CHECK(file_handle.pre_allocate(SEGMENT_SIZE_BYTES));
  }

 private:
  rocksdb::Env* env_;
  asio::any_io_executor executor_;
};

}  // namespace lepton

#endif  // _LEPTON_TEST_FILE_H_
