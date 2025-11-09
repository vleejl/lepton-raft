#ifndef _LEPTON_TEST_FILE_H_
#define _LEPTON_TEST_FILE_H_
#include <fmt/format.h>
#include <rocksdb/env.h>
#include <rocksdb/status.h>

#include <filesystem>
#include <string>

#include "leaf.h"
#include "leaf.hpp"
#include "lepton_error.h"
#include "read_dir.h"
namespace fs = std::filesystem;

void delete_if_exists(const fs::path& file_path);

namespace lepton {

class writable_file {
 public:
  writable_file(rocksdb::Env* env) : env_(env) {}

 private:
  rocksdb::Env* env_;
  rocksdb::EnvOptions env_opts_;
  std::unique_ptr<rocksdb::WritableFile> file_handle_;
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

  // leaf::result<void> create_dir_all(const std::string& dir_name) {
  //   LEPTON_LEAF_CHECK(touch_dir(dir_name));
  //   BOOST_LEAF_AUTO(files, read_dir(dir_name));
  //   if (!files.empty()) {
  //     return new_error(std::make_error_code(std::errc::directory_not_empty),
  //                      fmt::format("Directory {} is not empty", std::quoted(dir_name)));
  //   }
  //   return {};
  // }

  leaf::result<void> create_wal(const std::string& dirpath) {
    // if (file_exist(dirpath)) {
    //   return new_error(std::make_error_code(std::errc::file_exists), fmt::format("dirpath {} already exists",
    //   dirpath));
    // }

    // // keep temporary wal directory so WAL initialization appears atomic
    // auto temp_dir_path = std::filesystem::path{dirpath} / ".tmp";
    // auto temp_dir_path_str = temp_dir_path.string();
    // if (file_exist(temp_dir_path_str)) {
    //   if (auto s = env_->DeleteDir(temp_dir_path_str); !s.ok()) {
    //     return new_error(s, fmt::format("Failed to delete existing temp dir {}: {}", temp_dir_path_str,
    //     s.ToString()));
    //   }
    // }

    // rocksdb::Env* env = rocksdb::Env::Default();
    // rocksdb::EnvOptions env_opts;
    // std::unique_ptr<rocksdb::WritableFile> file_handle;

    // auto s = env->NewWritableFile(file_name, &file_handle, env_opts);
    // if (!s.ok()) {
    //   // TODO: map rocksdb status to lepton error codes
    //   return new_error(s, fmt::format("Failed to create writable file: {}", s.ToString()));
    // }
  }

 private:
  rocksdb::Env* env_;
};

}  // namespace lepton

#endif  // _LEPTON_TEST_FILE_H_
