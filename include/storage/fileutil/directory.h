#pragma once

#ifndef _LEPTON_DIRECTORY_H_
#define _LEPTON_DIRECTORY_H_
#include <rocksdb/env.h>

#include <cassert>
#include <memory>
#include <string>

#include "error/leaf.h"

namespace lepton::storage::fileutil {
class directory {
 public:
  directory() = default;

  directory(rocksdb::Env* env, std::unique_ptr<rocksdb::Directory>&& dir) : env_(env), dir_(std::move(dir)) {}

  leaf::result<void> fsync();

  leaf::result<void> close();

  // IsDirWriteable checks if dir is writable by writing and removing a file
  // to dir. It returns nil if dir is writable.
  leaf::result<void> is_dir_writeable(const std::string& dir_name);

 private:
  rocksdb::Env* env_;
  std::unique_ptr<rocksdb::Directory> dir_;
};

leaf::result<directory> new_directory(rocksdb::Env* env, const std::string& path);

// IsDirWriteable checks if dir is writable by writing and removing a file
// to dir. It returns nil if dir is writable.
leaf::result<void> is_dir_writeable(rocksdb::Env* env, const std::string& dir_name);

leaf::result<void> ensure_directory_writable(rocksdb::Env* env, const std::string& dir_name);

leaf::result<void> create_dir_all(rocksdb::Env* env, const std::string& dir_name);

}  // namespace lepton::storage::fileutil

#endif  // _LEPTON_DIRECTORY_H_
