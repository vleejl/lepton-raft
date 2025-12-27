#pragma once

#include <string>
#ifndef _LEPTON_DIRECTORY_H_
#define _LEPTON_DIRECTORY_H_
#include <rocksdb/env.h>

#include <cassert>
#include <memory>

#include "error/leaf.h"
#include "error/lepton_error.h"

namespace lepton::storage::fileutil {
class directory {
 public:
  directory() = default;

  explicit directory(std::unique_ptr<rocksdb::Directory>&& dir) : dir_(std::move(dir)) {}

  leaf::result<void> fsync() {
    assert(dir_ != nullptr);
    auto s = dir_->Fsync();
    if (s.ok()) {
      return {};
    }
    return new_error(s);
  }

  leaf::result<void> close() {
    assert(dir_ != nullptr);
    auto s = dir_->Close();
    if (s.ok()) {
      return {};
    }
    return new_error(s);
  }

 private:
  std::unique_ptr<rocksdb::Directory> dir_;
};

leaf::result<directory> new_directory(rocksdb::Env* env, const std::string& path);
}  // namespace lepton::storage::fileutil

#endif  // _LEPTON_DIRECTORY_H_
