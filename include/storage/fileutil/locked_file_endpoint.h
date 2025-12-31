#pragma once
#ifndef _LEPTON_LOCKED_FILE_ENDPOINT_H_
#define _LEPTON_LOCKED_FILE_ENDPOINT_H_
#include <rocksdb/env.h>

#include <memory>

#include "storage/fileutil/file_endpoint.h"

namespace lepton::storage::fileutil {
class locked_file_endpoint : public file_endpoint {
 public:
  locked_file_endpoint(const std::string& filename, asio::stream_file&& file, rocksdb::Env* env,
                       rocksdb::FileLock* lock)
      : file_endpoint(filename, std::move(file)), env_(env), lock_(lock) {}

  locked_file_endpoint(file_endpoint&& base, rocksdb::Env* env, rocksdb::FileLock* lock)
      : file_endpoint(std::move(base)),  // 移动构造基类
        env_(env),
        lock_(lock) {}

  ~locked_file_endpoint() {
    if (lock_ && env_) {
      env_->UnlockFile(lock_);
    }
  }

  locked_file_endpoint(locked_file_endpoint&& lhs) : file_endpoint(std::move(lhs)), env_(lhs.env_), lock_(lhs.lock_) {
    lhs.env_ = nullptr;
    lhs.lock_ = nullptr;
  }

 private:
  rocksdb::Env* env_;
  rocksdb::FileLock* lock_;
};

using locked_file_handle = std::unique_ptr<locked_file_endpoint>;

}  // namespace lepton::storage::fileutil

#endif  // _LEPTON_LOCKED_FILE_ENDPOINT_H_
