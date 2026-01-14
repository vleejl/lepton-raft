#pragma once
#ifndef _LEPTON_LOCKED_FILE_ENDPOINT_H_
#define _LEPTON_LOCKED_FILE_ENDPOINT_H_
#include <rocksdb/env.h>

#include "error/expected.h"
#include "error/leaf.h"
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

  virtual expected<void> close() {
    if (lock_ && env_) {
      env_->UnlockFile(lock_);
    }
    lock_ = nullptr;
    env_ = nullptr;
    return ok();
  }

  ~locked_file_endpoint() { close(); }

  locked_file_endpoint(locked_file_endpoint&& lhs) : file_endpoint(std::move(lhs)), env_(lhs.env_), lock_(lhs.lock_) {
    lhs.env_ = nullptr;
    lhs.lock_ = nullptr;
  }

 private:
  rocksdb::Env* env_;
  rocksdb::FileLock* lock_;
};

using locked_file_endpoint_handle = std::unique_ptr<locked_file_endpoint>;

leaf::result<locked_file_endpoint_handle> create_locked_file_endpoint(rocksdb::Env* env, file_endpoint&& base,
                                                                      const std::string& filename);

leaf::result<locked_file_endpoint_handle> create_locked_file_endpoint(rocksdb::Env* env, asio::any_io_executor executor,
                                                                      const std::string& filename,
                                                                      asio::file_base::flags open_flags);
}  // namespace lepton::storage::fileutil

#endif  // _LEPTON_LOCKED_FILE_ENDPOINT_H_
