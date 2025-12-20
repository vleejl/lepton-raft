#pragma once
#ifndef _LEPTON_ENV_FILE_ENDPOINT_H_
#define _LEPTON_ENV_FILE_ENDPOINT_H_

#include <fmt/format.h>
#include <rocksdb/env.h>
#include <rocksdb/status.h>

#include <asio/stream_file.hpp>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "basic/utility_macros.h"
#include "error/expected.h"
#include "error/leaf.h"
#include "error/lepton_error.h"
#include "storage/ioutil/byte_span.h"
namespace lepton::storage::fileutil {
class env_file_endpoint {
  NOT_COPYABLE(env_file_endpoint)

 public:
  env_file_endpoint() = default;

  env_file_endpoint(const std::string& filename, asio::stream_file&& file, rocksdb::Env* env, rocksdb::FileLock* lock)
      : file_name_(filename), file_(std::move(file)), env_(env), lock_(lock) {}

  env_file_endpoint(env_file_endpoint&& lhs)
      : file_name_(lhs.file_name_), file_(std::move(lhs.file_)), env_(lhs.env_), lock_(lhs.lock_) {
    lhs.env_ = nullptr;
    lhs.lock_ = nullptr;
  }

  ~env_file_endpoint() {
    if (lock_ != nullptr) {
      env_->UnlockFile(lock_);
      lock_ = nullptr;
    }
  }

  const std::string& name() const { return file_name_; }

  leaf::result<std::size_t> size() const;

  leaf::result<void> seek_start(std::int64_t offset);

  leaf::result<std::uint64_t> seek_curr();
  // 移动文件指针：到文件末尾（偏移0，从末尾开始）
  // 确保追加写入：避免意外覆盖现有数据
  leaf::result<std::uint64_t> seek_end();

  leaf::result<void> pre_allocate(uint64_t length, bool extend_file);

  leaf::result<std::size_t> read(asio::mutable_buffer buffer);

  asio::awaitable<expected<std::size_t>> async_read(asio::mutable_buffer buffer);

  leaf::result<std::size_t> write(ioutil::byte_span data);

  asio::awaitable<expected<std::size_t>> async_write(ioutil::byte_span data);

  asio::awaitable<expected<std::size_t>> async_write_vectored_asio(std::span<const std::span<const std::byte>> spans);

 private:
  asio::stream_file& raw_file() {
    assert(file_);
    return *file_;
  }

 private:
  std::string file_name_;
  std::optional<asio::stream_file> file_;
  rocksdb::Env* env_;
  rocksdb::FileLock* lock_ = nullptr;
};

}  // namespace lepton::storage::fileutil

#endif  // _LEPTON_ENV_FILE_ENDPOINT_H_
