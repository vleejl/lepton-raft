#pragma once
#ifndef _LEPTON_ASYNC_WRITABLE_FILE_H_
#define _LEPTON_ASYNC_WRITABLE_FILE_H_
#include <rocksdb/env.h>

#include <asio.hpp>
#include <cstdint>
#include <memory>

#include "error/expected.h"
#include "error/leaf.h"
namespace lepton::storage::fs {

class asio_writable_file {
 public:
  asio_writable_file(asio::any_io_executor ex, asio::thread_pool& pool, std::unique_ptr<rocksdb::WritableFile> file,
                     const std::string& path)
      : executor_(ex), pool_(pool), file_(std::move(file)), path_(path), offset_(0) {}

  asio::any_io_executor get_executor() const noexcept { return executor_; }

  asio::awaitable<expected<std::size_t>> async_write(const char* data, std::size_t size);

  leaf::result<void> truncate(std::uint64_t size);

  leaf::result<void> preallocate(std::uint64_t size, bool extend);

  expected<void> fdatasync();

  // 当前 offset
  std::uint64_t offset() const noexcept { return offset_; }

 private:
  asio::any_io_executor executor_;
  asio::thread_pool& pool_;
  rocksdb::Env* env_;
  std::unique_ptr<rocksdb::WritableFile> file_;
  std::string path_;
  std::uint64_t offset_;  // 用户维护 cursor
};
}  // namespace lepton::storage::fs

#endif  // _LEPTON_ASYNC_WRITABLE_FILE_H_
