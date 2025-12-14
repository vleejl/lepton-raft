#ifndef _LEPTON_WAL_FILE_H_
#define _LEPTON_WAL_FILE_H_

#include <fmt/format.h>
#include <rocksdb/env.h>
#include <rocksdb/status.h>

#include <asio/stream_file.hpp>
#include <cstddef>
#include <cstdint>
#include <string>

#include "byte_span.h"
#include "encoder.h"
#include "expected.h"
#include "leaf.h"
#include "lepton_error.h"
#include "utility_macros.h"
namespace lepton::storage::wal {
class wal_file {
 public:
  MOVABLE_BUT_NOT_COPYABLE(wal_file)

  explicit wal_file(const std::string& filename, asio::stream_file&& file, rocksdb::Env* env, rocksdb::FileLock* lock)
      : file_name_(filename), file_(std::move(file)), env_(env), lock(lock) {}

  ~wal_file() {
    if (lock != nullptr) {
      env_->UnlockFile(lock);
      lock = nullptr;
    }
  }

  static std::string wal_file_name(std::uint64_t seq, std::uint64_t index) {
    return fmt::format("{:016x}-{:016x}.wal", seq, index);
  }

  const std::string& name() const { return file_name_; }

  leaf::result<std::size_t> size() const;

  leaf::result<std::uint64_t> seek_curr();
  // 移动文件指针：到文件末尾（偏移0，从末尾开始）
  // 确保追加写入：避免意外覆盖现有数据
  leaf::result<std::uint64_t> seek_end();

  leaf::result<void> pre_allocate(uint64_t length);

  leaf::result<std::size_t> read(asio::mutable_buffer buffer);

  asio::awaitable<expected<std::size_t>> async_read(asio::mutable_buffer buffer);

  leaf::result<std::size_t> write(ioutil::byte_span data);

  asio::awaitable<expected<std::size_t>> async_write(ioutil::byte_span data);

  asio::awaitable<expected<std::size_t>> async_write_vectored_asio(std::span<const std::span<const std::byte>> spans);

  auto& file() { return file_; }

 private:
  std::string file_name_;
  asio::stream_file file_;
  rocksdb::Env* env_;
  rocksdb::FileLock* lock = nullptr;
};

leaf::result<encoder> new_file_encoder(wal_file& file, std::uint32_t prev_crc,
                                       std::shared_ptr<lepton::logger_interface> logger);
}  // namespace lepton::storage::wal

#endif  // _LEPTON_WAL_FILE_H_
