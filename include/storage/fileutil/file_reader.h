#pragma once
#ifndef _LEPTON_FILE_READER_H_
#define _LEPTON_FILE_READER_H_

#include <fmt/format.h>

#include <asio/awaitable.hpp>
#include <asio/stream_file.hpp>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "basic/utility_macros.h"
#include "error/expected.h"
#include "error/leaf.h"
namespace lepton::storage::fileutil {
class file_reader {
  NOT_COPYABLE(file_reader)

 public:
  file_reader() = default;

  file_reader(const std::string& filename, asio::stream_file&& file) : file_name_(filename), file_(std::move(file)) {}

  file_reader(file_reader&& lhs) : file_name_(lhs.file_name_), file_(std::move(lhs.file_)) {}

  const std::string& name() const { return file_name_; }

  leaf::result<std::size_t> size() const;

  leaf::result<void> seek_start(std::int64_t offset);

  leaf::result<std::uint64_t> seek_curr();
  // 移动文件指针：到文件末尾（偏移0，从末尾开始）
  // 确保追加写入：避免意外覆盖现有数据
  leaf::result<std::uint64_t> seek_end();

  leaf::result<std::size_t> read(asio::mutable_buffer buffer);

  asio::awaitable<expected<std::size_t>> async_read(asio::mutable_buffer buffer);

  expected<void> close();

 protected:
  asio::stream_file& raw_file() {
    assert(file_);
    return *file_;
  }

 protected:
  std::string file_name_;
  std::optional<asio::stream_file> file_;
};

using file_reader_handle = std::unique_ptr<file_reader>;

leaf::result<file_reader_handle> create_file_reader(asio::any_io_executor executor, const std::string& filename);

}  // namespace lepton::storage::fileutil

#endif  // _LEPTON_FILE_READER_H_
