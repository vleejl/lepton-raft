#pragma once
#ifndef _LEPTON_FILE_BUF_READER_H_
#define _LEPTON_FILE_BUF_READER_H_
#include <asio.hpp>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <system_error>
#include <tl/expected.hpp>

#include "asio/buffer.hpp"
#include "basic/utility_macros.h"
#include "error/expected.h"
#include "storage/ioutil/fixed_byte_buffer.h"
#include "storage/ioutil/reader.h"
#include "v4/proxy.h"
namespace lepton::storage::ioutil {
// A buffered file reader that wraps a reader and provides buffering capabilities.
// golang bufio.Reader 类似实现
class file_buf_reader {
  static constexpr std::size_t DEFAULT_BUFFER_SIZE = 4096;
  static constexpr std::size_t MIN_READE_BUFFER_SIZE = 4096;

 public:
  explicit file_buf_reader(pro::proxy_view<reader> file) : file_buf_reader(file, DEFAULT_BUFFER_SIZE) {}

  file_buf_reader(pro::proxy_view<reader> file, std::size_t buf_size)
      : file_(file), buf_(std::max(MIN_READE_BUFFER_SIZE, buf_size)) {}

  NOT_COPYABLE(file_buf_reader);

  file_buf_reader(file_buf_reader&&) = default;

  std::size_t buffered() const noexcept { return buf_write_pos_ - buf_read_pos_; }

  const std::string& name() const { return file_->name(); }

  leaf::result<std::size_t> size() const { return file_->size(); }

  asio::awaitable<expected<std::size_t>> async_read(asio::mutable_buffer buffer) {
    if (buffer.size() == 0) {
      if (buffered() > 0) {  // 缓冲区有数据：返回 0（符合读取0字节的约定）
        co_return 0;
      }
      // 缓冲区空：返回之前的错误（如果有）
      co_return tl::unexpected(read_err());
    }
    if (buf_read_pos_ == buf_write_pos_) {  // 缓冲区为空
      if (ec_) {
        co_return tl::unexpected(read_err());
      }
      if (buffer.size() >= buf_.size()) {
        // Large read, empty buffer.
        // Read directly into p to avoid copy.
        auto read_size = co_await async_read_buf_impl(buffer);
        if (ec_) {
          co_return tl::unexpected(read_err());
        }
        co_return read_size;
      }
      // One read.
      // Do not use b.fill, which will loop.
      buf_read_pos_ = 0;
      buf_write_pos_ = 0;
      auto read_size = co_await async_read_buf_impl(asio::buffer(buf_.data(), buf_.size()));
      if (read_size == 0) {
        co_return tl::unexpected(read_err());
      }
      buf_write_pos_ += read_size;
    }

    // copy as much as we can
    std::size_t available = static_cast<std::size_t>(buf_write_pos_ - buf_read_pos_);
    asio::const_buffer src(buf_.data() + buf_read_pos_, available);
    std::size_t copied = asio::buffer_copy(buffer, src);
    buf_read_pos_ += copied;
    co_return copied;
  }

 private:
  std::error_code read_err() {
    auto ec = ec_;
    ec_ = std::error_code{};
    return ec;
  }

  asio::awaitable<std::size_t> async_read_buf_impl(asio::mutable_buffer buffer) {
    auto read_result = co_await file_->async_read(buffer);
    if (!read_result) {
      ec_ = read_result.error();
      co_return 0;
    }
    ec_ = std::error_code{};
    co_return read_result.value();
  }

 private:
  pro::proxy_view<reader> file_;
  fixed_byte_buffer buf_;
  std::error_code ec_;
  std::size_t buf_write_pos_;
  std::size_t buf_read_pos_;
};

}  // namespace lepton::storage::ioutil

#endif  // _LEPTON_FILE_BUF_READER_H_
