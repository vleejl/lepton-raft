#ifndef _LEPTON_BUFFERED_FILE_READER_H_
#define _LEPTON_BUFFERED_FILE_READER_H_
#include <asio.hpp>
#include <span>

#include "byte_span.h"
#include "expected.h"
#include "fixed_byte_buffer.h"
#include "logic_error.h"

namespace lepton {

class buffered_file_reader {
  static constexpr std::size_t DEFAULT_BUFFER_SIZE = 4096;

 public:
  buffered_file_reader(asio::stream_file& file, std::size_t buf_size)
      : file_(file), buf_(buf_size), buf_pos_(0), buf_used_(0), file_offset_(0) {}

  explicit buffered_file_reader(asio::stream_file& file) : buffered_file_reader(file, DEFAULT_BUFFER_SIZE) {}

  buffered_file_reader(const buffered_file_reader&) = delete;
  buffered_file_reader& operator=(const buffered_file_reader&) = delete;

  // 异步填充 buffer
  asio::awaitable<std::size_t> async_fill() {
    if (buf_pos_ < buf_used_) co_return buf_used_ - buf_pos_;

    buf_used_ = co_await asio::async_read(file_, asio::buffer(buf_.data(), buf_.size()), asio::use_awaitable);
    buf_pos_ = 0;
    co_return buf_used_;
  }

  // 异步读取 exact 长度
  asio::awaitable<expected<byte_span>> async_read_exact(std::size_t n) {
    if (n > buf_.size()) {
      // requested size exceeds buffer size
      co_return tl::unexpected(make_error_code(logic_error::INVALID_PARAM));
    }

    std::size_t copied = 0;
    while (copied < n) {
      if (buf_pos_ >= buf_used_) {
        std::size_t filled = co_await async_fill();
        if (filled == 0) {
          co_return tl::unexpected(make_error_code(logic_error::UNEXPECTED_EOF));
        }
      }

      std::size_t to_copy = std::min(n - copied, buf_used_ - buf_pos_);
      if (to_copy < n) {
        // 数据不足时，将未消费数据移动到 buffer 开头
        std::memmove(buf_.data(), buf_.data() + buf_pos_, buf_used_ - buf_pos_);
        buf_used_ = buf_used_ - buf_pos_;
        buf_pos_ = 0;
        continue;  // 下一轮填充
      }

      buf_pos_ += to_copy;
      copied += to_copy;
    }

    file_offset_ += n;

    co_return byte_span(buf_.data(), n);
  }

  // Peek n 字节，不移动 buf_pos_
  asio::awaitable<std::span<const std::byte>> async_peek(std::size_t n) {
    while (buf_used_ - buf_pos_ < n) {
      if (buf_used_ == buf_.size()) {
        // peek size exceeds buffer capacity
        co_return tl::unexpected(make_error_code(logic_error::INVALID_PARAM));
      }
      std::size_t filled = co_await async_fill();
      if (filled == 0) break;  // EOF
    }

    std::size_t avail = buf_used_ - buf_pos_;
    if (n > avail) n = avail;
    co_return std::span<const std::byte>(buf_.data() + buf_pos_, n);
  }

  // 回退 n 字节
  void unread_bytes(std::size_t n) {
    if (n > buf_pos_) {
      // cannot unread beyond buffer start
      co_return tl::unexpected(make_error_code(logic_error::INVALID_PARAM));
    }
    buf_pos_ -= n;
    file_offset_ -= n;
  }

  // 检查是否有 torn write (假设 sector_size = 512)
  bool is_torn_write(std::size_t sector_size = 512) const {
    std::size_t pos = 0;
    while (pos < buf_used_) {
      std::size_t chunk_len = std::min(sector_size - (file_offset_ % sector_size), buf_used_ - pos);
      bool all_zero = true;
      for (std::size_t i = 0; i < chunk_len; ++i) {
        if (buf_.data()[pos + i] != std::byte{0}) {
          all_zero = false;
          break;
        }
      }
      if (all_zero) return true;
      pos += chunk_len;
    }
    return false;
  }

  std::size_t available() const noexcept { return buf_used_ - buf_pos_; }
  std::size_t file_offset() const noexcept { return file_offset_; }

 private:
  asio::stream_file& file_;
  fixed_byte_buffer buf_;
  std::size_t buf_pos_;
  std::size_t buf_used_;
  std::size_t file_offset_;
};

}  // namespace lepton

#endif  // _LEPTON_BUFFERED_FILE_READER_H_
