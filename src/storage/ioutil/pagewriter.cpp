#include "pagewriter.h"

#include <cstddef>

#include "expected.h"
#include "tl/expected.hpp"
#include "v4/proxy.h"
namespace lepton {

asio::awaitable<expected<std::size_t>> page_writer::async_write(byte_span data) {
  size_t n = 0;
  auto len = data.size();
  auto p = data.data();

  // Case 1: fits in buffer
  if (len + buffered_bytes_ <= buf_watermark_bytes_) {
    std::memcpy(buf_.get() + buffered_bytes_, p, len);
    buffered_bytes_ += len;
    co_return len;
  }

  // slack = bytes remaining to complete a page
  size_t mod = (page_offset_ + buffered_bytes_) % page_bytes_;
  size_t slack = (mod == 0) ? page_bytes_ : (page_bytes_ - mod);

  // If slack < page_bytes_, buffer currently ends in unaligned page
  if (slack != page_bytes_) {
    bool partial = slack > len;
    size_t copy_bytes = partial ? len : slack;

    std::memcpy(buf_.get() + buffered_bytes_, p, copy_bytes);
    buffered_bytes_ += copy_bytes;
    n += copy_bytes;

    p += copy_bytes;
    len -= copy_bytes;

    if (partial) {
      // not enough to complete slack page, stop here
      co_return n;
    }
  }

  // Now buffer is page-aligned -> flush it
  if (auto err = co_await async_flush(); err) {
    co_return tl::unexpected(err.error());
  }

  // Write full pages directly
  if (len > page_bytes_) {
    size_t pages = len / page_bytes_;
    size_t bytes = pages * page_bytes_;

    auto result = co_await w_->async_write(byte_span{p, bytes});
    if (!result) {
      co_return tl::unexpected(result.error());
    }

    n += result.value();

    p += bytes;
    len -= bytes;
    page_offset_ += bytes;
  }

  // Remaining tail goes into buffer
  std::memcpy(buf_.get(), p, len);
  buffered_bytes_ = len;
  n += len;

  co_return n;
}

// flush buffer to underlying writer
asio::awaitable<expected<void>> page_writer::async_flush() {
  if (buffered_bytes_ == 0) {
    co_return expected<void>{};
  }

  auto result = co_await w_->async_write(byte_span{buf_.get(), buffered_bytes_});
  if (!result) {
    co_return tl::unexpected(result.error());
  }
  page_offset_ += result.value();
  buffered_bytes_ = 0;
  co_return expected<void>{};
}

}  // namespace lepton
