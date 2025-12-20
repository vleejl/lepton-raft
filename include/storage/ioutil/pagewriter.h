#pragma once
#ifndef _LEPTON_PAGEWRITER_H_
#define _LEPTON_PAGEWRITER_H_
#include <cassert>
#include <cstddef>

#include "basic/utility_macros.h"
#include "error/expected.h"
#include "error/leaf.h"
#include "storage/ioutil/writer.h"
#include "v4/proxy.h"

namespace lepton::storage::ioutil {

// 128 KB
constexpr size_t DEFAULT_BUFFER_BYTES = 128 * 1024;

// PageWriter implements the io.Writer interface so that writes will
// either be in page chunks or from flushing.
class page_writer {
  NOT_COPYABLE(page_writer)
 public:
  page_writer() = default;
  page_writer(page_writer&&) = default;
  // PageBytes is the number of bytes
  // to write per page. pageOffset is the starting offset of io.Writer.
  page_writer(pro::proxy_view<writer> w, size_t page_offset, size_t page_bytes)
      : w_(w),
        page_offset_(page_offset),
        page_bytes_(page_bytes),
        buffered_bytes_(0),
        // 设置为 defaultBufferBytes + pageBytes的原因是为了处理页面对齐的边界情况
        buf_(std::make_unique<std::byte[]>(DEFAULT_BUFFER_BYTES + page_bytes)),
        buf_watermark_bytes_(DEFAULT_BUFFER_BYTES) {
    // invalid pageBytes (%d) value, it must be greater than 0
    assert(page_bytes > 0);
  }

  pro::proxy_view<writer> writer_view() const { return w_; }

  /*
      +--------------------------+
  input --> | 1) fits buffer?         |--> yes --> copy to buf
      +--------------------------+
                  |
                  no
                  v
  +-----------------------------------------------+
  | 2) fill slack page (if needed)                |
  +-----------------------------------------------+
                  |
                  v
      +--------------------------+
      | 3) Flush full pages     |
      +--------------------------+
                  |
                  v
      +--------------------------+
      | 4) write full pages     |
      +--------------------------+
                  |
                  v
      +--------------------------+
      | 5) buffer tail (recurse)|
      +--------------------------+

  */
  // 写入 p[0..len)
  asio::awaitable<expected<std::size_t>> async_write(byte_span data);

  // flush buffer to underlying writer
  asio::awaitable<expected<void>> async_flush();

 private:
  pro::proxy_view<writer> w_;
  // pageOffset tracks the page offset of the base of the buffer
  // 当前 page 的 offset
  std::size_t page_offset_;
  // pageBytes is the number of bytes per page
  // 当前 page size
  const size_t page_bytes_ = 0;
  // bufferedBytes counts the number of bytes pending for write in the buffer
  // 当前 buffer 中的数据量
  std::size_t buffered_bytes_;
  // buf holds the write buffer
  std::unique_ptr<std::byte[]> buf_;
  // bufWatermarkBytes is the number of bytes the buffer can hold before it needs
  // to be flushed. It is less than len(buf) so there is space for slack writes
  // to bring the writer to page alignment.
  const std::size_t buf_watermark_bytes_ = 0;
};

}  // namespace lepton::storage::ioutil

#endif  // _LEPTON_PAGEWRITER_H_
