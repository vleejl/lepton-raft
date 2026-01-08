#include "storage/wal/encoder.h"

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <tl/expected.hpp>

#include "basic/logger.h"
#include "error/error.h"
#include "error/protobuf_error.h"
#include "storage/ioutil/byte_span.h"
#include "storage/ioutil/disk_constants.h"
#include "storage/ioutil/fixed_byte_buffer.h"
#include "storage/ioutil/pagewriter.h"
#include "storage/ioutil/writer.h"
#include "v4/proxy.h"
namespace lepton::storage::wal {

// walPageBytes is the alignment for flushing records to the backing Writer.
// It should be a multiple of the minimum sector size so that WAL can safely
// distinguish between torn writes and ordinary data corruption.
constexpr auto wal_page_bytes = ioutil::MIN_SECTOR_SIZE * 8;  // 4096 bytes

static void put_uint64LE(std::array<std::byte, sizeof(std::uint64_t)>& b, uint64_t v) {
  b[0] = static_cast<std::byte>(v);
  b[1] = static_cast<std::byte>(v >> 8);
  b[2] = static_cast<std::byte>(v >> 16);
  b[3] = static_cast<std::byte>(v >> 24);
  b[4] = static_cast<std::byte>(v >> 8 * 4);
  b[5] = static_cast<std::byte>(v >> 8 * 5);
  b[6] = static_cast<std::byte>(v >> 8 * 6);
  b[7] = static_cast<std::byte>(v >> 8 * 7);
}

static asio::awaitable<expected<void>> write(pro::proxy_view<ioutil::writer> writer, std::span<const std::byte> data,
                                             std::uint64_t len_field) {
  // 先写入 len_field（8 字节）
  std::array<std::byte, sizeof(std::uint64_t)> write_buf;
  put_uint64LE(write_buf, len_field);
  auto write_result = co_await writer->async_write(ioutil::byte_span(write_buf));
  if (!write_result) {
    co_return tl::unexpected(write_result.error());
  }

  // write the record with padding
  write_result = co_await writer->async_write(ioutil::byte_span(data.data(), data.size()));
  if (!write_result) {
    co_return tl::unexpected(write_result.error());
  }
  co_return ok();
}

// encode_frame_size 的功能：
// 1. 根据 data_bytes 计算 8 字节对齐所需的 padding 字节数。
// 2. 返回带有编码后的长度字段（len_field）以及 pad 字节数（pad_bytes）。
// 3. 若无需 padding，则 len_field = data_bytes。
// 4. 若需要 padding，则最高字节格式：
//      - bit7 = 1 表示存在 padding
//      - bit0..bit2 表示 pad 的数量（1~7）
//      - 剩余 56 bits 存真实 data_bytes
//
// Go 原函数：
//   func encodeFrameSize(dataBytes int) (lenField uint64, padBytes int)
//
// 本 C++ 等价返回：
//   std::pair<uint64_t, int> (len_field, pad_bytes)
INTERNAL_FUNC std::pair<uint64_t, std::size_t> encode_frame_size(std::size_t data_bytes) {
  uint64_t len_field = static_cast<uint64_t>(data_bytes);

  // 计算 8 字节对齐所需的 padding 数（范围 0~7）
  //
  // padding = (8 - (data_bytes % 8)) % 8
  // 第一个 mod 得到剩余字节，第二个 mod 确保 8 -> 0
  // force 8 byte alignment so length never gets a torn write
  int pad_bytes = (8 - (data_bytes % 8)) % 8;

  // 如果有 padding，需要把 pad_bytes 编到 len_field 的最高字节中：
  //   [最高字节] = 0x80 | pad_bytes
  //
  // 例如：
  //   pad_bytes = 4 -> 最高字节 = 0x84
  //
  // 然后左移 56 bits 放入最高字节。
  if (pad_bytes != 0) {
    uint64_t top_byte = static_cast<uint64_t>(0x80 | pad_bytes);
    len_field |= (top_byte << 56);
  }

  return {len_field, pad_bytes};
}

encoder::encoder(asio::any_io_executor executor, pro::proxy_view<ioutil::writer> w, std::uint32_t prev_crc,
                 std::uint64_t page_offset, std::shared_ptr<lepton::logger_interface> logger)
    : strand_(executor),
      crc_(absl::crc32c_t(prev_crc)),
      page_writer_(w, wal_page_bytes, page_offset),
      // 1MB buffer
      write_buf_(1024 * 1024),
      logger_(std::move(logger)) {}

asio::awaitable<expected<void>> encoder::encode(walpb::Record& r) {
  if (!strand_.running_in_this_thread()) {
    co_await asio::dispatch(strand_, asio::use_awaitable);
  }
  crc_ = absl::ExtendCrc32c(crc_, r.data());
  r.set_crc(static_cast<std::uint32_t>(crc_));
  const auto bytes_size_long = r.ByteSizeLong();
  auto [len_field, pad_bytes] = encode_frame_size(bytes_size_long);

  if (bytes_size_long + pad_bytes > write_buf_.size()) {
    ioutil::fixed_byte_buffer write_buf(static_cast<std::size_t>(bytes_size_long + pad_bytes));
    co_return co_await write_record_frame(r, bytes_size_long, len_field, pad_bytes, write_buf);
  } else {
    co_return co_await write_record_frame(r, bytes_size_long, len_field, pad_bytes, write_buf_);
  }
  co_return ok();
}

asio::awaitable<expected<void>> encoder::flush() {
  if (!strand_.running_in_this_thread()) {
    co_await asio::dispatch(strand_, asio::use_awaitable);
  }
  co_return co_await page_writer_.async_flush();
}

asio::awaitable<expected<void>> encoder::write_record_frame(const walpb::Record& r, const std::uint64_t bytes_size_long,
                                                            const uint64_t len_field, const std::size_t pad_bytes,
                                                            ioutil::fixed_byte_buffer& write_buf) {
  assert(write_buf.size() >= (bytes_size_long + pad_bytes));
  // Clear the buffer before each use to avoid leftover data in padding bytes
  write_buf.clear();
  auto is_success = r.SerializeToArray(static_cast<void*>(write_buf.data()), static_cast<int>(write_buf.size()));
  if (!is_success) {
    LOGGER_ERROR(logger_, "Failed to serialize record to array, size: {}", bytes_size_long);
    co_return unexpected(protobuf_error::SERIALIZE_TO_ARRAY_FAILED);
  }
  // 对于 write_buf 默认已经清零的情况，padding 部分自然是 0。
  auto data = std::span<const std::byte>(write_buf.data(), static_cast<std::size_t>(bytes_size_long + pad_bytes));
  auto result = co_await write(page_writer_.writer_view(), data, len_field);
  if (!result) {
    LOGGER_ERROR(logger_, "Failed to write data, error: {}", result.error().message());
    co_return tl::unexpected(result.error());
  }
  co_return ok();
}

}  // namespace lepton::storage::wal
