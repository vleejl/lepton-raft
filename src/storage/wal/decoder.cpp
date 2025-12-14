#include "decoder.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "absl/crc/crc32c.h"
#include "asio/buffer.hpp"
#include "defer.h"
#include "disk_constants.h"
#include "expected.h"
#include "file_buf_reader.h"
#include "fixed_byte_buffer.h"
#include "io.h"
#include "io_error.h"
#include "leaf_expected.h"
#include "logger.h"
#include "logic_error.h"
#include "protobuf_error.h"
#include "proxy.h"
#include "reader.h"
#include "tl/expected.hpp"
#include "v4/proxy.h"
#include "wal_protobuf.h"
namespace lepton::storage::wal {

/*
WAL 文件结构：
+-------------------------+
| 8 字节：frame size      |   ← 这就是 FRAME_SIZE_BYTES
+-------------------------+
| recBytes (Record 数据)  |
+-------------------------+
| padBytes (padding)      |
+-------------------------+
etcd 中每条 WAL entry 都以前 8 字节开头，这 8 字节是一个 int64，用小端编码保存：
表示 WAL entry 的帧头长度：固定 8 字节，用来存放该 entry 的总长度（record size + padding）。
*/
// frameSizeBytes is frame size in bytes, including record size and padding size
constexpr std::size_t FRAME_SIZE_BYTES = 8;

static asio::awaitable<expected<std::uint64_t>> read_uint64(pro::proxy_view<ioutil::reader> reader) {
  std::uint64_t n = 0;
  std::array<std::byte, sizeof(n)> buf;
  auto read_result = co_await reader->async_read(asio::buffer(buf));
  if (!read_result) {
    co_return tl::unexpected(read_result.error());
  }
  if (read_result.value() != sizeof(n)) {
    co_return tl::unexpected(make_error_code(io_error::UNEXPECTED_EOF));
  }
  for (std::uint64_t i = 0; i < sizeof(n); ++i) {
    n |= static_cast<std::uint64_t>(buf[i]) << (8 * i);
  }
  co_return n;
}

// decodeFrameSize decodes the 64-bit length field used in etcd's WAL (Write-Ahead Log) format.
//
// The etcd WAL uses a compact encoding scheme that combines record size and padding
// information into a single 64-bit field with the following bit layout:
//
//	Bit 0-55:  Record data size in bytes (lower 56 bits)
//	Bit 56-62: Reserved (must be zero)
//	Bit 63:    Padding flag (1 = padding present, 0 = no padding)
//
// When the padding flag (MSB) is set, the padding size is stored in bits 56-58
// (the lower 3 bits of the highest byte).
//
// This design allows etcd to:
// - Support very large records (up to 2^56 - 1 bytes)
// - Optional padding for filesystem block alignment
// - Efficient encoding in a single 64-bit field
//
// Examples:
//   - 0x0000000000000100 → 256 byte record, no padding
//   - 0x8000000000000064 → 100 byte record, no padding (but padding flag incorrectly set)
//   - 0x8300000000000064 → 100 byte record, 3 bytes of padding
//   - 0x8700000000FFFFFF → 0xFFFFFF byte record, 7 bytes of padding (max padding)
//
// Parameters:
//   - lenField: The 64-bit length field read from WAL file header
//
// Returns:
//   - recBytes: The actual record data size in bytes (excluding padding)
//   - padBytes: The number of padding bytes (0-7) if padding flag is set
static std::pair<uint64_t, uint64_t> decode_frame_size(uint64_t len_field) {
  // 提取低56位作为记录大小
  constexpr uint64_t RECORD_MASK = 0x00FFFFFFFFFFFFFFULL;
  uint64_t rec_bytes = len_field & RECORD_MASK;

  // 检查最高位（第63位）判断是否有填充
  uint64_t pad_bytes = 0;
  if (len_field & 0x8000000000000000ULL) {
    // 从最高字节提取低3位作为填充大小
    pad_bytes = (len_field >> 56) & 0x7ULL;
  }

  return {rec_bytes, pad_bytes};
}

decoder::decoder(std::shared_ptr<lepton::logger_interface> logger,
                 const std::vector<pro::proxy_view<ioutil::reader>>& readers)
    : crc_(absl::crc32c_t()), last_valid_off_(0), continue_on_crc_error_(false), logger_(std::move(logger)) {
  readers_.reserve(readers.size());
  for (const auto& r : readers) {
    readers_.emplace_back(pro::make_proxy<ioutil::reader, ioutil::file_buf_reader>(r));
  }
}

asio::awaitable<expected<void>> decoder::decode_record(walpb::record& r) {
  r.Clear();
  std::lock_guard<std::mutex> guard(mutex_);
  co_return co_await decode_record_impl(r);
}

asio::awaitable<expected<void>> decoder::decode_record_impl(walpb::record& rec) {
  if (readers_.empty()) {
    co_return tl::unexpected(io_error::IO_EOF);
  }
  auto& buf_reader = readers_.front();
  auto read_result = co_await read_uint64(buf_reader);
  if ((!read_result && read_result.error() == asio::error::eof) || (read_result && read_result.value() == 0)) {
    // hit end of file or preallocated space
    readers_.erase(readers_.begin());
    if (readers_.empty()) {
      co_return tl::unexpected(io_error::IO_EOF);
    }
    co_return co_await decode_record_impl(rec);
  }
  if (!read_result) {
    co_return tl::unexpected(read_result.error());
  }
  auto [rec_bytes, pad_bytes] = decode_frame_size(read_result.value());
  // The length of current WAL entry must be less than the remaining file size.
  auto file_size_result = leaf_to_expected([&]() -> leaf::result<std::size_t> {
    BOOST_LEAF_AUTO(file_size, buf_reader->size());
    return file_size;
  });
  if (!file_size_result) {
    co_return tl::unexpected(file_size_result.error());
  }
  const auto file_size = file_size_result.value();
  auto max_entry_limit = file_size - last_valid_off_ - pad_bytes;
  if (rec_bytes > max_entry_limit) {
    LOG_ERROR(logger_,
              "[wal] max entry size limit exceeded when reading {}, recBytes: {}, fileSize({}) - offset({}) - "
              "padBytes({}) = entryLimit({})",
              buf_reader->name(), rec_bytes, file_size, last_valid_off_, pad_bytes, max_entry_limit);
    co_return tl::unexpected(io_error::UNEXPECTED_EOF);
  }

  ioutil::fixed_byte_buffer record_buf(static_cast<std::size_t>(rec_bytes + pad_bytes));
  auto read_full_result = co_await read_full(buf_reader, record_buf.asio_mutable_buffer());
  if (!read_full_result) {
    // ReadFull returns io.EOF only if no bytes were read
    // the decoder should treat this as an ErrUnexpectedEOF instead.
    // 这里预期如果返回的为 IO_EOF 都应该是lepton封装的错误码
    if (read_full_result.error() == io_error::IO_EOF) {
      co_return tl::unexpected(io_error::UNEXPECTED_EOF);
    }
    co_return tl::unexpected(read_full_result.error());
  }
  if (!rec.ParseFromArray(static_cast<void*>(record_buf.data()), static_cast<int>(rec_bytes))) {
    if (is_torn_entry(record_buf)) {
      co_return tl::unexpected(io_error::UNEXPECTED_EOF);
    }
    co_return tl::unexpected(protobuf_error::PROOBUF_PARSE_FAILED);
  }
  // skip crc checking if the record type is CrcType
  if (rec.type() != walpb::record_type::CRC_TYPE) {
    crc_ = absl::ExtendCrc32c(crc_, rec.data());

    if (auto validate_crc_result = pb::validate_rec_crc(rec, crc_); !validate_crc_result) {
      if (!continue_on_crc_error_) {
        rec.Clear();
      }
      DEFER({ last_valid_off_ += FRAME_SIZE_BYTES + rec_bytes + pad_bytes; });
      if (is_torn_entry(record_buf)) {
        LOG_ERROR(logger_, "{}: in file '{}' at position: {}", make_error_code(io_error::UNEXPECTED_EOF).message(),
                  buf_reader->name(), last_valid_off_);
        co_return tl::unexpected(io_error::UNEXPECTED_EOF);
      }
      LOG_ERROR(logger_, "{}: in file '{}' at position: {}, expected crc: {}, record crc: {}",
                validate_crc_result.error().message(), buf_reader->name(), last_valid_off_,
                static_cast<std::uint32_t>(crc_), rec.crc());
      co_return validate_crc_result;
    }
  }
  // record decoded as valid; point last valid offset to end of record
  last_valid_off_ += FRAME_SIZE_BYTES + rec_bytes + pad_bytes;
  co_return ok();
}

bool decoder::is_torn_entry(ioutil::fixed_byte_buffer& record_buf) const {
  if (readers_.size() != 1) {
    // 必须是最后一个文件（=最后一个 WAL segment）
    // 因为 torn write 只可能发生在当前 WAL 的最后一个 segment 的末尾。
    // 对于旧的 WAL 文件，内容已经写满，不会有撕裂写入的问题。
    return false;
  }
  std::vector<std::span<std::byte>> chunks;
  // split data on sector boundaries
  std::size_t curr_off = 0;
  auto file_off = last_valid_off_ + FRAME_SIZE_BYTES;
  while (curr_off < record_buf.size()) {
    auto sector_remain = ioutil::MIN_SECTOR_SIZE - (file_off % ioutil::MIN_SECTOR_SIZE);
    if (sector_remain > record_buf.size() - curr_off) {
      sector_remain = record_buf.size() - curr_off;
    }
    chunks.emplace_back(record_buf.data() + curr_off, record_buf.data() + curr_off + sector_remain);
    curr_off += sector_remain;
    file_off += sector_remain;
  }

  // if any data for a sector chunk is all 0, it's a torn write
  for (const auto& chunk : chunks) {
    bool all_zero = true;
    for (const auto& b : chunk) {
      if (b != std::byte{0}) {
        all_zero = false;
        break;
      }
    }
    if (all_zero) {
      return true;
    }
  }
  return false;
}

}  // namespace lepton::storage::wal
