#pragma once
#ifndef _LEPTON_DECODER_H_
#define _LEPTON_DECODER_H_
#include <wal.pb.h>

#include <cstdint>
#include <vector>

#include "absl/crc/crc32c.h"
#include "asio/any_io_executor.hpp"
#include "basic/logger.h"
#include "basic/utility_macros.h"
#include "storage/ioutil/file_buf_reader.h"
#include "storage/ioutil/fixed_byte_buffer.h"
#include "storage/ioutil/reader.h"
#include "v4/proxy.h"
namespace lepton::storage::wal {

class decoder {
  NOT_COPYABLE(decoder)

 public:
  decoder(asio::any_io_executor executor, std::shared_ptr<lepton::logger_interface> logger,
          const std::vector<pro::proxy_view<ioutil::reader>>& readers)
      : strand_(executor),
        crc_(absl::crc32c_t()),
        last_valid_off_(0),
        continue_on_crc_error_(false),
        logger_(std::move(logger)) {
    readers_.reserve(readers.size());
    for (const auto& r : readers) {
      readers_.emplace_back(pro::make_proxy<ioutil::reader, ioutil::file_buf_reader>(r));
    }
  }

  // Decode reads the next record out of the file.
  // In the success path, fills 'rec' and returns nil.
  // When it fails, it returns err and usually resets 'rec' to the defaults.
  // When continueOnCrcError is set, the method may return ErrUnexpectedEOF or ErrCRCMismatch, but preserve the read
  // (potentially corrupted) record content.
  asio::awaitable<expected<void>> decode_record(walpb::record& r);

  void update_crc(std::uint32_t prev_crc) { crc_ = absl::crc32c_t(prev_crc); }

  auto last_crc() const { return static_cast<std::uint32_t>(crc_); }

  auto last_valid_off() const { return last_valid_off_; }

 private:
  asio::awaitable<expected<void>> decode_at_offset(pro::proxy_view<ioutil::reader> buf_reader, std::uint64_t offset,
                                                   walpb::record& rec);

  // isTornEntry determines whether the last entry of the WAL was partially written
  // and corrupted because of a torn write.
  // 判断 WAL 的最后一个 entry 是否是 torn write（写一半断电导致的“撕裂写入”）。
  bool is_torn_entry(ioutil::fixed_byte_buffer& record_buf) const;

 private:
  asio::strand<asio::any_io_executor> strand_;

  absl::crc32c_t crc_;
  // 每个 segment 文件一个 reader
  std::vector<pro::proxy<ioutil::reader>> readers_;

  // lastValidOff file offset following the last valid decoded record
  std::int64_t last_valid_off_ = 0;

  // continueOnCrcError - causes the decoder to continue working even in case of crc mismatch.
  // This is a desired mode for tools performing inspection of the corrupted WAL logs.
  // See comments on 'Decode' method for semantic.
  bool continue_on_crc_error_ = false;

  std::shared_ptr<lepton::logger_interface> logger_;
};

}  // namespace lepton::storage::wal

#endif  // _LEPTON_DECODER_H_
