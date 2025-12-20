#pragma once
#ifndef _LEPTON_ENCODER_H_
#define _LEPTON_ENCODER_H_
#include <wal.pb.h>

#include <cstdint>

#include "absl/crc/crc32c.h"
#include "basic/logger.h"
#include "basic/utility_macros.h"
#include "storage/ioutil/fixed_byte_buffer.h"
#include "storage/ioutil/pagewriter.h"
#include "storage/ioutil/writer.h"
#include "v4/proxy.h"

namespace lepton::storage::wal {
class encoder {
  NOT_COPYABLE(encoder)

 public:
  encoder() = default;
  encoder(pro::proxy_view<ioutil::writer> w, std::uint32_t prev_crc, std::uint64_t page_offset,
          std::shared_ptr<lepton::logger_interface> logger);
  encoder(encoder&& lhs)
      : crc_(lhs.crc_),
        page_writer_(std::move(lhs.page_writer_)),
        write_buf_(std::move(lhs.write_buf_)),
        logger_(std::move(lhs.logger_)) {}

  asio::awaitable<expected<void>> encode(walpb::record& r);

  asio::awaitable<expected<void>> flush();

#ifdef LEPTON_TEST
 public:
#else
 private:
#endif
  asio::awaitable<expected<void>> write_record_frame(const walpb::record& r, const std::uint64_t bytes_size_long,
                                                     const uint64_t len_field, const std::size_t pad_bytes,
                                                     ioutil::fixed_byte_buffer& write_buf);

 private:
  std::mutex mutex_;
  absl::crc32c_t crc_;
  ioutil::page_writer page_writer_;

  ioutil::fixed_byte_buffer write_buf_;

  std::shared_ptr<lepton::logger_interface> logger_;
};

#ifdef LEPTON_TEST
std::pair<uint64_t, std::size_t> encode_frame_size(std::size_t data_bytes);
#endif

}  // namespace lepton::storage::wal

#endif  // _LEPTON_ENCODER_H_
