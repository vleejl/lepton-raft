#ifndef _LEPTON_DECODER_H_
#define _LEPTON_DECODER_H_
#include <wal.pb.h>

#include <cstdint>
#include <mutex>
#include <vector>

#include "absl/crc/crc32c.h"
#include "fixed_byte_buffer.h"
#include "logger.h"
#include "reader.h"
#include "utility_macros.h"
#include "v4/proxy.h"
namespace lepton::storage::wal {

class decoder {
 public:
  NOT_COPYABLE(decoder)
  decoder(std::shared_ptr<lepton::logger_interface> logger,
          const std::vector<pro::proxy_view<ioutil::reader>>& readers = {});

  // Decode reads the next record out of the file.
  // In the success path, fills 'rec' and returns nil.
  // When it fails, it returns err and usually resets 'rec' to the defaults.
  // When continueOnCrcError is set, the method may return ErrUnexpectedEOF or ErrCRCMismatch, but preserve the read
  // (potentially corrupted) record content.
  asio::awaitable<expected<void>> decode_record(walpb::record& r);

 private:
  asio::awaitable<expected<void>> decode_record_impl(walpb::record& r);

  // isTornEntry determines whether the last entry of the WAL was partially written
  // and corrupted because of a torn write.
  // 判断 WAL 的最后一个 entry 是否是 torn write（写一半断电导致的“撕裂写入”）。
  bool is_torn_entry(ioutil::fixed_byte_buffer& record_buf) const;

 private:
  std::mutex mutex_;
  absl::crc32c_t crc_;
  // 每个 segment 文件一个 reader
  std::vector<pro::proxy<ioutil::reader>> readers_;

  // lastValidOff file offset following the last valid decoded record
  std::uint64_t last_valid_off_ = 0;

  // continueOnCrcError - causes the decoder to continue working even in case of crc mismatch.
  // This is a desired mode for tools performing inspection of the corrupted WAL logs.
  // See comments on 'Decode' method for semantic.
  bool continue_on_crc_error_ = false;

  std::shared_ptr<lepton::logger_interface> logger_;
};

}  // namespace lepton::storage::wal

#endif  // _LEPTON_DECODER_H_
