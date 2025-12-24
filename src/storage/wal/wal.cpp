#include "storage/wal/wal.h"

#include "wal.pb.h"

namespace lepton::storage::wal {

asio::awaitable<expected<void>> wal::save_crc(std::uint32_t prev_crc) {
  walpb::record record;
  record.set_type(::walpb::record_type::CRC_TYPE);
  record.set_crc(prev_crc);
  co_return co_await encoder_.encode(record);
}

}  // namespace lepton::storage::wal
