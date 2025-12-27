#include "storage/wal/wal.h"

#include <mutex>

#include "basic/logger.h"
#include "basic/time.h"
#include "error/leaf.h"
#include "error/logic_error.h"
#include "storage/fileutil/path.h"
#include "storage/pb/wal_protobuf.h"
#include "wal.pb.h"

namespace lepton::storage::wal {

constexpr auto WARN_SYNC_DURATION = std::chrono::seconds{1};

asio::awaitable<expected<void>> wal::save_crc(std::uint32_t prev_crc) {
  walpb::record record;
  record.set_type(::walpb::record_type::CRC_TYPE);
  record.set_crc(prev_crc);
  co_return co_await encoder_->encode(record);
}

asio::awaitable<expected<void>> wal::save_snapshot(const pb::snapshot &snapshot) {
  if (auto ret = pb::validate_snapshot_for_write(snapshot); !ret) {
    co_return ret;
  }

  std::lock_guard<std::mutex> guard(mutex_);
  walpb::record record;
  record.set_type(::walpb::record_type::SNAPSHOT_TYPE);
  if (!snapshot.SerializeToString(record.mutable_data())) {
    LOG_CRITICAL(logger_, "SerializeToString snapshot failed");
    co_return unexpected(logic_error::SERIALIZE_FAILED);
  }

  CO_CHECK_AWAIT(encoder_->encode(record));

  // update enti only when snapshot is ahead of last index
  if (entry_index_ < snapshot.index()) {
    entry_index_ = snapshot.index();
  }

  co_return co_await sync();
}

asio::awaitable<expected<void>> wal::sync() {
  if (encoder_ != nullptr) {
    CO_CHECK_AWAIT(encoder_->flush());
  }

  if (unsafe_no_sync_) {
    co_return ok();
  }

  auto start = std::chrono::steady_clock::now();
  auto result = tail()->fdatasync();
  if (auto took = time_since(start); took > WARN_SYNC_DURATION) {
    LOG_WARN(logger_, "slow fdatasyc, took: {}, expect: {}", to_seconds(took), to_seconds(WARN_SYNC_DURATION));
  }
  co_return result;
}

leaf::result<void> wal::rename_wal(const std::string &tmp_dir_path) { LEPTON_LEAF_CHECK(fileutil::remove_all(dir_)); }

}  // namespace lepton::storage::wal
