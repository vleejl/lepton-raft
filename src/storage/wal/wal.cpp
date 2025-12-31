#include "storage/wal/wal.h"

#include <cstddef>
#include <filesystem>
#include <format>
#include <memory>
#include <string>

#include "asio/use_future.hpp"
#include "basic/defer.h"
#include "basic/logger.h"
#include "basic/time.h"
#include "error/expected.h"
#include "error/io_error.h"
#include "error/leaf.h"
#include "error/leaf_expected.h"
#include "error/lepton_error.h"
#include "error/logic_error.h"
#include "fmt/format.h"
#include "leaf.hpp"
#include "storage/fileutil/directory.h"
#include "storage/fileutil/path.h"
#include "storage/fileutil/read_dir.h"
#include "storage/pb/types.h"
#include "storage/pb/wal_protobuf.h"
#include "storage/wal/wal.h"
#include "storage/wal/wal_file.h"
#include "wal.pb.h"
namespace lepton::storage::wal {

constexpr auto WARN_SYNC_DURATION = std::chrono::seconds{1};

asio::awaitable<expected<void>> wal::save_crc(std::uint32_t prev_crc) {
  walpb::record record;
  record.set_type(::walpb::record_type::CRC_TYPE);
  record.set_crc(prev_crc);
  co_return co_await encoder_->encode(record);
}

asio::awaitable<expected<void>> wal::save_snapshot(const pb::snapshot& snapshot) {
  if (auto ret = pb::validate_snapshot_for_write(snapshot); !ret) {
    LOGGER_ERROR(logger_, "validate failed, error:{}", ret.error().message());
    co_return ret;
  }

  auto self = shared_from_this();
  if (!strand_.running_in_this_thread()) {
    co_await asio::dispatch(strand_, asio::use_awaitable);
  }

  walpb::record record;
  record.set_type(::walpb::record_type::SNAPSHOT_TYPE);
  if (snapshot.ByteSizeLong() > 0) {
    std::string data;
    if (!snapshot.SerializeToString(&data)) {
      LOGGER_CRITICAL(logger_, "SerializeToString snapshot failed");
      co_return unexpected(logic_error::SERIALIZE_FAILED);
    }
    record.set_data(std::move(data));
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
    LOGGER_WARN(logger_, "slow fdatasyc, took: {} seconds, expect: {} seconds", to_seconds(took),
                to_seconds(WARN_SYNC_DURATION));
  }
  co_return result;
}

leaf::result<void> wal::rename_wal(const std::string& tmp_dir_path) {
  LEPTON_LEAF_CHECK(fileutil::remove_all(dir_));
  LEPTON_LEAF_CHECK(fileutil::rename(tmp_dir_path, dir_));
  BOOST_LEAF_AUTO(file_pipeline, new_file_pipeline(executor_, env_, dir_, segment_size_bytes_, logger_));
  file_pipeline_ = std::move(file_pipeline);
  BOOST_LEAF_AUTO(dir_file, fileutil::new_directory(env_, dir_));
  dir_file_ = std::move(dir_file);
  return {};
}

void wal::cleanup_wal() {
  co_spawn(
      executor_,
      [&]() -> asio::awaitable<void> {
        if (auto result = co_await close(); !result) {
          LOGGER_CRITICAL(logger_, "Failed to close wal during cleanup: {}", result.error().message());
        }
        co_return;
      },
      asio::use_future);

  using namespace std::chrono;
  auto now = system_clock::now();
  auto seconds = floor<std::chrono::seconds>(now);
  auto micros = duration_cast<microseconds>(now - seconds).count();
  auto broken_dir_name = std::format("{}.broken.{:%Y%m%d.%H%M%S}.{:06}", dir_, seconds, micros);
  if (auto result = leaf_to_expected([&]() { return fileutil::rename(dir_, broken_dir_name); }); !result) {
    LOGGER_CRITICAL(logger_, "Failed to rename broken wal dir {} to {}: {}", dir_, broken_dir_name,
                    result.error().message());
  }
  return;
}

asio::awaitable<expected<void>> wal::close() {
  auto self = shared_from_this();
  if (!strand_.running_in_this_thread()) {
    co_await asio::dispatch(strand_, asio::use_awaitable);
  }

  if (is_closed_) co_return ok();
  is_closed_ = true;
  if (file_pipeline_ != nullptr) {
    co_await file_pipeline_->close();
    file_pipeline_.reset();
  }

  if (tail() != nullptr) {
    CO_CHECK_AWAIT(sync());
  }
  for (auto& file_handle : lock_files_) {
    if (file_handle == nullptr) {
      continue;
    }
    if (auto result = file_handle->close(); !result) {
      LOGGER_WARN(logger_, "Failed to close wal file handle: {}", result.error().message());
    }
  }
  co_return leaf_to_expected([&]() -> leaf::result<void> { return dir_file_.close(); });
}

bool exist_wal(const std::string& dir) {
  auto read_dir_result = fileutil::read_dir(dir, fileutil::read_dir_op::with_ext(".wal"));
  if (!read_dir_result) {
    return false;
  }
  auto& entries = read_dir_result.value();
  return !entries.empty();
}

asio::awaitable<expected<wal_handle>> create_wal(rocksdb::Env* env, asio::any_io_executor executor,
                                                 const std::string& dirpath, const std::string& metadata,
                                                 const std::size_t segment_size_bytes,
                                                 std::shared_ptr<lepton::logger_interface> logger) {
  if (exist_wal(dirpath)) {
    LOGGER_ERROR(logger, "dirpath {} already exists", dirpath);
    co_return unexpected(io_error::PARH_HAS_EXIT);
  }

  // keep temporary wal directory so WAL initialization appears atomic
  const auto temp_dir_path = fmt::format("{}.tmp", dirpath);
  if (fileutil::path_exist(temp_dir_path)) {
    if (auto s = leaf_to_expected([&]() { return fileutil::remove_all(temp_dir_path); }); !s.has_value()) {
      LOGGER_ERROR(logger, "Failed to delete existing temp dir {}: {}", temp_dir_path, s.error().message());
      co_return tl::unexpected(s.error());
    }
  }

  DEFER({ (void)fileutil::remove_all(temp_dir_path); });

  auto wal_handle_result = leaf_to_expected([&]() -> leaf::result<wal_handle> {
    LEPTON_LEAF_CHECK(fileutil::create_dir_all(env, temp_dir_path));
    auto wal_file_path = std::filesystem::path(temp_dir_path) / wal_file_name(0, 0);
    BOOST_LEAF_AUTO(wal_file_handle, create_new_wal_file(executor, env, wal_file_path.string(), false));
    BOOST_LEAF_AUTO(_, wal_file_handle->seek_end());
    LEPTON_LEAF_CHECK(wal_file_handle->pre_allocate(segment_size_bytes, true));
    BOOST_LEAF_AUTO(encoder, new_file_encoder(executor, *wal_file_handle, 0, logger));
    auto wal_handle =
        std::make_shared<wal>(executor, env, std::move(encoder), dirpath, metadata, segment_size_bytes, logger);
    wal_handle->append_lock_file(std::move(wal_file_handle));
    return wal_handle;
  });
  if (!wal_handle_result) {
    LOGGER_ERROR(logger, "Failed to create wal in temp dir {}: {}", temp_dir_path, wal_handle_result.error().message());
    co_return tl::unexpected(wal_handle_result.error());
  }
  auto wal_handle = std::move(wal_handle_result.value());
  CO_CHECK_AWAIT(wal_handle->save_crc(0));

  walpb::record record;
  record.set_type(::walpb::record_type::METADATA_TYPE);
  record.set_data(metadata);
  CO_CHECK_AWAIT(wal_handle->encode(record));
  CO_CHECK_AWAIT(wal_handle->save_snapshot(pb::snapshot{}));

  if (auto result = leaf_to_expected([&]() -> leaf::result<void> { return wal_handle->rename_wal(temp_dir_path); });
      !result.has_value()) {
    LOGGER_WARN(logger, "Failed to rename wal from {} to {}: {}", temp_dir_path, dirpath, result.error().message());
    co_return tl::unexpected{result.error()};
  }

  std::filesystem::path dir_path(wal_handle->dir());
  auto parent_path = dir_path.parent_path();
  if (auto result = leaf_to_expected([&]() -> leaf::result<void> {
        // directory was renamed; sync parent dir to persist rename
        BOOST_LEAF_AUTO(parent_dir, fileutil::new_directory(env, parent_path.string()));
        if (!parent_dir.fsync()) {
          (void)parent_dir.close();
          return new_error(io_error::FSYNC_FAILED, fmt::format("Failed to fsync parent dir {}", parent_path.string()));
        }
        LEPTON_LEAF_CHECK(parent_dir.close());
        return {};
      });
      !result.has_value()) {
    LOGGER_WARN(logger, "Failed to fsync parent directory {}", parent_path.string());
    wal_handle->cleanup_wal();
    co_return tl::unexpected{result.error()};
  }
  co_return wal_handle;
}
}  // namespace lepton::storage::wal
