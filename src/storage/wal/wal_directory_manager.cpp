#include "storage/wal/wal_directory_manager.h"

#include <filesystem>
#include <memory>

#include "basic/defer.h"
#include "basic/logger.h"
#include "error/expected.h"
#include "error/io_error.h"
#include "error/leaf_expected.h"
#include "error/lepton_error.h"
#include "leaf.hpp"
#include "storage/fileutil/path.h"
#include "storage/fileutil/read_dir.h"
#include "storage/pb/types.h"
#include "storage/wal/wal.h"
#include "storage/wal/wal_file.h"
#include "wal.pb.h"
namespace lepton::storage::wal {

leaf::result<void> wal_directory_manager::is_dir_writeable(const std::string& dir_name) {
  std::filesystem::path dir_path(dir_name);
  std::filesystem::path touch_file_path = dir_path / ".touch";

  rocksdb::EnvOptions env_opts;
  std::unique_ptr<rocksdb::WritableFile> touch_file_handle;

  if (auto s = env_->NewWritableFile(touch_file_path, &touch_file_handle, env_opts); !s.ok()) {
    return new_error(s, fmt::format("Directory {} is not writable: {}", dir_name, s.ToString()));
  }

  if (auto s = env_->DeleteFile(touch_file_path); !s.ok()) {
    return new_error(s, fmt::format("Failed to delete test file {}: {}", touch_file_path.string(), s.ToString()));
  }
  return {};
}

leaf::result<void> wal_directory_manager::ensure_directory_writable(const std::string& dir_name) {
  auto s = env_->CreateDirIfMissing(dir_name);
  if (!s.ok()) {
    return new_error(s, fmt::format("Failed to create directory:{}, error:{}", dir_name, s.ToString()));
  }
  return is_dir_writeable(dir_name);
}

leaf::result<void> wal_directory_manager::create_dir_all(const std::string& dir_name) {
  LEPTON_LEAF_CHECK(ensure_directory_writable(dir_name));
  BOOST_LEAF_AUTO(files, fileutil::read_dir(dir_name));
  if (!files.empty()) {
    return new_error(std::make_error_code(std::errc::directory_not_empty),
                     fmt::format("Directory {} is not empty", dir_name));
  }
  return {};
}

asio::awaitable<expected<wal_handle>> wal_directory_manager::create_wal(
    const std::string& dirpath, const std::string& metadata, std::shared_ptr<lepton::logger_interface> logger) {
  if (file_exist(dirpath)) {
    LOG_ERROR(logger, "dirpath {} already exists", dirpath);
    co_return unexpected(io_error::PARH_HAS_EXIT);
  }

  // keep temporary wal directory so WAL initialization appears atomic
  auto temp_dir_path = std::filesystem::path{dirpath} / ".tmp";
  const auto temp_dir_path_str = temp_dir_path.string();
  if (file_exist(temp_dir_path_str)) {
    if (auto s = env_->DeleteDir(temp_dir_path_str); !s.ok()) {
      LOG_ERROR(logger, "Failed to delete existing temp dir {}: {}", temp_dir_path_str, s.ToString());
      co_return unexpected(s);
    }
  }

  DEFER({ fileutil::remove_all(temp_dir_path_str); });

  auto wal_handle_result = leaf_to_expected([&]() -> leaf::result<std::unique_ptr<wal>> {
    LEPTON_LEAF_CHECK(create_dir_all(temp_dir_path_str));

    auto wal_file_path = temp_dir_path / wal_file_name(0, 0);
    BOOST_LEAF_AUTO(wal_file_handle, create_new_wal_file(executor_, env_, wal_file_path.string(), false));
    LEPTON_LEAF_CHECK(wal_file_handle.pre_allocate(SEGMENT_SIZE_BYTES, true));
    BOOST_LEAF_AUTO(encoder, new_file_encoder(wal_file_handle, 0, logger));
    auto wal_handle = std::make_unique<wal>(std::move(encoder), dirpath, metadata, logger);
    wal_handle->append_lock_file(std::move(wal_file_handle));
    return wal_handle;
  });
  if (!wal_handle_result) {
    co_return tl::unexpected(wal_handle_result.error());
  }
  auto wal_handle = std::move(wal_handle_result.value());
  CO_CHECK_AWAIT(wal_handle->save_crc(0));

  walpb::record record;
  record.set_type(::walpb::record_type::METADATA_TYPE);
  record.set_data(metadata);
  CO_CHECK_AWAIT(wal_handle->encode(record));
  CO_CHECK_AWAIT(wal_handle->save_snapshot(pb::snapshot{}));

  // TODO
}

}  // namespace lepton::storage::wal
