#include "storage/wal/wal_file.h"

#include <memory>

#include "basic/logger.h"
#include "leaf.hpp"
#include "storage/fileutil/locked_file_endpoint.h"
#include "storage/wal/encoder.h"

namespace lepton::storage::wal {

leaf::result<fileutil::file_endpoint> create_new_wal_file(asio::any_io_executor executor, const std::string& filename,
                                                          bool force_new) {
  asio::error_code ec;
  namespace fs = std::filesystem;

  // --- 第一步：初始创建 ---
  {
    asio::stream_file temp_stream(executor);
    asio::file_base::flags open_flags = asio::file_base::read_write | asio::file_base::create;
    if (force_new) {
      open_flags |= asio::file_base::truncate;
    }

    ec = temp_stream.open(filename, open_flags, ec);
    if (ec) {
      LOG_ERROR("Failed to create file {}: {}", filename, ec.message());
      return new_error(ec, fmt::format("Initial creation failed: {}", filename));
    }
  }

  // --- 第二步：跨平台设置权限 (0600) ---
  std::error_code perms_ec;
  fs::permissions(filename, fs::perms::owner_read | fs::perms::owner_write, fs::perm_options::replace, perms_ec);

  if (perms_ec) {
    LOG_ERROR("Failed to set private permissions on file {}: {}", filename, perms_ec.message());
    return new_error(perms_ec, "Failed to set private permissions");
  }

  // --- 第三步：正式打开并返回 ---
  asio::stream_file final_stream(executor);
  // 注意：此时不再使用 create/truncate 标志，确保权限不会被 reset
  ec = final_stream.open(filename, asio::file_base::read_write, ec);
  if (ec) {
    LOG_ERROR("Failed to re-open file {} after permission change: {}", filename, ec.message());
    return new_error(ec, "Failed to re-open file after permission change");
  }

  return fileutil::file_endpoint(filename, std::move(final_stream));
}

leaf::result<fileutil::locked_file_handle> create_new_wal_file(asio::any_io_executor executor, rocksdb::Env* env,
                                                               const std::string& filename, bool force_new) {
  BOOST_LEAF_AUTO(file_handle, create_new_wal_file(executor, filename, force_new));
  rocksdb::FileLock* lock;
  if (auto s = env->LockFile(filename, &lock); !s.ok()) {
    return new_error(s, fmt::format("Failed to lock WAL file {}: {}", filename, s.ToString()));
  }
  return std::make_unique<fileutil::locked_file_endpoint>(std::move(file_handle), env, lock);
}

leaf::result<std::unique_ptr<encoder>> new_file_encoder(asio::any_io_executor executor,
                                                        fileutil::locked_file_endpoint& file, std::uint32_t prev_crc,
                                                        std::shared_ptr<lepton::logger_interface> logger) {
  BOOST_LEAF_AUTO(offset, file.seek_curr());
  pro::proxy_view<ioutil::writer> writer = &file;
  return std::make_unique<encoder>(executor, writer, prev_crc, static_cast<std::uint32_t>(offset), std::move(logger));
}

}  // namespace lepton::storage::wal
