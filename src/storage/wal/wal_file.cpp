#include "wal_file.h"

namespace lepton::storage::wal {

leaf::result<fileutil::env_file_endpoint> create_new_wal_file(asio::any_io_executor executor, rocksdb::Env* env,
                                                              const std::string& filename, bool force_new) {
  asio::stream_file stream_file(executor);
  asio::file_base::flags open_flags = asio::random_access_file::read_write | asio::random_access_file::create;
  if (force_new) {
    open_flags |= asio::random_access_file::truncate;
  }
  asio::error_code ec;
  stream_file.open(filename, open_flags, ec);  // NOLINT(bugprone-unused-return-value)
  if (ec) {
    return new_error(ec, fmt::format("Failed to create WAL file {}: {}", filename, ec.message()));
  }

  rocksdb::FileLock* lock;
  if (auto s = env->LockFile(filename, &lock); !s.ok()) {
    return new_error(s, fmt::format("Failed to lock WAL file {}: {}", filename, s.ToString()));
  }

  return fileutil::env_file_endpoint{filename, std::move(stream_file), env, lock};
}

leaf::result<encoder> new_file_encoder(fileutil::env_file_endpoint& file, std::uint32_t prev_crc,
                                       std::shared_ptr<lepton::logger_interface>&& logger) {
  BOOST_LEAF_AUTO(offset, file.seek_curr());
  pro::proxy_view<ioutil::writer> writer = &file;
  return encoder(writer, prev_crc, static_cast<std::uint32_t>(offset), std::move(logger));
}

}  // namespace lepton::storage::wal
