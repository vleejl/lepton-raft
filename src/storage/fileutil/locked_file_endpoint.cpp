#include "storage/fileutil/locked_file_endpoint.h"

#include <asio/any_io_executor.hpp>
#include <memory>

#include "basic/logger.h"
#include "error/error.h"
#include "error/io_error.h"
#include "error/rocksdb_err.h"  // IWYU pragma: keep
#include "storage/fileutil/file_endpoint.h"
#include "storage/fileutil/path.h"

namespace lepton::storage::fileutil {

leaf::result<locked_file_endpoint_handle> create_locked_file_endpoint(rocksdb::Env* env, file_endpoint&& base,
                                                                      const std::string& filename) {
  rocksdb::FileLock* lock;
  if (auto s = env->LockFile(filename, &lock); !s.ok()) {
    LOG_ERROR("Failed to lock WAL file {}: {}", filename, s.ToString());
    return new_error(s, fmt::format("Failed to lock WAL file {}: {}", filename, s.ToString()));
  }
  return std::make_unique<locked_file_endpoint>(std::move(base), env, lock);
}

leaf::result<locked_file_endpoint_handle> create_locked_file_endpoint(rocksdb::Env* env, asio::any_io_executor executor,
                                                                      const std::string& filename,
                                                                      asio::file_base::flags open_flags) {
  if (!fileutil::path_exist(filename)) {
    LOG_ERROR("file: {} not exist", filename);
    return new_error(io_error::PARH_NOT_EXIT);
  }
  std::error_code ec;
  asio::stream_file file_stream(executor);
  ec = file_stream.open(filename, open_flags, ec);
  if (ec) {
    LOG_ERROR("Failed to re-open file {} after permission change: {}", filename, ec.message());
    return new_error(ec, "Failed to re-open file after permission change");
  }
  return create_locked_file_endpoint(env, file_endpoint{filename, std::move(file_stream)}, filename);
}
}  // namespace lepton::storage::fileutil