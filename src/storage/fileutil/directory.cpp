#include "storage/fileutil/directory.h"

#include <fmt/format.h>

#include <filesystem>

#include "basic/spdlog_logger.h"
#include "error/lepton_error.h"
#include "storage/fileutil/path.h"
#include "storage/fileutil/read_dir.h"

namespace lepton::storage::fileutil {

leaf::result<void> directory::fsync() {
  assert(dir_ != nullptr);
  auto s = dir_->Fsync();
  if (s.ok()) {
    return {};
  }
  return new_error(s);
}

leaf::result<void> directory::close() {
  assert(dir_ != nullptr);
  auto s = dir_->Close();
  if (s.ok()) {
    return {};
  }
  return new_error(s);
}

leaf::result<void> directory::is_dir_writeable(const std::string& dir_name) {
  std::filesystem::path dir_path(dir_name);
  std::filesystem::path touch_file_path = dir_path / ".touch";

  rocksdb::EnvOptions env_opts;
  std::unique_ptr<rocksdb::WritableFile> touch_file_handle;

  if (auto s = env_->NewWritableFile(touch_file_path, &touch_file_handle, env_opts); !s.ok()) {
    return new_error(s, fmt::format("Directory {} is not writable: {}", dir_name, s.ToString()));
  }

  LEPTON_LEAF_CHECK(fileutil::remove_all(touch_file_path));
  return {};
}

leaf::result<directory> new_directory(rocksdb::Env* env, const std::string& path) {
  assert(env != nullptr);
  std::unique_ptr<rocksdb::Directory> dir;
  auto s = env->NewDirectory(path, &dir);
  if (s.ok()) {
    return directory{env, std::move(dir)};
  }
  return new_error(s);
}

leaf::result<void> is_dir_writeable(rocksdb::Env* env, const std::string& dir_name) {
  BOOST_LEAF_AUTO(dir_handle, fileutil::new_directory(env, dir_name));
  return dir_handle.is_dir_writeable(dir_name);
}

leaf::result<void> ensure_directory_writable(rocksdb::Env* env, const std::string& dir_name) {
  auto s = env->CreateDirIfMissing(dir_name);
  if (!s.ok()) {
    LOG_ERROR("Failed to create directory {}: {}", dir_name, s.ToString());
    return new_error(s, fmt::format("Failed to create directory:{}, error:{}", dir_name, s.ToString()));
  }
  return is_dir_writeable(env, dir_name);
}

leaf::result<void> create_dir_all(rocksdb::Env* env, const std::string& dir_name) {
  LEPTON_LEAF_CHECK(ensure_directory_writable(env, dir_name));
  BOOST_LEAF_AUTO(files, fileutil::read_dir(dir_name));
  if (!files.empty()) {
    return new_error(std::make_error_code(std::errc::directory_not_empty),
                     fmt::format("Directory {} is not empty", dir_name));
  }
  return {};
}

}  // namespace lepton::storage::fileutil
