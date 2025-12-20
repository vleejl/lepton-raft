#include "storage/wal/wal_directory_manager.h"

#include <filesystem>

#include "basic/defer.h"
#include "storage/fileutil/path.h"
#include "storage/fileutil/read_dir.h"
namespace lepton::storage::wal {

leaf::result<void> wal_directory_manager::is_dir_writeable(const std::string& dir_name) {
  std::filesystem::path dir_path(dir_name);
  std::filesystem::path touch_file_path = dir_path / ".touch";

  rocksdb::EnvOptions env_opts;
  std::unique_ptr<rocksdb::WritableFile> touch_file_handle;
  auto s = env_->NewWritableFile(touch_file_path, &touch_file_handle, env_opts);
  if (!s.ok()) {
    return new_error(s, fmt::format("Directory {} is not writable: {}", dir_name, s.ToString()));
  }
  s = env_->DeleteFile(touch_file_path);
  if (!s.ok()) {
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

leaf::result<void> wal_directory_manager::create_wal(const std::string& dirpath) {
  if (file_exist(dirpath)) {
    return new_error(std::make_error_code(std::errc::file_exists), fmt::format("dirpath {} already exists", dirpath));
  }

  // keep temporary wal directory so WAL initialization appears atomic
  auto temp_dir_path = std::filesystem::path{dirpath} / ".tmp";
  const auto temp_dir_path_str = temp_dir_path.string();
  if (file_exist(temp_dir_path_str)) {
    if (auto s = env_->DeleteDir(temp_dir_path_str); !s.ok()) {
      return new_error(s, fmt::format("Failed to delete existing temp dir {}: {}", temp_dir_path_str, s.ToString()));
    }
  }
  DEFER({ fileutil::remove_all(temp_dir_path_str); });

  LEPTON_LEAF_CHECK(create_dir_all(temp_dir_path_str));

  auto wal_file_path = temp_dir_path / wal_file_name(0, 0);
  BOOST_LEAF_AUTO(wal_file_handle, create_new_wal_file(executor_, env_, wal_file_path.string(), false));
  LEPTON_LEAF_CHECK(wal_file_handle.pre_allocate(SEGMENT_SIZE_BYTES, true));
  // TODO
}

}  // namespace lepton::storage::wal
