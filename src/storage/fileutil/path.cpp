#include "storage/fileutil/path.h"

#include <fmt/format.h>

#include <filesystem>
#include <system_error>

#include "error/lepton_error.h"

namespace fs = std::filesystem;

namespace lepton::storage::fileutil {
leaf::result<void> remove_all(const std::string& path) {
  std::error_code ec;

  // 检查路径是否存在
  if (!fs::exists(path, ec)) {
    return {};
  }

  // 递归删除目录或文件
  uintmax_t /*removed_count*/ _ = fs::remove_all(path, ec);

  if (ec) {
    return new_error(ec, fmt::format("Failed to remove path {}: {}", path, ec.message()));
  }

  return {};
}

leaf::result<void> rename(const std::string& old_path, const std::string& new_path) {
  if (old_path == new_path) {
    return {};
  }
  if (!fs::exists(old_path)) {
    return new_error(std::make_error_code(std::errc::no_such_file_or_directory),
                     fmt::format("Source path {} does not exist", old_path));
  }
  if (fs::exists(new_path)) {
    return new_error(std::make_error_code(std::errc::file_exists),
                     fmt::format("Destination path {} already exists", new_path));
  }

  std::error_code ec;

  fs::rename(old_path, new_path, ec);

  if (ec) {
    return new_error(ec, fmt::format("Failed to rename from {} to {}: {}", old_path, new_path, ec.message()));
  }

  return {};
}

}  // namespace lepton::storage::fileutil
