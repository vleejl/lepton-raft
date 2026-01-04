#include "storage/fileutil/path.h"

#include <fmt/format.h>

#include <filesystem>
#include <string>
#include <system_error>
#ifndef _WIN32
#include <unistd.h>  // POSIX 平台必须，用于 ftruncate
#endif
#include "error/lepton_error.h"

namespace fs = std::filesystem;

namespace lepton::storage::fileutil {

bool path_exist(const std::string& path) {
  std::error_code ec;

  // 检查路径是否存在
  if (!fs::exists(path, ec)) {
    return false;
  }
  return true;
}

leaf::result<std::size_t> file_size(const std::string& path) {
  if (!path_exist(path)) {
    return new_error(std::make_error_code(std::errc::no_such_file_or_directory),
                     fmt::format("Path {} does not exist", path));
  }
  std::error_code ec;
  auto size = fs::file_size(path, ec);
  if (ec) {
    return new_error(ec, fmt::format("Failed to get file size of {}: {}", path, ec.message()));
  }
  return size;
}

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

std::string base_name(const std::string& path_str) {
  if (path_str.empty()) return ".";

  fs::path p(path_str);

  // 处理末尾带斜杠的情况，例如 "a/b/"，Go 的 Base 返回 "b"
  // fs::path 如果末尾是斜杠，filename() 可能返回空
  if (p.has_filename()) {
    return p.filename().string();
  } else if (p.has_parent_path()) {
    return p.parent_path().filename().string();
  }
  return p.string();
}

std::error_code truncate([[maybe_unused]] native_handle_t fd, [[maybe_unused]] const std::string& path,
                         std::uint64_t length) {
#ifndef _WIN32
  // --- POSIX 平台 ---
  // 直接操作 FD，即使目录被重命名也能准确截断
  if (::ftruncate(fd, static_cast<off_t>(length)) != 0) {
    return std::error_code(errno, std::system_category());
  }
  return {};
#else
  // --- Windows 平台 ---
  // 使用标准库。注意：如果此时目录被重命名，此调用可能会失败
  std::error_code ec;
  std::filesystem::resize_file(path, length, ec);
  return ec;
#endif
}

}  // namespace lepton::storage::fileutil
