#include "path.h"

#include <fmt/format.h>

#include <filesystem>
#include <system_error>

#include "lepton_error.h"

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

}  // namespace lepton::storage::fileutil
