
#include "storage/fileutil/read_dir.h"

#include <fmt/format.h>

#include <algorithm>
#include <filesystem>

#include "basic/logger.h"
#include "error/error.h"
namespace fs = std::filesystem;

namespace lepton::storage::fileutil {

leaf::result<std::vector<std::string>> read_dir_with_opts(const std::string& directory,
                                                          const std::vector<read_dir_op::option>& opts) {
  // 应用选项
  read_dir_op op;
  op.apply_opts(opts);

  std::vector<std::string> files;

  // 检查目录是否存在
  if (!fs::exists(directory) || !fs::is_directory(directory)) {
    LOG_ERROR("directory: {} does not exist or is not a directory", directory);
    return new_error(std::make_error_code(std::errc::no_such_file_or_directory),
                     fmt::format("directory: {} does not exist or is not a director", directory));
  }

  // 遍历目录
  for (const auto& entry : fs::directory_iterator(directory)) {
    std::string filename = entry.path().filename().string();
    bool include = true;

    // 扩展名过滤
    if (!op.ext().empty()) {
      if (entry.path().extension() != op.ext()) {
        include = false;
      }
    }

    // 前缀过滤
    if (!op.prefix().empty()) {
      if (filename.find(op.prefix()) != 0) {
        include = false;
      }
    }

    if (include) {
      files.push_back(filename);
    }
  }

  // 排序
  if (op.should_sort()) {
    std::sort(files.begin(), files.end());
  }

  return files;
}

}  // namespace lepton::storage::fileutil
