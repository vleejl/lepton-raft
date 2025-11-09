#include "test_file.h"

#include <iostream>

void delete_if_exists(const fs::path& file_path) {
  try {
    if (fs::exists(file_path)) {
      fs::remove(file_path);
    }
  } catch (const fs::filesystem_error& e) {
    std::cerr << "文件操作错误: " << e.what() << std::endl;
  }
}
