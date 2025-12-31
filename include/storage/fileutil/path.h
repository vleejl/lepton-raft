#pragma once
#include <cstddef>
#ifndef _LEPTON_PATH_H_
#define _LEPTON_PATH_H_
#include <filesystem>

#include "error/leaf.h"
namespace lepton::storage::fileutil {

bool path_exist(const std::string& path);

leaf::result<std::size_t> file_size(const std::string& path);

leaf::result<void> remove_all(const std::string& path);

leaf::result<void> rename(const std::string& old_path, const std::string& new_path);

std::string base_name(const std::string& path_str);

template <typename... Args>
std::string join_paths(Args... args) {
  namespace fs = std::filesystem;
  fs::path result;
  // 利用折叠表达式 (C++17) 依次连接路径
  ((result /= fs::path(args)), ...);
  return result.string();
}
}  // namespace lepton::storage::fileutil

#endif  // _LEPTON_PATH_H_
