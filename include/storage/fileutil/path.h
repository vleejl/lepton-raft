#pragma once
#ifndef _LEPTON_PATH_H_
#define _LEPTON_PATH_H_
#include <cstddef>
#include <filesystem>

#include "error/leaf.h"
#include "storage/fileutil/types.h"
namespace lepton::storage::fileutil {

bool path_exist(const std::string& path);

leaf::result<std::size_t> file_size(const std::string& path);

leaf::result<void> remove(const std::string& path);

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

/**
 * @brief 跨平台截断逻辑
 * @param fd 文件原生描述符 (POSIX 为 int)
 * @param path 文件路径 (Windows 平台标准库使用)
 * @param length 目标截断长度
 */
std::error_code truncate([[maybe_unused]] native_handle_t fd, const std::string& path, std::uint64_t length);
}  // namespace lepton::storage::fileutil

#endif  // _LEPTON_PATH_H_
