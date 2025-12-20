#pragma once
#ifndef _LEPTON_TEST_CLI_H_
#define _LEPTON_TEST_CLI_H_

#include <fmt/format.h>

#include <cassert>
#include <cctype>
#include <charconv>
#include <cstddef>
#include <string>
#include <system_error>
#include <type_traits>
#include <vector>

#include "error/leaf.h"
#include "error/lepton_error.h"
#include "fmt/format.h"

lepton::leaf::result<void> handle_bool(const std::string& val, bool& dest);

class cmd_arg {
 public:
  std::string key_;
  std::vector<std::string> vals_;

  // 主扫描函数模板
  template <typename T>
  lepton::leaf::result<void> scan_err(std::size_t i, T& dest) const {
    // 修复：移除 i < 0 检查（无符号整数不可能小于0）
    if (i >= vals_.size()) {
      return lepton::new_error(lepton::logic_error::OUT_OF_BOUNDS,
                               fmt::format("index {} out of range for key {}", i, key_));
    }

    const std::string& val = vals_[i];

    // 根据目标类型分发处理
    if constexpr (std::is_same_v<T, std::string>) {
      dest = val;
    } else if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
      // 修复：排除布尔类型
      return handle_integer(val, dest);
    } else if constexpr (std::is_same_v<T, bool>) {
      return handle_bool(val, dest);
    } else {
      return lepton::new_error(lepton::logic_error::INVALID_PARAM,
                               fmt::format("unsupported type for destination #{} (might be easy to add it)", i + 1));
    }

    return {};  // 成功返回空结果
  }

 private:
  // 整数类型处理（支持有符号/无符号，自动检测位宽）
  template <typename T>
  lepton::leaf::result<void> handle_integer(const std::string& val, T& dest) const {
    // 检查是否为有效整数格式
    if (val.empty() || (!std::isdigit(static_cast<unsigned char>(val[0])) && val[0] != '-' && val[0] != '+')) {
      return lepton::new_error(lepton::logic_error::INVALID_PARAM, "invalid integer format");
    }

    // 使用charconv进行高性能转换
    T result;
    auto [ptr, ec] = std::from_chars(val.data(), val.data() + val.size(), result);

    if (ec != std::errc()) {
      return lepton::new_error(lepton::logic_error::INVALID_PARAM, "integer conversion failed");
    }

    if (ptr != val.data() + val.size()) {
      return lepton::new_error(lepton::logic_error::INVALID_PARAM, "extra characters after integer");
    }

    dest = result;
    return {};
  }
};

std::vector<std::string> get_test_files(const std::string& dir);
std::vector<cmd_arg> parse_command_line(const std::string& cmd, const std::string& line);

#endif  // _LEPTON_TEST_CLI_H_
