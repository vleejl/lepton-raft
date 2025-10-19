#ifndef _LEPTON_TEST_UTILITY_DATA_H_
#define _LEPTON_TEST_UTILITY_DATA_H_

#include <charconv>
#include <cstdint>
#include <random>
#include <sstream>
#include <string>
#include <string_view>

#include "absl/strings/str_replace.h"
#include "leaf.h"

template <typename T>
lepton::leaf::result<std::uint64_t> safe_stoull(
    const T& str,
    typename std::enable_if<std::is_same<T, std::string>::value || std::is_same<T, std::string_view>::value>::type* =
        nullptr) {
  std::uint64_t result = 0;

  // 使用 std::from_chars 来解析字符串
  auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), result);

  // 错误处理
  if (ec != std::errc{}) {
    return lepton::leaf::new_error("can not parse");
  }
  if (ptr != str.data() + str.size()) {
    return lepton::leaf::new_error("can not parse");
  }

  return result;
}

inline bool string_to_bool(const std::string& str) {
  std::istringstream iss(str);
  // std::boolalpha 使得 "true"/"false" 被正确解析为布尔值
  bool result;
  iss >> std::boolalpha >> result;
  return result;
}

inline double rand_float64() {
  // 使用梅森旋转算法作为随机引擎，64位版本提供更高的精度
  static thread_local std::mt19937_64 generator(std::random_device{}());
  // 定义均匀分布，范围 [0.0, 1.0)
  static thread_local std::uniform_real_distribution<double> distribution(0.0, 1.0);
  return distribution(generator);
}

inline std::string remove_new_lines(const std::string& input) {
  return absl::StrReplaceAll(input, {
                                        {"\n", ""},   // 替换 LF (Unix 换行)
                                        {"\r", ""},   // 替换 CR (Mac 换行)
                                        {"\r\n", ""}  // 替换 CRLF (Windows 换行)
                                    });
}

inline void ensure_new_line(std::string& str) {
  // 检查字符串中是否包含换行符 '\n'
  bool has_newline = (str.find('\n') != std::string::npos);

  // 如果没有找到换行符，在末尾添加一个
  if (!has_newline) {
    str += '\n';
  }
}

inline const std::error_code EC_SUCCESS;

#endif  // _LEPTON_TEST_UTILITY_DATA_H_
