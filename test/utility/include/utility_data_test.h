#include <charconv>
#include <cstdint>
#include <sstream>
#include <string>
#include <string_view>

#include "error.h"

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