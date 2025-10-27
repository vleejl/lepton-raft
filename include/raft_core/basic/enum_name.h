#ifndef _LEPTON_LEPTON_MAGIC_ENUM_H_
#define _LEPTON_LEPTON_MAGIC_ENUM_H_
#include <string>

#include "magic_enum.hpp"
namespace lepton {
#ifdef LEPTON_TEST
inline std::string convert_enum_name(std::string_view enum_name) {
  std::string result;
  bool capitalize_next = true;  // 标记下一个字符是否需要大写

  for (char c : enum_name) {
    if (c == '_') {
      // 遇到下划线，标记下一个字符需要大写
      capitalize_next = true;
    } else {
      if (capitalize_next) {
        // 大写当前字符
        result += std::toupper(c);
        capitalize_next = false;
      } else {
        // 小写当前字符
        result += std::tolower(c);
      }
    }
  }
  return result;
}
#endif

template <typename T>
inline auto enum_name(T value) {
#ifdef LEPTON_TEST
  return convert_enum_name(magic_enum::enum_name(value));
#else
  return magic_enum::enum_name(value);
#endif
}
}  // namespace lepton

#endif  // _LEPTON_LEPTON_MAGIC_ENUM_H_
