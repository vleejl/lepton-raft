#pragma once
#ifndef _LEPTON_LEPTON_MAGIC_ENUM_H_
#define _LEPTON_LEPTON_MAGIC_ENUM_H_
#include <string>

#include "magic_enum.hpp"
namespace lepton {
#ifdef LEPTON_TEST
inline std::string convert_enum_name(std::string_view enum_name) {
  std::string result;
  result.reserve(enum_name.size());  // 优化：预留空间减少内存分配次数
  bool capitalize_next = true;

  for (char c : enum_name) {
    if (c == '_') {
      capitalize_next = true;
    } else {
      // 关键：先转换为 unsigned char 保证安全，再转换回 char 消除警告
      unsigned char uc = static_cast<unsigned char>(c);
      if (capitalize_next) {
        result += static_cast<char>(std::toupper(uc));
        capitalize_next = false;
      } else {
        result += static_cast<char>(std::tolower(uc));
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
