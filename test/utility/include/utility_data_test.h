#include <charconv>
#include <cstdint>
#include <string>

#include "error.h"

inline lepton::leaf::result<std::uint64_t> safe_stoull(const std::string& str) {
  std::uint64_t result = 0;
  auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), result);
  if (ec != std::errc{}) {
    return lepton::leaf::new_error("can not parse");
  }
  if (ptr != str.data() + str.size()) {
    return lepton::leaf::new_error("can not parse");
  }
  return result;
}
