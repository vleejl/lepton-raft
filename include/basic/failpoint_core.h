#pragma once
#ifndef _LEPTON_FAILPOINT_CORE_H_
#define _LEPTON_FAILPOINT_CORE_H_

#include <cstdlib>
#include <cstring>

namespace lepton::failpoint {

enum class action {
  none,
  panic,
  ret,
  sleep,
  errno_inject,
};

struct decision {
  action act{action::none};
  int value{0};  // sleep ms or errno
};

inline decision parse(const char* v) noexcept {
  if (!v || !*v) return {};

  if (std::strcmp(v, "panic") == 0) return {action::panic, 0};
  if (std::strcmp(v, "return") == 0) return {action::ret, 0};

  if (std::strncmp(v, "sleep(", 6) == 0) {
    return {action::sleep, std::atoi(v + 6)};
  }

  if (std::strncmp(v, "errno(", 6) == 0) {
    return {action::errno_inject, std::atoi(v + 6)};
  }

  return {};
}

inline decision decide(const char* name) noexcept {
#ifndef NDEBUG
  return parse(std::getenv(name));
#else
  (void)name;
  return {};
#endif
}

[[noreturn]] inline void panic() { std::abort(); }

}  // namespace lepton::failpoint

#endif  // _LEPTON_FAILPOINT_CORE_H_
