#pragma once
#ifndef _LEPTON_TIME_H_
#define _LEPTON_TIME_H_
#include <chrono>
namespace lepton {

inline auto time_since(const std::chrono::steady_clock::time_point &start) {
  return std::chrono::steady_clock::now() - start;
}

inline std::int64_t to_seconds(std::chrono::steady_clock::duration d) {
  return std::chrono::duration_cast<std::chrono::seconds>(d).count();
}

}  // namespace lepton

#endif  // _LEPTON_TIME_H_
