#pragma once
#ifndef _LEPTON_FAILPOINT_SYNC_H_
#define _LEPTON_FAILPOINT_SYNC_H_

#include <chrono>
#include <thread>

#include "failpoint_core.h"

namespace lepton::failpoint::sync {

inline void point(const char* name) {
  auto fp = decide(name);

  if (fp.act == action::panic) panic();

  if (fp.act == action::sleep && fp.value > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(fp.value));
  }
}

template <typename Ret>
inline Ret point_return(const char* name, Ret r) {
  auto fp = decide(name);

  if (fp.act == action::panic) panic();
  if (fp.act == action::ret) return r;

  if (fp.act == action::sleep && fp.value > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(fp.value));
  }

  return r;  // 不触发 failpoint 时无影响
}

#define FAILPOINT_SYNC(name) ::lepton::failpoint::sync::point(#name)

#define FAILPOINT_SYNC_RETURN(name, ret) return ::lepton::failpoint::sync::point_return(#name, ret)

}  // namespace lepton::failpoint::sync

#endif  // _LEPTON_FAILPOINT_SYNC_H_
