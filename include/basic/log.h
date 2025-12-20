#pragma once
#ifndef _LEPTON_LOG_H_
#define _LEPTON_LOG_H_
#include <spdlog/spdlog.h>

// 调试断点（跨平台）
#if defined(_MSC_VER)
#define LEPTON_DEBUG_BREAK() __debugbreak()
#elif defined(__GNUC__) || defined(__clang__)
#define LEPTON_DEBUG_BREAK() __builtin_trap()
#else
#define LEPTON_DEBUG_BREAK() std::abort()
#endif

// 分层终止宏
#ifdef NDEBUG
#define LEPTON_CRITICAL(...)      \
  do {                            \
    SPDLOG_CRITICAL(__VA_ARGS__); \
    std::abort();                 \
  } while (0)
#else
#define LEPTON_CRITICAL(...)      \
  do {                            \
    SPDLOG_CRITICAL(__VA_ARGS__); \
    LEPTON_DEBUG_BREAK();         \
  } while (0)
#endif

namespace lepton {}  // namespace lepton

#endif  // _LEPTON_LOG_H_
