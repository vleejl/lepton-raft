#pragma once
#ifndef _LEPTON_LOGGER_H_
#define _LEPTON_LOGGER_H_
#include <fmt/core.h>

#include <memory>
#include <source_location>
namespace lepton {

enum class log_level {
  trace = 0,
  debug = 1,
  info = 2,
  warn = 3,
  error = 4,
  critical = 5,
  off = 6,
};

class logger_interface {
 public:
  virtual ~logger_interface() = default;

  virtual bool should_log(log_level level) const = 0;

  //
  // --- 虚函数接口（固定类型，派生类实现）
  //
  virtual void trace_impl(std::string_view msg, std::source_location loc) = 0;
  virtual void debug_impl(std::string_view msg, std::source_location loc) = 0;
  virtual void info_impl(std::string_view msg, std::source_location loc) = 0;
  virtual void warn_impl(std::string_view msg, std::source_location loc) = 0;
  virtual void error_impl(std::string_view msg, std::source_location loc) = 0;
  virtual void critical_impl(std::string_view msg, std::source_location loc) = 0;

  //
  // --- 模板包装层：对外接口（使用 fmt::format + 默认 source_location）
  //
  template <typename... Args>
  void trace(fmt::format_string<Args...> fmt_str, Args&&... args,
             std::source_location loc = std::source_location::current()) {
    trace_impl(fmt::format(fmt_str, std::forward<Args>(args)...), loc);
  }

  template <typename... Args>
  void debug(fmt::format_string<Args...> fmt_str, Args&&... args,
             std::source_location loc = std::source_location::current()) {
    debug_impl(fmt::format(fmt_str, std::forward<Args>(args)...), loc);
  }

  template <typename... Args>
  void info(fmt::format_string<Args...> fmt_str, Args&&... args,
            std::source_location loc = std::source_location::current()) {
    info_impl(fmt::format(fmt_str, std::forward<Args>(args)...), loc);
  }

  template <typename... Args>
  void warn(fmt::format_string<Args...> fmt_str, Args&&... args,
            std::source_location loc = std::source_location::current()) {
    warn_impl(fmt::format(fmt_str, std::forward<Args>(args)...), loc);
  }

  template <typename... Args>
  void error(fmt::format_string<Args...> fmt_str, Args&&... args,
             std::source_location loc = std::source_location::current()) {
    error_impl(fmt::format(fmt_str, std::forward<Args>(args)...), loc);
  }

  template <typename... Args>
  void critical(fmt::format_string<Args...> fmt_str, Args&&... args,
                std::source_location loc = std::source_location::current()) {
    critical_impl(fmt::format(fmt_str, std::forward<Args>(args)...), loc);
  }
};

// ----------------------
// 宏 + 模板包装函数
// ----------------------

template <typename Logger, typename... Args>
inline void log_trace(Logger& l, std::source_location loc, fmt::format_string<Args...> fmt, Args&&... args) {
  l.trace_impl(fmt::format(fmt, std::forward<Args>(args)...), loc);
}

inline void log_trace(logger_interface& l, std::source_location loc, std::string_view msg) { l.trace_impl(msg, loc); }

template <typename Logger, typename... Args>
inline void log_debug(Logger& l, std::source_location loc, fmt::format_string<Args...> fmt, Args&&... args) {
  l.debug_impl(fmt::format(fmt, std::forward<Args>(args)...), loc);
}

inline void log_debug(logger_interface& l, std::source_location loc, std::string_view msg) { l.debug_impl(msg, loc); }

template <typename Logger, typename... Args>
inline void log_info(Logger& l, std::source_location loc, fmt::format_string<Args...> fmt, Args&&... args) {
  l.info_impl(fmt::format(fmt, std::forward<Args>(args)...), loc);
}

inline void log_info(logger_interface& l, std::source_location loc, std::string_view msg) { l.info_impl(msg, loc); }

template <typename Logger, typename... Args>
inline void log_warn(Logger& l, std::source_location loc, fmt::format_string<Args...> fmt, Args&&... args) {
  l.warn_impl(fmt::format(fmt, std::forward<Args>(args)...), loc);
}

inline void log_warn(logger_interface& l, std::source_location loc, std::string_view msg) { l.warn_impl(msg, loc); }

template <typename Logger, typename... Args>
inline void log_error(Logger& l, std::source_location loc, fmt::format_string<Args...> fmt, Args&&... args) {
  l.error_impl(fmt::format(fmt, std::forward<Args>(args)...), loc);
}

inline void log_error(logger_interface& l, std::source_location loc, std::string_view msg) { l.error_impl(msg, loc); }

template <typename Logger, typename... Args>
inline void log_critical(Logger& l, std::source_location loc, fmt::format_string<Args...> fmt, Args&&... args) {
  l.critical_impl(fmt::format(fmt, std::forward<Args>(args)...), loc);
}

inline void log_critical(logger_interface& l, std::source_location loc, std::string_view msg) {
  l.critical_impl(msg, loc);
}

template <typename T>
logger_interface& get_logger_ref(T& l) {
  return l;  // 普通对象或引用直接返回
}

template <typename T>
logger_interface& get_logger_ref(std::unique_ptr<T>& l) {
  return *l;  // unique_ptr 解引用
}

template <typename T>
logger_interface& get_logger_ref(std::shared_ptr<T>& l) {
  return *l;  // shared_ptr 解引用
}

#define LOGGER_CALL(logger_obj, level, ...)                               \
  do {                                                                    \
    auto& _logger = get_logger_ref(logger_obj);                           \
    if (_logger.should_log(::lepton::log_level::level)) {                 \
      log_##level(_logger, std::source_location::current(), __VA_ARGS__); \
    }                                                                     \
  } while (0)

#define LOGGER_TRACE(obj, ...) LOGGER_CALL(obj, trace, __VA_ARGS__)
#define LOGGER_DEBUG(obj, ...) LOGGER_CALL(obj, debug, __VA_ARGS__)
#define LOGGER_INFO(obj, ...) LOGGER_CALL(obj, info, __VA_ARGS__)
#define LOGGER_WARN(obj, ...) LOGGER_CALL(obj, warn, __VA_ARGS__)
#define LOGGER_ERROR(obj, ...) LOGGER_CALL(obj, error, __VA_ARGS__)
#define LOGGER_CRITICAL(obj, ...) LOGGER_CALL(obj, critical, __VA_ARGS__)

void set_default_logger(std::shared_ptr<logger_interface> l);

logger_interface& default_logger();

// ----------------------
// 全局宏定义 (无对象版)
// ----------------------
#define LOG_TRACE(...) LOGGER_TRACE(lepton::default_logger(), __VA_ARGS__)
#define LOG_DEBUG(...) LOGGER_DEBUG(lepton::default_logger(), __VA_ARGS__)
#define LOG_INFO(...) LOGGER_INFO(lepton::default_logger(), __VA_ARGS__)
#define LOG_WARN(...) LOGGER_WARN(lepton::default_logger(), __VA_ARGS__)
#define LOG_ERROR(...) LOGGER_ERROR(lepton::default_logger(), __VA_ARGS__)
#define LOG_CRITICAL(...) LOGGER_CRITICAL(lepton::default_logger(), __VA_ARGS__)

}  // namespace lepton

#endif  // _LEPTON_LOGGER_H_
