#ifndef _LEPTON_LOGGER_H_
#define _LEPTON_LOGGER_H_
#include <fmt/core.h>

#include <memory>
#include <source_location>
namespace lepton {

class logger {
 public:
  virtual ~logger() = default;

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

template <typename Logger, typename... Args>
inline void log_debug(Logger& l, std::source_location loc, fmt::format_string<Args...> fmt, Args&&... args) {
  l.debug_impl(fmt::format(fmt, std::forward<Args>(args)...), loc);
}

template <typename Logger, typename... Args>
inline void log_info(Logger& l, std::source_location loc, fmt::format_string<Args...> fmt, Args&&... args) {
  l.info_impl(fmt::format(fmt, std::forward<Args>(args)...), loc);
}

template <typename Logger, typename... Args>
inline void log_warn(Logger& l, std::source_location loc, fmt::format_string<Args...> fmt, Args&&... args) {
  l.warn_impl(fmt::format(fmt, std::forward<Args>(args)...), loc);
}

template <typename Logger, typename... Args>
inline void log_error(Logger& l, std::source_location loc, fmt::format_string<Args...> fmt, Args&&... args) {
  l.error_impl(fmt::format(fmt, std::forward<Args>(args)...), loc);
}

template <typename Logger, typename... Args>
inline void log_critical(Logger& l, std::source_location loc, fmt::format_string<Args...> fmt, Args&&... args) {
  l.critical_impl(fmt::format(fmt, std::forward<Args>(args)...), loc);
}

template <typename T>
logger& get_logger_ref(T& l) {
  return l;  // 普通对象或引用直接返回
}

template <typename T>
logger& get_logger_ref(std::unique_ptr<T>& l) {
  return *l;  // unique_ptr 解引用
}

template <typename T>
logger& get_logger_ref(std::shared_ptr<T>& l) {
  return *l;  // shared_ptr 解引用
}

#define LOG_TRACE(logger_obj, ...) log_trace(get_logger_ref(logger_obj), std::source_location::current(), __VA_ARGS__)

#define LOG_DEBUG(logger_obj, ...) log_debug(get_logger_ref(logger_obj), std::source_location::current(), __VA_ARGS__)

#define LOG_INFO(logger_obj, ...) log_info(get_logger_ref(logger_obj), std::source_location::current(), __VA_ARGS__)

#define LOG_WARN(logger_obj, ...) log_warn(get_logger_ref(logger_obj), std::source_location::current(), __VA_ARGS__)

#define LOG_ERROR(logger_obj, ...) log_error(get_logger_ref(logger_obj), std::source_location::current(), __VA_ARGS__)

#define LOG_CRITICAL(logger_obj, ...) \
  log_critical(get_logger_ref(logger_obj), std::source_location::current(), __VA_ARGS__)

}  // namespace lepton

#endif  // _LEPTON_LOGGER_H_
