#pragma once

#include <fmt/format.h>

#include <array>
#include <cassert>
#include <cstddef>
#include <source_location>
#include <sstream>
#include <string>
#include <string_view>

#include "basic/logger.h"  // 你前面定义的 logger_interface

namespace interaction {

class redirect_logger final : public lepton::logger_interface {
 public:
  enum level { trace = 0, debug, info, warn, error, fatal, none };

  bool should_log(lepton::log_level l) const override { return static_cast<level>(l) >= lvl_; }

  inline static std::array<std::string, 7> lvl_names = {"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "NONE"};

  redirect_logger(level lvl, std::ostringstream* buffer) : lvl_(lvl), buffer_(buffer) {}

  void set_level(level lvl) noexcept { lvl_ = lvl; }
  [[nodiscard]] level get_level() const noexcept { return lvl_; }

  [[nodiscard]] std::string str() const { return buffer_->str(); }

  void clear() {
    if (buffer_ == nullptr) {
      return;
    }
    buffer_->str({});
    buffer_->clear();
  }

  // --- 实现虚函数接口 ---
  void trace_impl(std::string_view msg, std::source_location loc) override { print(trace, msg, loc); }

  void debug_impl(std::string_view msg, std::source_location loc) override { print(debug, msg, loc); }

  void info_impl(std::string_view msg, std::source_location loc) override { print(info, msg, loc); }

  void warn_impl(std::string_view msg, std::source_location loc) override { print(warn, msg, loc); }

  void error_impl(std::string_view msg, std::source_location loc) override { print(error, msg, loc); }

  void critical_impl(std::string_view msg, std::source_location loc) override { print(fatal, msg, loc); }

  void set_builder(std::ostringstream* buf) { buffer_ = buf; }

  std::ostringstream* get_builder() const { return buffer_; }

  bool quiet() const {
    if (lvl_ == none) {
      return true;
    }
    return false;
  }

  void write_string(std::string_view msg) {
    if (quiet()) {
      return;
    }
    assert(buffer_ != nullptr);
    *buffer_ << msg;
  }

 private:
  void print(level msg_level, std::string_view msg, std::source_location loc) {
    if (lvl_ > msg_level || lvl_ == none) {
      return;  // 静音模式
    }

    *buffer_ << fmt::format("{} {}", lvl_names[static_cast<std::size_t>(msg_level)], msg);
    if (!msg.ends_with('\n')) {
      *buffer_ << '\n';
    }
  }

  level lvl_;
  std::ostringstream* buffer_;
};

}  // namespace interaction
