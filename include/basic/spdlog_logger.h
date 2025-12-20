#pragma once
#ifndef _LEPTON_SPDLOG_LOGGER_H_
#define _LEPTON_SPDLOG_LOGGER_H_
#include <spdlog/spdlog.h>

#include "basic/logger.h"
namespace lepton {

class spdlog_logger : public logger_interface {
 public:
  void trace_impl(std::string_view msg, std::source_location loc) override {
    spdlog::trace("[{}:{}] {}", loc.file_name(), loc.line(), msg);
  }
  void debug_impl(std::string_view msg, std::source_location loc) override {
    spdlog::debug("[{}:{}] {}", loc.file_name(), loc.line(), msg);
  }
  void info_impl(std::string_view msg, std::source_location loc) override {
    spdlog::info("[{}:{}] {}", loc.file_name(), loc.line(), msg);
  }
  void warn_impl(std::string_view msg, std::source_location loc) override {
    spdlog::warn("[{}:{}] {}", loc.file_name(), loc.line(), msg);
  }
  void error_impl(std::string_view msg, std::source_location loc) override {
    spdlog::error("[{}:{}] {}", loc.file_name(), loc.line(), msg);
  }
  void critical_impl(std::string_view msg, std::source_location loc) override {
    spdlog::critical("[{}:{}] {}", loc.file_name(), loc.line(), msg);
  }
};

}  // namespace lepton

#endif  // _LEPTON_SPDLOG_LOGGER_H_
