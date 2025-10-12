#include <gtest/gtest.h>
#include <proxy.h>
#include <spdlog/spdlog.h>

#include <memory>
#include <source_location>

#include "spdlog_logger.h"

class spdlog_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

TEST_F(spdlog_test_suit, basic_use) {
  //   spdlog::set_pattern("[%H:%M:%S %z] [%g] [%l] %v [%@]");
  //   spdlog::set_pattern("[%H:%M:%S %z] [%n] [%^---%L---%$] [thread %t] %v");
  //   spdlog::set_pattern("[source %s] [function %!] [line %#] %v");
  {
    std::unique_ptr<lepton::logger_interface> log = std::make_unique<lepton::spdlog_logger>();
    LOG_TRACE(log, "peer {} connected", 42);
    LOG_DEBUG(log, "peer {} connected", 42);
    LOG_WARN(log, "peer {} connected", 42);
    LOG_ERROR(log, "peer {} connected", 42);
    LOG_CRITICAL(log, "peer {} connected", 42);
  }
  {
    std::shared_ptr<lepton::logger_interface> log = std::make_shared<lepton::spdlog_logger>();
    LOG_TRACE(log, "peer {} connected", 42);
    LOG_DEBUG(log, "peer {} connected", 42);
    LOG_WARN(log, "peer {} connected", 42);
    LOG_ERROR(log, "peer {} connected", 42);
    LOG_CRITICAL(log, "peer {} connected", 42);
  }
  {
    lepton::spdlog_logger slog;
    lepton::logger_interface &log = slog;
    LOG_TRACE(log, "peer {} connected", 42);
    LOG_DEBUG(log, "peer {} connected", 42);
    LOG_WARN(log, "peer {} connected", 42);
    LOG_ERROR(log, "peer {} connected", 42);
    LOG_CRITICAL(log, "peer {} connected", 42);
  };

  spdlog::info("Hello, {}!", "world");
  SPDLOG_INFO("Welcome to spdlog!");
}