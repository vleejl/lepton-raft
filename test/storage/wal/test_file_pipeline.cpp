#include <gtest/gtest.h>

#include "basic/spdlog_logger.h"
#include "storage/fileutil/path.h"
#include "storage/wal/file_pipeline.h"
#include "storage/wal/wal.h"

using namespace lepton::storage::fileutil;
using namespace lepton::storage::wal;

class file_pipeline_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    remove_all(wal_file_path);
    EXPECT_TRUE(std::filesystem::create_directory(wal_file_path));
    std::cout << "run before first case..." << std::endl;
  }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }

  constexpr static auto wal_file_path = "./file_pipeline_tmpwal";
};

TEST_F(file_pipeline_test_suit, open_close) {
  asio::io_context io_context;
  rocksdb::Env *env = rocksdb::Env::Default();
  auto handle_result = new_file_pipeline(io_context.get_executor(), env, wal_file_path, SEGMENT_SIZE_BYTES,
                                         std::make_unique<lepton::spdlog_logger>());
  ASSERT_TRUE(handle_result);
  auto handle = std::move(handle_result.value());

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        LOG_INFO("ready to open file.......");
        auto file_result = co_await handle->open();
        EXPECT_TRUE(file_result.has_value());
        LOG_INFO("success open file.......");
        co_await handle->close();
        LOG_INFO("file pipeline finished.......");
        co_return;
      },
      asio::detached);
  io_context.run();
}

TEST_F(file_pipeline_test_suit, test_file_pipeline_fail_preallocate) {
  asio::io_context io_context;
  rocksdb::Env *env = rocksdb::Env::Default();
  const auto max_file_size = std::numeric_limits<std::uint64_t>::max();
  auto handle_result = new_file_pipeline(io_context.get_executor(), env, wal_file_path, max_file_size,
                                         std::make_unique<lepton::spdlog_logger>());
  ASSERT_TRUE(handle_result);
  auto handle = std::move(handle_result.value());

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        LOG_INFO("ready to open file.......");
        auto file_result = co_await handle->open();
        EXPECT_FALSE(file_result.has_value());
        LOG_INFO("open file finshed.......");
        co_await handle->close();
        co_return;
      },
      asio::detached);
  io_context.run();
}