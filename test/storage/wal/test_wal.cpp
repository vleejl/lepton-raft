#include <gtest/gtest.h>

#include "asio/use_future.hpp"
#include "basic/defer.h"
#include "basic/spdlog_logger.h"
#include "error/leaf_expected.h"
#include "storage/fileutil/path.h"
#include "storage/wal/wal.h"
#include "storage/wal/wal_file.h"
using namespace lepton::storage::fileutil;
using namespace lepton::storage::wal;

class wal_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    remove_all(wal_file_path);
    EXPECT_TRUE(std::filesystem::create_directory(wal_file_path));
    std::cout << "run before first case..." << std::endl;
  }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }

  constexpr static auto wal_file_path = "./tmpwal";
};

TEST_F(wal_test_suit, create_new_wal_file) {
  asio::io_context io_context;
  rocksdb::Env* env = rocksdb::Env::Default();
  std::filesystem::path wal_file_dir = wal_file_path;
  auto file_path = wal_file_dir / wal_file_name(0, 0);
  auto result = create_new_wal_file(io_context.get_executor(), env, file_path.string(), false);
  ASSERT_TRUE(result);
  io_context.run();
}

TEST_F(wal_test_suit, new_wal) {
  asio::io_context io_context;
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_result = co_await create_wal(env, io_context.get_executor(), "", "somedata",
                                              std::make_shared<lepton::spdlog_logger>());
        auto& wal_handle = wal_result.value();
        EXPECT_TRUE(wal_handle);

        auto tail_wal_file_handle = wal_handle->tail();
        EXPECT_TRUE(tail_wal_file_handle);
        EXPECT_FALSE(tail_wal_file_handle->name().empty());
        EXPECT_NE(tail_wal_file_handle->name(), wal_file_name(0, 0));
        DEFER(wal_handle->close());

        // file is preallocated to segment size; only read data written by wal
        auto seek_result = lepton::leaf_to_expected([&]() { return tail_wal_file_handle->seek_curr(); });
        EXPECT_TRUE(seek_result);
        auto seek_value = *seek_result;
        EXPECT_NE(seek_value, 0);
        co_return;
      },
      asio::use_future);
  io_context.run();
}
