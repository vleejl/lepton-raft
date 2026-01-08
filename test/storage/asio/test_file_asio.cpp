#include <gtest/gtest.h>
#include <raft.pb.h>
#include <rocksdb/env.h>
#include <rocksdb/status.h>

#include <asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/io_context.hpp>
#include <asio/random_access_file.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/use_future.hpp>
#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <system_error>
#include <tl/expected.hpp>

#include "asio/cancellation_signal.hpp"
#include "asio/detached.hpp"
#include "asio/error_code.hpp"
#include "coroutine/channel.h"
#include "coroutine/channel_endpoint.h"
#include "coroutine/signal_channel_endpoint.h"
#include "error/error.h"
#include "error/expected.h"
#include "error/leaf.h"
#include "error/raft_error.h"
#include "error/rocksdb_err.h"
#include "error/storage_error.h"
#include "fmt/format.h"
#include "spdlog/spdlog.h"
#include "storage/fileutil/path.h"

using asio::awaitable;
using asio::co_spawn;
using asio::detached;
using asio::io_context;
using asio::steady_timer;
using asio::use_awaitable;
using asio::experimental::make_parallel_group;

class asio_file_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

awaitable<void> random_async_write_file(asio::any_io_executor ex) {
  asio::stream_file stream_file(ex);
  // 打开一个文件
  asio::random_access_file file(ex);

  constexpr auto filename = "test.txt";
  lepton::storage::fileutil::remove(filename);
  // 打开（可选 flags：read_only, read_write, append 等）
  file.open(filename, asio::random_access_file::read_write | asio::random_access_file::create);

  // 写入一些数据
  std::string data = "Hello, Asio file!\n";
  std::size_t bytes = co_await file.async_write_some_at(0, asio::buffer(data));
  std::cout << "wrote " << bytes << " bytes\n";

  file.sync_data();

  // 读出数据
  std::array<char, 64> buf{};
  bytes = co_await file.async_read_some_at(0, asio::buffer(buf));
  std::cout << "read: " << std::string_view(buf.data(), bytes) << "\n";

  EXPECT_EQ(data, std::string_view(buf.data(), bytes));
  // 关闭文件
  file.close();
  co_return;
}

TEST(asio_file_test_suit, test_random_file) {
  asio::io_context io;
  asio::co_spawn(io, random_async_write_file(io.get_executor()), asio::detached);
  io.run();
}