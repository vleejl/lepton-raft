#include <gtest/gtest.h>
#include <rocksdb/env.h>
#include <rocksdb/status.h>

#include <asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/io_context.hpp>
#include <asio/random_access_file.hpp>
#include <asio/use_awaitable.hpp>
#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <system_error>

#include "asio/cancellation_signal.hpp"
#include "asio/detached.hpp"
#include "asio/error_code.hpp"
#include "asio/use_future.hpp"
#include "channel.h"
#include "channel_endpoint.h"
#include "expected.h"
#include "fmt/format.h"
#include "leaf.h"
#include "lepton_error.h"
#include "raft.pb.h"
#include "raft_error.h"
#include "rocksdb_err.h"
#include "signal_channel_endpoint.h"
#include "spdlog/spdlog.h"
#include "storage_error.h"
#include "test_file.h"
#include "tl/expected.hpp"
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

class writable_file {
 public:
  writable_file(rocksdb::Env* env) : env_(env) {}

 private:
  rocksdb::Env* env_;
  rocksdb::EnvOptions env_opts_;
  std::unique_ptr<rocksdb::WritableFile> file_handle_;
};

class file_helper {
 public:
  bool file_exist(const std::string& file_name) { return env_->FileExists(file_name).ok(); }

  lepton::leaf::result<void> create_dir_all(const std::string& dir_name) {
    auto tmp_dir = fmt::format("{}.tmp", dir_name);
    if (file_exist(tmp_dir)) {
      env_->DeleteDir(tmp_dir);
    }

    rocksdb::Env* env = rocksdb::Env::Default();
    auto s = env->CreateDirIfMissing("test_dir/sub_dir");
    if (!s.ok()) {
      return lepton::new_error(s, fmt::format("Failed to create directory: {}", s.ToString()));
    }
    return {};
  }

  lepton::leaf::result<void> create_writable_file(const std::string& file_name) {
    ;
    if (file_exist(file_name)) {
      return lepton::new_error(std::make_error_code(std::errc::file_exists),
                               fmt::format("File {} already exists", file_name));
    }

    rocksdb::Env* env = rocksdb::Env::Default();
    rocksdb::EnvOptions env_opts;
    std::unique_ptr<rocksdb::WritableFile> file_handle;

    auto s = env->NewWritableFile(file_name, &file_handle, env_opts);
    if (!s.ok()) {
      // TODO: map rocksdb status to lepton error codes
      return lepton::new_error(s, fmt::format("Failed to create writable file: {}", s.ToString()));
    }
  }

 private:
  rocksdb::Env* env_;
};

awaitable<void> random_async_write_file(asio::any_io_executor ex) {
  asio::stream_file stream_file(ex);
  // 打开一个文件
  asio::random_access_file file(ex);

  constexpr auto filename = "test.txt";
  delete_if_exists(filename);
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