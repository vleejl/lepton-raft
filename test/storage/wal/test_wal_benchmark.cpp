#include <benchmark/benchmark.h>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <gtest/gtest.h>
#include <proxy.h>

#include <asio/use_future.hpp>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "basic/spdlog_logger.h"
#include "raft.pb.h"
#include "storage/fileutil/path.h"
#include "storage/wal/wal.h"
#include "test_utility_macros.h"
using namespace lepton::storage::fileutil;
using namespace lepton::storage::wal;
using namespace lepton::storage;
using namespace std::string_literals;

constexpr static auto wal_dir_path = "./wal_test_dir";

static void BM_benchmark_write_entry(benchmark::State& state) {
  const auto size = static_cast<std::size_t>(state.range(0));
  const auto batch = state.range(1);

  // 1. 环境准备（不计入时间）
  std::filesystem::remove_all(wal_dir_path);
  std::filesystem::create_directories(join_paths(wal_dir_path, "wal_test"));

  rocksdb::Env* env = rocksdb::Env::Default();
  asio::io_context io_context;
  auto wal_test_dir = join_paths(wal_dir_path, "wal_test");

  // 预构造数据
  std::vector<char> raw_data(size, 'a');
  raftpb::Entry entry;
  entry.set_data(raw_data.data(), raw_data.size());
  const auto entry_size = static_cast<std::int64_t>(entry.ByteSizeLong());

  // 使用 future 确保同步等待协程结束
  auto future = asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_test_dir, "somedata",
                                              SEGMENT_SIZE_BYTES, std::make_shared<lepton::spdlog_logger>());
        auto wal_handle = wal_result.value();

        int n = 0;

        // --- 开始精确计时 ---
        for (auto _ : state) {
          auto status = co_await wal_handle->save_entry(entry);
          if (!status) {
            state.SkipWithError("SaveEntry failed");
            break;
          }

          if (batch > 0 && ++n >= batch) {
            co_await wal_handle->sync();
            n = 0;
          }
        }
        // --- 结束精确计时 ---

        co_await wal_handle->close();
        co_return;
      },
      asio::use_future);

  io_context.run();
  future.get();  // 确保异常能抛出

  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * entry_size);
  std::filesystem::remove_all(wal_dir_path);
}

BENCHMARK(BM_benchmark_write_entry)
    ->Args({100, 0})   // WithoutBatch
    ->Args({100, 10})  // Batch 10
    ->Args({100, 100})
    ->Args({1000, 0})
    ->Args({1000, 10})
    ->Args({1000, 1000})
    ->Unit(benchmark::kMicrosecond);  // 设置时间单位为微秒
