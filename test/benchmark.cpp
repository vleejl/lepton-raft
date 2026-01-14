#include <benchmark/benchmark.h>
#include <spdlog/spdlog.h>

struct BenchmarkInitializer {
  BenchmarkInitializer() {
    // 开启日志输出后会影响 benchmark 执行
    spdlog::set_level(spdlog::level::off);
    std::ios_base::sync_with_stdio(false);
  }

  ~BenchmarkInitializer() {
    // 清理资源
  }
};

// 全局初始化器
static BenchmarkInitializer benchmark_init;

BENCHMARK_MAIN();