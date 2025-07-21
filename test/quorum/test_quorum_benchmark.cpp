#include <benchmark/benchmark.h>
#include <gtest/gtest.h>
#include <proxy.h>

#include <cstdint>
#include <set>

#include "majority.h"
#include "quorum.h"
using namespace lepton;

static void BM_majority_config_committed_index(benchmark::State& state) {
  auto n = state.range(0);  // Get the value of n from the benchmark range

  std::set<std::uint64_t> id_set;
  std::map<std::uint64_t, quorum::log_index> id_log_index_ack_map;

  // Initialize MajorityConfig and AckedIndexer
  for (uint64_t i = 0; i < static_cast<uint64_t>(n); ++i) {
    id_set.insert(i + 1);  // Add dummy value to MajorityConfig
    id_log_index_ack_map[i + 1] =
        static_cast<quorum::log_index>(rand() % (std::numeric_limits<int64_t>::max()));  // Random Index
  }

  quorum::majority_config c{std::move(id_set)};
  auto l = pro::make_proxy<quorum::acked_indexer_builder, quorum::map_ack_indexer>(std::move(id_log_index_ack_map));
  pro::proxy_view<quorum::acked_indexer_builder> l_pro_view = l;

  // Run the benchmark for n iterations
  for (auto _ : state) {
    quorum::log_index result = c.committed_index(l_pro_view);
    benchmark::DoNotOptimize(result);
  }
}

// Register individual benchmarks for different values of n
BENCHMARK(BM_majority_config_committed_index)->Arg(1);
BENCHMARK(BM_majority_config_committed_index)->Arg(3);
BENCHMARK(BM_majority_config_committed_index)->Arg(5);
BENCHMARK(BM_majority_config_committed_index)->Arg(7);
BENCHMARK(BM_majority_config_committed_index)->Arg(9);
BENCHMARK(BM_majority_config_committed_index)->Arg(11);