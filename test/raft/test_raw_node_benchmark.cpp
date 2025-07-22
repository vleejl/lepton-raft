#include <benchmark/benchmark.h>
#include <gtest/gtest.h>
#include <proxy.h>
#include <raft.pb.h>

#include <cmath>
#include <cstddef>
#include <cstdio>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "raft.h"
#include "raw_node.h"
#include "state.h"
#include "storage.h"
#include "test_raft_utils.h"

using namespace lepton;

static auto BM_status_init_raw_node(std::size_t members) {
  std::vector<std::uint64_t> peers;
  peers.reserve(members);
  for (std::size_t i = 0; i < members; ++i) {
    peers.push_back(i + 1);
  }
  auto sm_cfg = new_test_config(
      1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers(std::move(peers))}})));
  auto r = new_test_raft(std::move(sm_cfg));
  r.become_follower(1, 1);
  r.become_candidate();
  r.become_leader();
  return lepton::raw_node{std::move(r), false};
}

static void BM_Status(benchmark::State& state) {
  auto members = state.range(0);
  auto rn = BM_status_init_raw_node(static_cast<std::uint64_t>(members));

  for (auto _ : state) {
    benchmark::DoNotOptimize(rn.status());
  }
}

static void BM_StatusExample(benchmark::State& state) {
  auto members = state.range(0);
  auto rn = BM_status_init_raw_node(static_cast<std::uint64_t>(members));

  for (auto _ : state) {
    auto s = rn.status();
    std::uint64_t n = 0;
    for (const auto& [_, pr] : s.progress.view()) {
      n += pr.match();
    }
    benchmark::DoNotOptimize(n);
  }
}

static void BM_BasicStatus(benchmark::State& state) {
  auto members = state.range(0);
  auto rn = BM_status_init_raw_node(static_cast<std::uint64_t>(members));

  for (auto _ : state) {
    benchmark::DoNotOptimize(rn.basic_status());
  }
}

static void BM_WithProgress(benchmark::State& state) {
  auto members = state.range(0);
  auto rn = BM_status_init_raw_node(static_cast<std::uint64_t>(members));

  for (auto _ : state) {
    rn.with_progress([](std::uint64_t, lepton::progress_type, tracker::progress&) {});
  }
}

static void BM_WithProgressExample(benchmark::State& state) {
  auto members = state.range(0);
  auto rn = BM_status_init_raw_node(static_cast<std::uint64_t>(members));

  for (auto _ : state) {
    std::uint64_t n = 0;
    rn.with_progress([&n](std::uint64_t, lepton::progress_type, tracker::progress& pr) { n += pr.match(); });
    benchmark::DoNotOptimize(n);
  }
}

// TODO(vleejl) 不影响功能，暂时不迁移该测试用例
// BenchmarkRawNode

#define REGISTER_BENCHMARK(test) BENCHMARK(test)->Arg(1)->Arg(3)->Arg(5)->Arg(100)

REGISTER_BENCHMARK(BM_Status);
REGISTER_BENCHMARK(BM_StatusExample);
REGISTER_BENCHMARK(BM_BasicStatus);
REGISTER_BENCHMARK(BM_WithProgress);
REGISTER_BENCHMARK(BM_WithProgressExample);