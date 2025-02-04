#include <gtest/gtest.h>
#include <raft.pb.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "raft_log_unstable.h"
#include "raft_pb.h"
#include "utility_macros_test.h"
using namespace lepton;

class unstable_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::cout << "run before first case..." << std::endl;
  }

  static void TearDownTestSuite() {
    std::cout << "run after last case..." << std::endl;
  }

  virtual void SetUp() override {
    std::cout << "enter from SetUp" << std::endl;
  }

  virtual void TearDown() override {
    std::cout << "exit from TearDown" << std::endl;
  }
};

lepton::pb::entry_ptr create_entry(std::uint64_t index, std::uint64_t term) {
  auto entry = std::make_unique<raftpb::entry>();
  entry->set_index(index);
  entry->set_term(term);
  return entry;
}

lepton::pb::snapshot_ptr create_snapshot(std::uint64_t index,
                                         std::uint64_t term) {
  auto snapshot_metadata = new raftpb::snapshot_metadata();
  snapshot_metadata->set_index(index);
  snapshot_metadata->set_term(1);
  auto snapshot = std::make_unique<raftpb::snapshot>();
  snapshot->set_allocated_metadata(snapshot_metadata);
  return snapshot;
}

unstable create_unstable(
    const std::vector<std::tuple<std::uint64_t, std::uint64_t>> &entrie_params,
    std::uint64_t offset,
    const std::optional<std::tuple<std::uint64_t, std::uint64_t>>
        &snapshot_params) {
  std::vector<lepton::pb::entry_ptr> entries;
  for (const auto &[index, term] : entrie_params) {
    entries.emplace_back(create_entry(index, term));
  }
  if (snapshot_params) {
    auto [snapshot_index, snapshot_term] = snapshot_params.value();
    return {create_snapshot(snapshot_index, snapshot_term), std::move(entries),
            offset};
  } else {
    return {nullptr, std::move(entries), offset};
  }
}

TEST_F(unstable_test_suit, maybe_first_index) {
  // 初始化 std::vector<std::tuple<...>>
  std::vector<
      std::tuple<std::vector<std::tuple<uint64_t, uint64_t>>,    // entries
                 uint64_t,                                       // offset
                 std::optional<std::tuple<uint64_t, uint64_t>>,  // snapshot
                 bool,                                           // wok
                 uint64_t                                        // windex
                 >>
      params = {{
                    // no snapshot
                    {{5, 1}},      // entries: vector of tuples
                    5,             // offset
                    std::nullopt,  // snapshot
                    false,         // wok
                    0              // windex
                },
                {
                    {},            // entries: empty vector
                    0,             // offset
                    std::nullopt,  // snapshot: no value
                    false,         // wok
                    0              // windex
                },
                // has snapshot
                {
                    {{5, 1}},  // entries: single tuple in vector
                    5,         // offset
                    std::make_optional(std::make_tuple(4, 1)),  // snapshot
                    true,                                       // wok
                    5                                           // windex
                },
                {
                    {{}},  // entries: single tuple in vector
                    5,     // offset
                    std::make_optional(std::make_tuple(4, 1)),  // snapshot
                    true,                                       // wok
                    5                                           // windex
                }};

  for (const auto &[entrie_params, offset, snapshot_params, wok, windex] :
       params) {
    auto u = create_unstable(entrie_params, offset, snapshot_params);
    auto result = u.maybe_first_index();
    if (wok) {
      ASSERT_FALSE(result.has_error());
      ASSERT_EQ(result.value(), windex);
    } else {
      ASSERT_TRUE(result.has_error());
    }
  }
}

TEST_F(unstable_test_suit, maybe_last_index) {
  SECTION("last in entries ---  case 1") {
    std::vector<lepton::pb::entry_ptr> entries;
    entries.emplace_back(create_entry(5, 1));

    unstable u{nullptr, std::move(entries), 5};
    auto result = u.maybe_last_index();
    ASSERT_FALSE(result.has_error());
    ASSERT_EQ(result.value(), 5);
  }

  SECTION("last in entries --- case 2") {
    std::vector<lepton::pb::entry_ptr> entries;
    entries.emplace_back(create_entry(5, 1));

    unstable u{create_snapshot(4, 1), std::move(entries), 5};
    auto result = u.maybe_last_index();
    ASSERT_FALSE(result.has_error());
    ASSERT_EQ(result.value(), 5);
  }

  SECTION("last in snapshot") {
    std::vector<lepton::pb::entry_ptr> entries;

    unstable u{create_snapshot(4, 1), std::move(entries), 5};
    auto result = u.maybe_last_index();
    ASSERT_FALSE(result.has_error());
    ASSERT_EQ(result.value(), 4);
  }

  SECTION("empty unstable") {
    std::vector<lepton::pb::entry_ptr> entries;

    unstable u{nullptr, std::move(entries), 5};
    auto result = u.maybe_last_index();
    ASSERT_TRUE(result.has_error());
  }
}