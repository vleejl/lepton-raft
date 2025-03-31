#include <gtest/gtest.h>
#include <raft.pb.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <tuple>
#include <utility>
#include <vector>

#include "protobuf.h"
#include "raft_log_unstable.h"
#include "test_raft_protobuf.h"
#include "utility_macros_test.h"
using namespace lepton;

class unstable_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

lepton::pb::snapshot_ptr create_snapshot_ptr(std::uint64_t index, std::uint64_t term) {
  auto snapshot_metadata = new raftpb::snapshot_metadata();
  snapshot_metadata->set_index(index);
  snapshot_metadata->set_term(term);
  auto snapshot = std::make_unique<raftpb::snapshot>();
  snapshot->set_allocated_metadata(snapshot_metadata);
  return snapshot;
}

unstable create_unstable(const std::vector<std::tuple<std::uint64_t, std::uint64_t>> &entrie_params,
                         std::uint64_t offset,
                         const std::optional<std::tuple<std::uint64_t, std::uint64_t>> &snapshot_params) {
  lepton::pb::repeated_entry entries = create_entries(entrie_params);
  if (snapshot_params) {
    auto [snapshot_index, snapshot_term] = snapshot_params.value();
    return {create_snapshot(snapshot_index, snapshot_term), std::move(entries), offset};
  } else {
    return {std::move(entries), offset};
  }
}

TEST_F(unstable_test_suit, maybe_first_index) {
  // 初始化 std::vector<std::tuple<...>>
  std::vector<std::tuple<std::vector<std::tuple<uint64_t, uint64_t>>,    // entries
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
                    {{5, 1}},                                   // entries: single tuple in vector
                    5,                                          // offset
                    std::make_optional(std::make_tuple(4, 1)),  // snapshot
                    true,                                       // wok
                    5                                           // windex
                },
                {
                    {},                                         // entries: single tuple in vector
                    5,                                          // offset
                    std::make_optional(std::make_tuple(4, 1)),  // snapshot
                    true,                                       // wok
                    5                                           // windex
                }};

  for (const auto &[entrie_params, offset, snapshot_params, wok, windex] : params) {
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
  // 初始化 std::vector<std::tuple<...>>
  std::vector<std::tuple<std::vector<std::tuple<uint64_t, uint64_t>>,    // entries
                         uint64_t,                                       // offset
                         std::optional<std::tuple<uint64_t, uint64_t>>,  // snapshot
                         bool,                                           // wok
                         uint64_t                                        // windex
                         >>
      params = {{
                    // last in entries
                    {{5, 1}},      // entries: vector of tuples
                    5,             // offset
                    std::nullopt,  // snapshot
                    true,          // wok
                    5              // windex
                },
                {
                    {{5, 1}},                                   // entries: single tuple in vector
                    5,                                          // offset
                    std::make_optional(std::make_tuple(4, 1)),  // snapshot
                    true,                                       // wok
                    5                                           // windex
                },
                // last in snapshot
                {
                    {},                                         // entries: single tuple in vector
                    5,                                          // offset
                    std::make_optional(std::make_tuple(4, 1)),  // snapshot
                    true,                                       // wok
                    4                                           // windex
                },
                // empty unstable
                {
                    {},            // entries: single tuple in vector
                    0,             // offset
                    std::nullopt,  // snapshot
                    false,         // wok
                    0              // windex
                }};

  for (const auto &[entrie_params, offset, snapshot_params, wok, windex] : params) {
    auto u = create_unstable(entrie_params, offset, snapshot_params);
    auto result = u.maybe_last_index();
    if (wok) {
      ASSERT_FALSE(result.has_error());
      ASSERT_EQ(result.value(), windex);
    } else {
      ASSERT_TRUE(result.has_error());
    }
  }
}

TEST_F(unstable_test_suit, maybe_term) {
  std::vector<std::tuple<std::vector<std::tuple<uint64_t, uint64_t>>,    // entries
                         uint64_t,                                       // offset
                         std::optional<std::tuple<uint64_t, uint64_t>>,  // snapshot
                         uint64_t,                                       // index
                         bool,                                           // wok
                         uint64_t                                        // wterm
                         >>
      params = {// term from entries
                {
                    {{5, 1}},      // entries: vector of tuples
                    5,             // offset
                    std::nullopt,  // snapshot
                    5,             // index
                    true,          // wok
                    1              // wterm
                },
                {
                    {{5, 1}},      // entries: vector of tuples
                    5,             // offset
                    std::nullopt,  // snapshot
                    6,             // index
                    false,         // wok
                    0              // wterm
                },
                {
                    {{5, 1}},      // entries: vector of tuples
                    5,             // offset
                    std::nullopt,  // snapshot
                    4,             // index
                    false,         // wok
                    0              // wterm
                },
                {
                    {{5, 1}},                                   // entries: vector of tuples
                    5,                                          // offset
                    std::make_optional(std::make_tuple(4, 1)),  // snapshot: tuple of (index, term)
                    5,                                          // index
                    true,                                       // wok
                    1                                           // wterm
                },
                {
                    {{5, 1}},                                   // entries: vector of tuples
                    5,                                          // offset
                    std::make_optional(std::make_tuple(4, 1)),  // snapshot: tuple of (index, term)
                    6,                                          // index
                    false,                                      // wok
                    0                                           // wterm
                },
                // term from snapshot
                {
                    {{5, 1}},                                   // entries: vector of tuples
                    5,                                          // offset
                    std::make_optional(std::make_tuple(4, 1)),  // snapshot: tuple of (index, term)
                    4,                                          // index
                    true,                                       // wok
                    1                                           // wterm
                },
                {
                    {{5, 1}},                                   // entries: vector of tuples
                    5,                                          // offset
                    std::make_optional(std::make_tuple(4, 1)),  // snapshot: tuple of (index, term)
                    3,                                          // index
                    false,                                      // wok
                    0                                           // wterm
                },
                {
                    {},                                         // entries: empty vector
                    5,                                          // offset
                    std::make_optional(std::make_tuple(4, 1)),  // snapshot: tuple of (index, term)
                    5,                                          // index
                    false,                                      // wok
                    0                                           // wterm
                },
                {
                    {},                                         // entries: empty vector
                    5,                                          // offset
                    std::make_optional(std::make_tuple(4, 1)),  // snapshot: tuple of (index, term)
                    4,                                          // index
                    true,                                       // wok
                    1                                           // wterm
                },
                {
                    {},            // entries: empty vector
                    0,             // offset
                    std::nullopt,  // snapshot
                    5,             // index
                    false,         // wok
                    0              // wterm
                }};
  for (const auto &[entrie_params, offset, snapshot_params, index, wok, windex] : params) {
    auto u = create_unstable(entrie_params, offset, snapshot_params);
    auto result = u.maybe_term(index);
    if (wok) {
      ASSERT_FALSE(result.has_error());
      ASSERT_EQ(result.value(), windex);
    } else {
      ASSERT_TRUE(result.has_error());
    }
  }
}

TEST_F(unstable_test_suit, restore) {
  std::vector<std::tuple<std::uint64_t, std::uint64_t>> entrie_params{{5, 1}};
  auto u = create_unstable({{5, 1}}, 5, std::make_optional(std::make_tuple(4, 1)));
  auto s = create_snapshot_ptr(6, 2);
  u.restore(create_snapshot(6, 2));
  ASSERT_EQ(u.offset(), s->metadata().index() + 1);
  ASSERT_TRUE(u.entries_view().empty());
  ASSERT_EQ(u.snapshot_view().SerializeAsString(), s->SerializeAsString());
}

TEST_F(unstable_test_suit, stable_snap_to) {
  std::vector<std::tuple<std::vector<std::tuple<uint64_t, uint64_t>>,    // entries
                         uint64_t,                                       // offset
                         std::optional<std::tuple<uint64_t, uint64_t>>,  // snapshot
                         uint64_t,                                       // index
                         uint64_t,                                       // term
                         uint64_t,                                       // woffset
                         int                                             // wlen
                         >>
      params = {{
                    {},            // entries: empty vector
                    0,             // offset
                    std::nullopt,  // snapshot
                    5,             // index
                    1,             // term
                    0,             // woffset
                    0              // wlen
                },
                {
                    {{5, 1}},      // entries: vector with a single tuple
                    5,             // offset
                    std::nullopt,  // snapshot
                    5,             // index
                    1,             // term
                    6,             // woffset
                    0              // wlen
                },
                {
                    {{5, 1}, {6, 1}},  // entries: vector with two tuples
                    5,                 // offset
                    std::nullopt,      // snapshot
                    5,                 // index
                    1,                 // term
                    6,                 // woffset
                    1                  // wlen
                },
                {
                    {{6, 2}},      // entries: vector with a single tuple
                    6,             // offset
                    std::nullopt,  // snapshot
                    6,             // index
                    1,             // term
                    6,             // woffset
                    1              // wlen
                },
                {
                    {{5, 1}},      // entries: vector with a single tuple
                    5,             // offset
                    std::nullopt,  // snapshot
                    4,             // index
                    1,             // term
                    5,             // woffset
                    1              // wlen
                },
                {
                    {{5, 1}},      // entries: vector with a single tuple
                    5,             // offset
                    std::nullopt,  // snapshot
                    4,             // index
                    2,             // term
                    5,             // woffset
                    1              // wlen
                },
                // with snapshot
                {
                    {{5, 1}},                                   // entries: vector with a single tuple
                    5,                                          // offset
                    std::make_optional(std::make_tuple(4, 1)),  // snapshot: tuple of (index, term)
                    5,                                          // index
                    1,                                          // term
                    6,                                          // woffset
                    0                                           // wlen
                },
                {
                    {{5, 1}, {6, 1}},                           // entries: vector with two tuples
                    5,                                          // offset
                    std::make_optional(std::make_tuple(4, 1)),  // snapshot: tuple of (index, term)
                    5,                                          // index
                    1,                                          // term
                    6,                                          // woffset
                    1                                           // wlen
                },
                {
                    {{6, 2}},                                   // entries: vector with a single tuple
                    6,                                          // offset
                    std::make_optional(std::make_tuple(5, 1)),  // snapshot: tuple of (index, term)
                    6,                                          // index
                    1,                                          // term
                    6,                                          // woffset
                    1                                           // wlen
                },
                {
                    {{5, 1}},                                   // entries: vector with a single tuple
                    5,                                          // offset
                    std::make_optional(std::make_tuple(4, 1)),  // snapshot: tuple of (index, term)
                    4,                                          // index
                    1,                                          // term
                    5,                                          // woffset
                    1                                           // wlen
                },
                {
                    {{5, 2}},                                   // entries: vector with a single tuple
                    5,                                          // offset
                    std::make_optional(std::make_tuple(4, 2)),  // snapshot: tuple of (index, term)
                    4,                                          // index
                    1,                                          // term
                    5,                                          // woffset
                    1                                           // wlen
                }};
  for (const auto &[entrie_params, offset, snapshot_params, index, term, woffset, wlen] : params) {
    auto u = create_unstable(entrie_params, offset, snapshot_params);
    u.stable_to(index, term);
    if (u.offset() != woffset) {
      ASSERT_EQ(u.offset(), offset);
    }

    ASSERT_EQ(u.entries_view().size(), wlen);
  }
}

TEST_F(unstable_test_suit, truncate_and_append) {
  std::vector<std::tuple<std::vector<std::tuple<uint64_t, uint64_t>>,    // entries
                         uint64_t,                                       // offset
                         std::optional<std::tuple<uint64_t, uint64_t>>,  // snapshot
                         std::vector<std::tuple<uint64_t, uint64_t>>,    // to_append
                         uint64_t,                                       // woffset
                         std::vector<std::tuple<uint64_t, uint64_t>>     // wentries
                         >>
      params = {// append to the end
                {
                    {{5, 1}},                 // entries
                    5,                        // offset
                    std::nullopt,             // snapshot
                    {{6, 1}, {7, 1}},         // to_append
                    5,                        // woffset
                    {{5, 1}, {6, 1}, {7, 1}}  // wentries
                },
                // replace the unstable entries
                {
                    {{5, 1}},          // entries
                    5,                 // offset
                    std::nullopt,      // snapshot
                    {{5, 2}, {6, 2}},  // to_append
                    5,                 // woffset
                    {{5, 2}, {6, 2}}   // wentries
                },
                {
                    {{5, 1}},                  // entries
                    5,                         // offset
                    std::nullopt,              // snapshot
                    {{4, 2}, {5, 2}, {6, 2}},  // to_append
                    4,                         // woffset
                    {{4, 2}, {5, 2}, {6, 2}}   // wentries
                },
                // truncate the existing entries and append
                {
                    {{5, 1}, {6, 1}, {7, 1}},  // entries
                    5,                         // offset
                    std::nullopt,              // snapshot
                    {{6, 2}},                  // to_append
                    5,                         // woffset
                    {{5, 1}, {6, 2}}           // wentries
                },
                {
                    {{5, 1}, {6, 1}, {7, 1}},         // entries
                    5,                                // offset
                    std::nullopt,                     // snapshot
                    {{7, 2}, {8, 2}},                 // to_append
                    5,                                // woffset
                    {{5, 1}, {6, 1}, {7, 2}, {8, 2}}  // wentries
                }};
  for (const auto &[entrie_params, offset, snapshot_params, toappend, woffset, wentries_params] : params) {
    auto u = create_unstable(entrie_params, offset, snapshot_params);
    auto entries = create_entries(toappend);
    u.truncate_and_append(std::move(entries));
    if (u.offset() != woffset) {
      ASSERT_EQ(u.offset(), offset);
    }

    auto compare_entries = [](const lepton::pb::repeated_entry &lhs_entries,
                              const lepton::pb::repeated_entry &rhs_entries) {
      ASSERT_EQ(lhs_entries.size(), rhs_entries.size());
      for (int i = 0; i < lhs_entries.size(); ++i) {
        auto lhs_entry = lhs_entries[i].SerializeAsString();
        auto rhs_entry = rhs_entries[i].SerializeAsString();
        ASSERT_EQ(lhs_entry, rhs_entry);
      }
    };
    auto wentries = create_entries(wentries_params);
    compare_entries(u.entries_view(), wentries);
  }
}

TEST_F(unstable_test_suit, convert_protobuf_2_vector) {
  std::vector<lepton::pb::entry_ptr> entries;
  {
    raftpb::message m;
    auto entry1 = m.add_entries();
    entry1->set_index(5);
    entry1->set_term(6);
    auto entry2 = m.add_entries();
    entry2->set_index(6);
    entry2->set_term(7);
    auto mutable_entries = std::move(*m.mutable_entries());
    for (auto &entry : mutable_entries) {
      entries.emplace_back(std::make_unique<raftpb::entry>(std::move(entry)));
    }
  }
  for (const auto &entry_ptr : entries) {
    std::cout << entry_ptr->index() << " " << entry_ptr->term() << std::endl;
  }
}