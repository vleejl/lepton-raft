#include <gtest/gtest.h>
#include <raft.pb.h>

#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <optional>
#include <ostream>
#include <system_error>
#include <utility>
#include <vector>

#include "config.h"
#include "error.h"
#include "leaf.hpp"
#include "memory_storage.h"
#include "test_raft_protobuf.h"
#include "types.h"
#include "utility_macros_test.h"
using namespace lepton;
inline const std::error_code EC_SUCCESS;

class memory_storage_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

TEST_F(memory_storage_test_suit, test_storage_term) {
  auto ents = create_entries({{3, 3}, {4, 4}, {5, 5}});
  struct test_case {
    std::uint64_t i;

    std::error_code werr;
    std::uint64_t wterm;
    // C++ can not catch panin
  };

  std::vector<test_case> tests = {{2, make_error_code(storage_error::COMPACTED), 0},
                                  {3, EC_SUCCESS, 3},
                                  {4, EC_SUCCESS, 4},
                                  {5, EC_SUCCESS, 5},
                                  {6, make_error_code(storage_error::UNAVAILABLE), 0}};
  for (const auto &iter_test : tests) {
    auto has_called_error = false;
    lepton::memory_storage mm_storage{ents};
    auto result = leaf::try_handle_some(
        [&]() -> leaf::result<std::uint64_t> {
          BOOST_LEAF_AUTO(v, mm_storage.term(iter_test.i));
          return v;
        },
        [&](const lepton::lepton_error &err) -> leaf::result<std::uint64_t> {
          has_called_error = true;
          if (err != iter_test.werr) {
            assert(false);
          }
          return 0;
        });
    if (iter_test.werr == EC_SUCCESS) {
      GTEST_ASSERT_FALSE(has_called_error);
    } else {
      GTEST_ASSERT_TRUE(has_called_error);
    }
    if (!result.has_value()) {
      printf("no valid result");
    }
    GTEST_ASSERT_TRUE(result.has_value());
    ASSERT_EQ(iter_test.wterm, result.value());
  };
}

TEST_F(memory_storage_test_suit, test_storage_entries) {
  auto ents = create_entries({{3, 3}, {4, 4}, {5, 5}, {6, 6}});
  struct test_case {
    std::uint64_t lo;
    std::uint64_t hi;
    std::uint64_t max_size;

    std::error_code werr;
    lepton::pb::repeated_entry wentries;
  };

  std::vector<test_case> tests = {
      {2, 6, NO_LIMIT, make_error_code(storage_error::COMPACTED)},

      {3, 4, NO_LIMIT, make_error_code(storage_error::COMPACTED)},

      {4, 5, NO_LIMIT, EC_SUCCESS, create_entries({{4, 4}})},

      {4, 6, NO_LIMIT, EC_SUCCESS, create_entries({{4, 4}, {5, 5}})},

      {4, 7, NO_LIMIT, EC_SUCCESS, create_entries({{4, 4}, {5, 5}, {6, 6}})},

      // even if maxsize is zero, the first entry should be returned
      {4, 7, 0, EC_SUCCESS, create_entries({{4, 4}})},

      // limit to 2
      {4, 7, ents[1].ByteSizeLong() + ents[2].ByteSizeLong(), EC_SUCCESS, create_entries({{4, 4}, {5, 5}})},

      // limit to 2
      {4, 7, ents[1].ByteSizeLong() + ents[2].ByteSizeLong() + ents[3].ByteSizeLong() / 2, EC_SUCCESS,
       create_entries({{4, 4}, {5, 5}})},
      {4, 7, ents[1].ByteSizeLong() + ents[2].ByteSizeLong() + ents[3].ByteSizeLong() - 1, EC_SUCCESS,
       create_entries({{4, 4}, {5, 5}})},

      // all
      {4, 7, ents[1].ByteSizeLong() + ents[2].ByteSizeLong() + ents[3].ByteSizeLong(), EC_SUCCESS,
       create_entries({{4, 4}, {5, 5}, {6, 6}})},
  };
  for (const auto &iter_test : tests) {
    auto has_called_error = false;
    lepton::memory_storage mm_storage{ents};
    auto result = leaf::try_handle_some(
        [&]() -> leaf::result<lepton::pb::repeated_entry> {
          BOOST_LEAF_AUTO(v, mm_storage.entries(iter_test.lo, iter_test.hi, iter_test.max_size));
          return v;
        },
        [&](const lepton::lepton_error &err) -> leaf::result<lepton::pb::repeated_entry> {
          has_called_error = true;
          if (err != iter_test.werr) {
            assert(false);
          }
          return {};
        });
    if (iter_test.werr == EC_SUCCESS) {
      GTEST_ASSERT_FALSE(has_called_error);
    } else {
      GTEST_ASSERT_TRUE(has_called_error);
    }
    if (!result.has_value()) {
      printf("no valid result");
    }
    GTEST_ASSERT_TRUE(result.has_value());
    if (iter_test.wentries != result.value()) {
      GTEST_ASSERT_TRUE(false);
    }
  };
}

TEST_F(memory_storage_test_suit, test_storage_last_index) {
  SECTION("case 1") {
    auto ents = create_entries({{3, 3}, {4, 4}, {5, 5}});
    lepton::memory_storage mm_storage{ents};
    auto v = mm_storage.last_index();
    GTEST_ASSERT_TRUE(v.has_value());
    ASSERT_EQ(5, v.value());
  }

  SECTION("case 2") {
    auto ents = create_entries({{3, 3}, {4, 4}, {5, 5}, {6, 5}});
    lepton::memory_storage mm_storage{ents};
    auto v = mm_storage.last_index();
    GTEST_ASSERT_TRUE(v.has_value());
    ASSERT_EQ(6, v.value());
  }
}

TEST_F(memory_storage_test_suit, test_storage_first_index) {
  auto ents = create_entries({{3, 3}, {4, 4}, {5, 5}});
  lepton::memory_storage mm_storage{ents};

  SECTION("case 1") {  // dummy entry
    auto v = mm_storage.first_index();
    GTEST_ASSERT_TRUE(v.has_value());
    ASSERT_EQ(4, v.value());
  }

  SECTION("case 2") {  // dummy entry
    mm_storage.compact(4);
    auto v = mm_storage.first_index();
    GTEST_ASSERT_TRUE(v.has_value());
    ASSERT_EQ(5, v.value());
  }
}

TEST_F(memory_storage_test_suit, test_storage_compact) {
  auto ents = create_entries({{3, 3}, {4, 4}, {5, 5}});
  struct test_case {
    std::uint64_t i;

    std::error_code werr;
    std::uint64_t windex;
    std::uint64_t wterm;
    int wlen;
  };

  std::vector<test_case> tests = {
      {2, make_error_code(storage_error::COMPACTED), 3, 3, 3},

      {3, make_error_code(storage_error::COMPACTED), 3, 3, 3},

      {4, EC_SUCCESS, 4, 4, 2},

      {5, EC_SUCCESS, 5, 5, 1},
  };

  for (const auto &iter_test : tests) {
    auto has_called_error = false;
    lepton::memory_storage mm_storage{ents};
    auto result = leaf::try_handle_some(
        [&]() -> leaf::result<void> {
          BOOST_LEAF_CHECK(mm_storage.compact(iter_test.i));
          return {};
        },
        [&](const lepton::lepton_error &err) -> leaf::result<void> {
          has_called_error = true;
          if (err != iter_test.werr) {
            assert(false);
          }
          return {};
        });
    if (iter_test.werr == EC_SUCCESS) {
      GTEST_ASSERT_FALSE(has_called_error);
    } else {
      GTEST_ASSERT_TRUE(has_called_error);
    }
    if (!result) {
      GTEST_ASSERT_TRUE(false);
    }
  }
}

TEST_F(memory_storage_test_suit, test_storage_create_snapshot) {
  auto ents = create_entries({{3, 3}, {4, 4}, {5, 5}});

  raftpb::conf_state cs;
  cs.add_voters(1);
  cs.add_voters(2);
  cs.add_voters(3);

  std::string data = "data";

  struct test_case {
    std::uint64_t i;

    std::error_code werr;
    raftpb::snapshot wsnap;
  };

  std::vector<test_case> tests = {
      {4, EC_SUCCESS, create_snapshot(4, 4, data, cs)},
      {5, EC_SUCCESS, create_snapshot(5, 5, data, cs)},
  };
  for (const auto &iter_test : tests) {
    auto has_called_error = false;
    lepton::memory_storage mm_storage{ents};
    auto result = leaf::try_handle_some(
        [&]() -> leaf::result<raftpb::snapshot> {
          BOOST_LEAF_AUTO(v, mm_storage.create_snapshot(iter_test.i, cs, "data"));
          return v;
        },
        [&](const lepton::lepton_error &err) -> leaf::result<raftpb::snapshot> {
          has_called_error = true;
          if (err != iter_test.werr) {
            assert(false);
          }
          return {};
        });
    if (iter_test.werr == EC_SUCCESS) {
      GTEST_ASSERT_FALSE(has_called_error);
    } else {
      GTEST_ASSERT_TRUE(has_called_error);
    }
    if (result.value().SerializeAsString() != iter_test.wsnap.SerializeAsString()) {
      GTEST_ASSERT_TRUE(false);
    }
  }
};

TEST_F(memory_storage_test_suit, test_storage_append) {
  auto ents = create_entries({{3, 3}, {4, 4}, {5, 5}});

  struct test_case {
    lepton::pb::repeated_entry entries;

    std::error_code werr;
    lepton::pb::repeated_entry wentries;
  };

  std::vector<test_case> tests = {
      // Case 1: 输入 [1:1, 2:2]，预期追加 [3:3,4:4,5:5]
      {create_entries({{1, 1}, {2, 2}}), EC_SUCCESS, create_entries({{3, 3}, {4, 4}, {5, 5}})},

      // Case 2: 输入与预期结果完全一致
      {create_entries({{3, 3}, {4, 4}, {5, 5}}), EC_SUCCESS, create_entries({{3, 3}, {4, 4}, {5, 5}})},

      // Case 3: 输入存在 term 不一致的条目
      {create_entries({{3, 3}, {4, 6}, {5, 6}}), EC_SUCCESS, create_entries({{3, 3}, {4, 6}, {5, 6}})},

      // Case 4: 输入包含更多条目
      {create_entries({{3, 3}, {4, 4}, {5, 5}, {6, 5}}), EC_SUCCESS, create_entries({{3, 3}, {4, 4}, {5, 5}, {6, 5}})},

      // Case 5: 截断现有条目后追加
      {create_entries({{2, 3}, {3, 3}, {4, 5}}), EC_SUCCESS, create_entries({{3, 3}, {4, 5}})},

      // Case 6: 截断并追加单个条目
      {create_entries({{4, 5}}), EC_SUCCESS, create_entries({{3, 3}, {4, 5}})},

      // Case 7: 直接追加后续条目
      {create_entries({{6, 5}}), EC_SUCCESS, create_entries({{3, 3}, {4, 4}, {5, 5}, {6, 5}})}};
  for (auto &iter_test : tests) {
    auto has_called_error = false;
    lepton::memory_storage mm_storage{ents};
    auto result = leaf::try_handle_some(
        [&]() -> leaf::result<void> {
          BOOST_LEAF_CHECK(mm_storage.append(std::move(iter_test.entries)));
          return {};
        },
        [&](const lepton::lepton_error &err) -> leaf::result<void> {
          has_called_error = true;
          if (err != iter_test.werr) {
            assert(false);
          }
          return {};
        });
    if (iter_test.werr == EC_SUCCESS) {
      GTEST_ASSERT_FALSE(has_called_error);
    } else {
      GTEST_ASSERT_TRUE(has_called_error);
    }
    if (mm_storage.entries_view() != iter_test.wentries) {
      GTEST_ASSERT_TRUE(false);
    }
  }
}

TEST_F(memory_storage_test_suit, test_storage_apply_snapshot) {
  raftpb::conf_state cs;
  cs.add_voters(1);
  cs.add_voters(2);
  cs.add_voters(3);

  std::string data = "data";
  struct test_case {
    raftpb::snapshot snap;

    std::error_code werr;
  };

  std::vector<test_case> tests = {
      {create_snapshot(4, 4, data, cs), EC_SUCCESS},
      {create_snapshot(3, 3, data, cs), make_error_code(storage_error::SNAP_OUT_OF_DATE)},
  };
  lepton::memory_storage mm_storage;
  for (auto &iter_test : tests) {
    auto has_called_error = false;
    auto result = leaf::try_handle_some(
        [&]() -> leaf::result<void> {
          BOOST_LEAF_CHECK(mm_storage.apply_snapshot(std::move(iter_test.snap)));
          return {};
        },
        [&](const lepton::lepton_error &err) -> leaf::result<void> {
          has_called_error = true;
          if (err != iter_test.werr) {
            assert(false);
          }
          return {};
        });
    if (iter_test.werr == EC_SUCCESS) {
      GTEST_ASSERT_FALSE(has_called_error);
    } else {
      GTEST_ASSERT_TRUE(has_called_error);
    }
  }
}