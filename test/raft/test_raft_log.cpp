#include <error.h>
#include <gtest/gtest.h>
#include <raft.pb.h>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <ostream>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/types/span.h"
#include "config.h"
#include "gtest/gtest.h"
#include "memory_storage.h"
#include "protobuf.h"
#include "raft_log.h"
#include "raft_log_unstable.h"
#include "test_raft_protobuf.h"
#include "types.h"
#include "utility_macros_test.h"
using namespace lepton;

class raft_log_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

TEST_F(raft_log_test_suit, test_find_conflict) {
  struct test_case {
    lepton::pb::repeated_entry entries;

    std::uint64_t wconflict;
  };

  std::vector<test_case> tests = {
      // 无冲突，空条目
      {create_entries({}), 0},

      // 无冲突
      {create_entries({{1, 1}, {2, 2}, {3, 3}}), 0},
      {create_entries({{2, 2}, {3, 3}}), 0},
      {create_entries({{3, 3}}), 0},

      // 无冲突但有新增条目
      {create_entries({{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 4}}), 4},
      {create_entries({{2, 2}, {3, 3}, {4, 4}, {5, 4}}), 4},
      {create_entries({{3, 3}, {4, 4}, {5, 4}}), 4},
      {create_entries({{4, 4}, {5, 4}}), 4},

      // 存在冲突的情况
      {create_entries({{1, 4}, {2, 4}}), 1},                 // 索引1处term不匹配
      {create_entries({{2, 1}, {3, 4}, {4, 4}}), 2},         // 索引2处term不匹配
      {create_entries({{3, 1}, {4, 2}, {5, 4}, {6, 4}}), 3}  // 索引3处term不匹配

  };

  for (const auto &iter_test : tests) {
    pro::proxy<storage_builer> memory_storager = pro::make_proxy<storage_builer, memory_storage>();
    pro::proxy_view<storage_builer> memory_storager_view = memory_storager;
    auto raft_log = new_raft_log(memory_storager_view);
    raft_log->append(create_entries({{1, 1}, {2, 2}, {3, 3}}));
    auto gconflict = raft_log->find_conflict(iter_test.entries);
    ASSERT_EQ(gconflict, iter_test.wconflict);
  }
}

TEST_F(raft_log_test_suit, test_find_conflict_by_term) {
  // 测试用例数据结构
  struct test_case {
    lepton::pb::repeated_entry entries;
    uint64_t index;
    uint64_t term;
    uint64_t want;
  };

  // 测试用例集合
  std::vector<test_case> test_cases = {
      // 日志从 index 1 开始
      {create_entries(0, {0, 2, 2, 5, 5, 5}), 100, 2, 100},  // ErrUnavailable
      {create_entries(0, {0, 2, 2, 5, 5, 5}), 5, 6, 5},
      {create_entries(0, {0, 2, 2, 5, 5, 5}), 5, 5, 5},
      {create_entries(0, {0, 2, 2, 5, 5, 5}), 5, 4, 2},
      {create_entries(0, {0, 2, 2, 5, 5, 5}), 5, 2, 2},
      {create_entries(0, {0, 2, 2, 5, 5, 5}), 5, 1, 0},
      {create_entries(0, {0, 2, 2, 5, 5, 5}), 1, 2, 1},
      {create_entries(0, {0, 2, 2, 5, 5, 5}), 1, 1, 0},
      {create_entries(0, {0, 2, 2, 5, 5, 5}), 0, 0, 0},

      // 包含压缩日志的案例
      {create_entries(10, {3, 3, 3, 4, 4, 4}), 30, 3, 30},  // ErrUnavailable
      {create_entries(10, {3, 3, 3, 4, 4, 4}), 14, 9, 14},
      {create_entries(10, {3, 3, 3, 4, 4, 4}), 14, 4, 14},
      {create_entries(10, {3, 3, 3, 4, 4, 4}), 14, 3, 12},
      {create_entries(10, {3, 3, 3, 4, 4, 4}), 14, 2, 9},
      {create_entries(10, {3, 3, 3, 4, 4, 4}), 11, 5, 11},
      {create_entries(10, {3, 3, 3, 4, 4, 4}), 10, 5, 10},
      {create_entries(10, {3, 3, 3, 4, 4, 4}), 10, 3, 10},
      {create_entries(10, {3, 3, 3, 4, 4, 4}), 10, 2, 9},
      {create_entries(10, {3, 3, 3, 4, 4, 4}), 9, 2, 9},  // ErrCompacted
      {create_entries(10, {3, 3, 3, 4, 4, 4}), 4, 2, 4},  // ErrCompacted
      {create_entries(10, {3, 3, 3, 4, 4, 4}), 0, 0, 0},  // ErrCompacted
  };
  for (auto &iter_test : test_cases) {
    ASSERT_NE(iter_test.entries.size(), 0);
    lepton::memory_storage mm_storage;
    auto snapshot = create_snapshot(iter_test.entries[0].index(), iter_test.entries[0].term());
    mm_storage.apply_snapshot(std::move(snapshot));
    pro::proxy_view<storage_builer> memory_storager_view = &mm_storage;
    iter_test.entries.DeleteSubrange(0, 1);
    auto raft_log = new_raft_log(memory_storager_view);
    raft_log->append(std::move(iter_test.entries));

    auto gconflict = raft_log->find_conflict_by_term(iter_test.index, iter_test.term);
    auto [index, term] = gconflict;
    ASSERT_EQ(iter_test.want, index);
    auto want_term = raft_log->zero_term_on_err_compacted(index);
    if (want_term != term) {
      ASSERT_EQ(want_term, term);
    }
  }
}

TEST_F(raft_log_test_suit, is_up_to_date) {
  pro::proxy<storage_builer> memory_storager = pro::make_proxy<storage_builer, memory_storage>();
  pro::proxy_view<storage_builer> memory_storager_view = memory_storager;
  auto raft_log = new_raft_log(memory_storager_view);
  raft_log->append(create_entries({{1, 1}, {2, 2}, {3, 3}}));
  struct test_case {
    std::uint64_t last_index;
    std::uint64_t term;
    bool wup_to_date;
  };

  std::vector<test_case> tests = {
      // greater term, ignore lastIndex
      {raft_log->last_index() - 1, 4, true},
      {raft_log->last_index(), 4, true},
      {raft_log->last_index() + 1, 4, true},

      // smaller term, ignore lastIndex
      {raft_log->last_index() - 1, 2, false},
      {raft_log->last_index(), 2, false},
      {raft_log->last_index() + 1, 2, false},

      // equal term, equal or lager lastIndex wins
      {raft_log->last_index() - 1, 3, false},
      {raft_log->last_index(), 3, true},
      {raft_log->last_index() + 1, 3, true},
  };
  for (const auto &iter_test : tests) {
    auto gup_to_date = raft_log->is_up_to_date(lepton::pb::entry_id{iter_test.term, iter_test.last_index});
    ASSERT_EQ(iter_test.wup_to_date, gup_to_date);
  }
}

TEST_F(raft_log_test_suit, append) {
  struct test_case {
    lepton::pb::repeated_entry entries;   // ents
    std::uint64_t windex;                 // windex
    lepton::pb::repeated_entry wentries;  // wents
    std::uint64_t wunstable;              // wunstable
  };

  std::vector<test_case> tests = {// 空输入，追加到索引2，预期生成条目[1:1,2:2]，不稳定点从3开始
                                  {
                                      create_entries({}),                // ents
                                      2,                                 // windex
                                      create_entries({{1, 1}, {2, 2}}),  // wents
                                      3                                  // wunstable
                                  },

                                  // 在索引3追加新条目，预期完整日志包含[1:1,2:2,3:2]
                                  {create_entries({{3, 2}}), 3, create_entries({{1, 1}, {2, 2}, {3, 2}}), 3},

                                  // 在索引1发生冲突（term不同）
                                  {
                                      create_entries({{1, 2}}),  // 输入的term与现有条目冲突
                                      1,                         // 冲突位置
                                      create_entries({{1, 2}}),  // 冲突后日志仅保留新条目
                                      1                          // 不稳定点重置到冲突位置
                                  },

                                  // 在索引2发生冲突，覆盖后续条目
                                  {
                                      create_entries({{2, 3}, {3, 3}}),          // 索引2的term从2变为3
                                      3,                                         // 最终写入到索引3
                                      create_entries({{1, 1}, {2, 3}, {3, 3}}),  // 覆盖原索引2的条目
                                      2                                          // 不稳定点从冲突位置开始
                                  }};
  for (auto &iter_test : tests) {
    lepton::memory_storage mm_storage;
    mm_storage.append(create_entries({{1, 1}, {2, 2}}));
    pro::proxy_view<storage_builer> memory_storager_view = &mm_storage;

    auto raft_log = new_raft_log(memory_storager_view);
    auto index = raft_log->append(std::move(iter_test.entries));
    ASSERT_EQ(index, iter_test.windex);

    auto g = raft_log->entries(1, NO_LIMIT);
    GTEST_ASSERT_TRUE(g.has_value());

    if (g.value() != iter_test.wentries) {
      GTEST_ASSERT_TRUE(false);
    }

    if (auto goff = raft_log->unstable_view().offset(); goff != iter_test.wunstable) {
      GTEST_ASSERT_TRUE(false);
    }
  }
}

// TestLogMaybeAppend ensures:
// If the given (index, term) matches with the existing log:
//  1. If an existing entry conflicts with a new one (same index
//     but different terms), delete the existing entry and all that
//     follow it
//  2. Append any new entries not already in the log
//
// If the given (index, term) does not match with the existing log:
//
//	return false
TEST_F(raft_log_test_suit, log_maybe_append) {
  constexpr uint64_t LAST_INDEX = 3;
  constexpr uint64_t LAST_TERM = 3;
  constexpr uint64_t INIT_COMMIT = 1;

  // 更新后的测试用例结构
  struct test_case {
    uint64_t log_term;
    uint64_t index;
    uint64_t committed;
    lepton::pb::repeated_entry ents;  // 使用 protobuf 类型

    uint64_t expected_last;
    bool expected_append;
    uint64_t expected_commit;
    bool expect_exception;
  };

  std::vector<test_case> tests = {
      // Case 1: 任期不匹配 (term 2 < current 3)
      {
          LAST_TERM - 1,  // log_term=2
          LAST_INDEX,     // index=3
          LAST_INDEX,     // committed=3
          create_entries({{4, 4}}),
          0,            // 无更新
          false,        // 追加失败
          INIT_COMMIT,  // 提交不变
          false         // 无异常
      },

      // Case 2: 索引越界 (prev_index=4 > last_index=3)
      {LAST_TERM,       // term=3
       LAST_INDEX + 1,  // index=4
       LAST_INDEX,      // committed=3
       create_entries({{5, 4}}),
       0,            // 无更新
       false,        // 追加失败
       INIT_COMMIT,  // 提交不变
       false},

      // Case 3: 精确匹配最后条目 (无新条目)
      {LAST_TERM,   // term=3
       LAST_INDEX,  // index=3
       LAST_INDEX,  // committed=3
       create_entries({}),
       LAST_INDEX,  // 维持原索引
       true,        // 追加成功
       LAST_INDEX,  // 提交更新为3
       false},

      // Case 4: 提交超过当前日志 (leader_commit=4)
      {LAST_TERM, LAST_INDEX,
       LAST_INDEX + 1,  // leader_commit=4
       create_entries({}),
       LAST_INDEX,  // 日志不变
       true,
       LAST_INDEX,  // 提交限制为3
       false},

      // Case 5: 提交回退 (leader_commit=0)
      {LAST_TERM, LAST_INDEX,
       0,  // leader_commit=0
       create_entries({}), LAST_INDEX, true,
       INIT_COMMIT,  // 维持原提交1
       false},

      // Case 6: 追加单个新条目
      {LAST_TERM, LAST_INDEX,
       LAST_INDEX,  // leader_commit=3
       create_entries({{4, 4}}),
       4,  // 新索引
       true,
       LAST_INDEX,  // 提交保持3
       false},

      // Case 7: 追加并推进提交 (leader_commit=4)
      {LAST_TERM, LAST_INDEX,
       LAST_INDEX + 1,  // leader_commit=4
       create_entries({{4, 4}}), 4, true,
       4,  // 提交更新到4
       false},

      // Case 8: 批量追加 (leader_commit=5)
      {LAST_TERM, LAST_INDEX,
       LAST_INDEX + 2,  // leader_commit=5
       create_entries({{4, 4}, {5, 4}}), 5, true,
       5,  // 提交到新末尾
       false},

      // Case 9: 中间覆盖 (prev_index=2, term=2)
      {LAST_TERM - 1,                         // term=2
       LAST_INDEX - 1,                        // index=2
       LAST_INDEX, create_entries({{3, 4}}),  // 覆盖原索引3
       3,                                     // 新末尾
       true,
       3,  // 提交到新位置
       false},

      // Case 10: 危险覆盖 (prev_index=0, term=0)
      {
          LAST_TERM - 3,                         // term=0
          LAST_INDEX - 3,                        // index=0
          LAST_INDEX, create_entries({{1, 4}}),  // 覆盖已提交的索引1
          1, true, 1,
          true  // 触发异常
      },

      // Case 11: 连续覆盖测试
      {LAST_TERM - 2,                                 // term=1
       LAST_INDEX - 2,                                // index=1
       LAST_INDEX, create_entries({{2, 4}, {3, 4}}),  // 覆盖索引2和3
       3, true, 3, false},
  };

  int test_index = -1;
  for (auto &iter_test : tests) {
    if (iter_test.expect_exception) {
      printf("const char *__restrict format, ...");
    }
    test_index++;
    printf("current test case index:%d\n", test_index);
    // initial
    pro::proxy<storage_builer> memory_storager = pro::make_proxy<storage_builer, memory_storage>();
    pro::proxy_view<storage_builer> memory_storager_view = memory_storager;
    auto raft_log = new_raft_log(memory_storager_view);
    raft_log->append(create_entries({{1, 1}, {2, 2}, {3, 3}}));
    raft_log->commit_to(INIT_COMMIT);

    // run test
    lepton::pb::repeated_entry ents;
    ents.CopyFrom(iter_test.ents);
    // TODO(pav-kv): for now, we pick a high enough app.term so that it
    // represents a valid append message. The maybeAppend currently ignores it,
    // but it must check that the append does not regress the term.
    lepton::pb::entry_id id{iter_test.log_term, iter_test.index};
    lepton::pb::log_slice app{100, id, std::move(iter_test.ents)};
    if (iter_test.expect_exception) {
      EXPECT_DEATH(raft_log->maybe_append(std::move(app), iter_test.committed), "");
      continue;
    }
    auto has_called_error = false;
    auto result = leaf::try_handle_some(
        [&]() -> leaf::result<std::uint64_t> {
          BOOST_LEAF_AUTO(v, raft_log->maybe_append(std::move(app), iter_test.committed));
          return v;
        },
        [&](const lepton::lepton_error &err) -> leaf::result<std::uint64_t> {
          has_called_error = true;
          return 0;
        });
    ASSERT_EQ(result.value(), iter_test.expected_last);
    ASSERT_EQ(!has_called_error, iter_test.expected_append);
    ASSERT_EQ(raft_log->committed(), iter_test.expected_commit);
    if (iter_test.expected_append && !ents.empty()) {
      auto gents = raft_log->slice(raft_log->last_index() - static_cast<std::uint64_t>(ents.size()) + 1,
                                   raft_log->last_index() + 1, NO_LIMIT);
      GTEST_ASSERT_TRUE(gents.has_value());
      if (ents != gents.value()) {
        GTEST_ASSERT_TRUE(false);
      }
    }
  }
}

// TestCompactionSideEffects ensures that all the log related functionality
// works correctly after a compaction.
TEST_F(raft_log_test_suit, compaction_side_effects) {
  // Populate the log with 1000 entries; 750 in stable storage and 250 in
  // unstable.
  constexpr std::uint64_t LAST_INDEX = 1000;
  constexpr std::uint64_t UNSTABLE_INDEX = 750;
  auto LAST_TERM = LAST_INDEX;
  // initial
  std::vector<std::tuple<uint64_t, uint64_t>> entrie_params;
  for (std::uint64_t i = 1; i <= UNSTABLE_INDEX; ++i) {
    entrie_params.push_back({i, i});
  }
  lepton::memory_storage mm_storage;
  mm_storage.append(create_entries(entrie_params));
  pro::proxy_view<storage_builer> memory_storager_view = &mm_storage;

  entrie_params.clear();
  for (std::uint64_t i = UNSTABLE_INDEX + 1; i <= LAST_INDEX; ++i) {
    entrie_params.push_back({i, i});
  }
  auto raft_log = new_raft_log(memory_storager_view);
  raft_log->append(create_entries(entrie_params));
  auto commit_result = raft_log->maybe_commit(lepton::pb::entry_id{.term = LAST_TERM, .index = LAST_INDEX});
  ASSERT_TRUE(commit_result);
  raft_log->applied_to(raft_log->committed(), 0);

  constexpr std::uint64_t COMPACT_INDEX = 500;
  mm_storage.compact(COMPACT_INDEX);
  ASSERT_EQ(raft_log->last_index(), LAST_INDEX);
  auto OFFSET = COMPACT_INDEX;

  for (auto i = OFFSET; i <= raft_log->last_index(); ++i) {
    auto term = i;
    auto result = raft_log->term(i);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), term);
    ASSERT_TRUE(raft_log->match_term({i, i}));
  }

  auto ents = raft_log->unstable_entries();
  ASSERT_EQ(ents.size(), 250);
  for (std::size_t i = 0; i < ents.size(); ++i) {
    ASSERT_EQ(ents[i]->index(), UNSTABLE_INDEX + 1 + i);
  }

  auto prev = raft_log->last_index();
  auto append_result = raft_log->append(create_entries({{prev + 1, prev + 1}}));
  ASSERT_EQ(append_result, prev + 1);
  ASSERT_EQ(raft_log->last_index(), prev + 1);

  auto ents_result = raft_log->entries(raft_log->last_index(), NO_LIMIT);
  ASSERT_TRUE(ents_result.has_value());
  ASSERT_EQ(ents_result.value().size(), 1);
}

TEST_F(raft_log_test_suit, has_next_committed_ents) {
  struct TestCase {
    uint64_t applied = 0;
    uint64_t applying = 0;
    bool allowUnstable = false;
    bool paused = false;
    bool snap = false;
    bool whasNext = false;
  };

  std::vector<TestCase> tests = {
      // allowUnstable = true 的测试组
      {.applied = 3, .applying = 3, .allowUnstable = true, .whasNext = true},
      {.applied = 3, .applying = 4, .allowUnstable = true, .whasNext = true},
      {.applied = 3, .applying = 5, .allowUnstable = true, .whasNext = false},
      {.applied = 4, .applying = 4, .allowUnstable = true, .whasNext = true},
      {.applied = 4, .applying = 5, .allowUnstable = true, .whasNext = false},
      {.applied = 5, .applying = 5, .allowUnstable = true, .whasNext = false},

      // allowUnstable = false 的测试组
      {.applied = 3, .applying = 3, .allowUnstable = false, .whasNext = true},
      {.applied = 3, .applying = 4, .allowUnstable = false, .whasNext = false},
      {.applied = 3, .applying = 5, .allowUnstable = false, .whasNext = false},
      {.applied = 4, .applying = 4, .allowUnstable = false, .whasNext = false},
      {.applied = 4, .applying = 5, .allowUnstable = false, .whasNext = false},
      {.applied = 5, .applying = 5, .allowUnstable = false, .whasNext = false},

      // paused = true 的测试用例
      {.applied = 3, .applying = 3, .allowUnstable = true, .paused = true, .whasNext = false},

      // snap = true 的测试用例
      {.applied = 3, .applying = 3, .allowUnstable = true, .snap = true, .whasNext = false},
  };
  int test_index = -1;
  for (const auto &iter_test : tests) {
    test_index++;
    printf("current test case index:%d\n", test_index);
    auto ents = create_entries(4, {1, 1, 1});
    lepton::memory_storage mm_storage;
    ASSERT_TRUE(mm_storage.apply_snapshot(create_snapshot(3, 1)));
    ASSERT_TRUE(mm_storage.append(lepton::pb::extract_range_without_copy(ents, 0, 1)));

    pro::proxy_view<storage_builer> memory_storager_view = &mm_storage;
    auto raft_log = new_raft_log(memory_storager_view);
    ASSERT_TRUE(raft_log.has_value());
    raft_log->append(create_entries(4, {1, 1, 1}));
    raft_log->stable_to(lepton::pb::entry_id{.term = 1, .index = 4});
    raft_log->maybe_commit(lepton::pb::entry_id{.term = 1, .index = 5});
    raft_log->applied_to(iter_test.applied, 0);
    raft_log->accept_applying(iter_test.applying, 0, iter_test.allowUnstable);
    raft_log->set_applying_ents_paused(iter_test.paused);
    if (iter_test.snap) {
      auto snap = create_snapshot(4, 1);
      raft_log->restore(std::move(snap));
    }
    if (iter_test.whasNext != raft_log->has_next_committed_ents(iter_test.allowUnstable)) {
      ASSERT_EQ(iter_test.whasNext, raft_log->has_next_committed_ents(iter_test.allowUnstable));
    }
  }
}

// TestUnstableEnts ensures unstableEntries returns the unstable part of the
// entries correctly.
TEST_F(raft_log_test_suit, unstable_ents) {
  auto previous_ents = create_entries({{1, 1}, {2, 2}});
  struct test_case {
    std::uint64_t unstable;
    lepton::pb::repeated_entry wents;
  };
  std::vector<test_case> tests = {
      {
          3,
      },
      {1, previous_ents},
  };
  for (auto &iter_test : tests) {
    lepton::memory_storage mm_storage;
    lepton::pb::repeated_entry ents;
    for (std::uint64_t i = 0; i < iter_test.unstable - 1; ++i) {
      ents.Add()->CopyFrom(previous_ents[i]);
    }
    mm_storage.append(std::move(ents));
    pro::proxy_view<storage_builer> memory_storager_view = &mm_storage;
    auto raft_log = new_raft_log(memory_storager_view);
    ASSERT_TRUE(raft_log.has_value());
    ents.Clear();
    for (std::uint64_t i = iter_test.unstable - 1; i < previous_ents.size(); ++i) {
      ents.Add()->CopyFrom(previous_ents[i]);
    }
    raft_log->append(std::move(ents));

    auto unstable_ents = raft_log->unstable_entries();
    auto wents_span = absl::MakeSpan(iter_test.wents);
    if (unstable_ents != wents_span) {
      GTEST_ASSERT_TRUE(false);
    }
    if (auto l = unstable_ents.size(); l > 0) {
      // 持久化后会清空 unstable entries；所以必须先对比才能stable
      raft_log->stable_to({unstable_ents[l - 1]->term(), unstable_ents[l - 1]->index()});
    }
    auto w = previous_ents[previous_ents.size() - 1].index() + 1;
    ASSERT_EQ(w, raft_log->unstable_view().offset());
  }
}

TEST_F(raft_log_test_suit, commit_to) {
  constexpr std::uint64_t COMMIT = 2;
  struct test_case {
    std::uint64_t commit;
    std::uint64_t wcommit;
    bool wpanic;
  };
  std::vector<test_case> tests = {
      {3, 3, false},
      {1, 2, false},  // never decrease
      {4, 0, true},   // commit out of range -> panic
  };
  for (auto &iter_test : tests) {
    pro::proxy<storage_builer> memory_storager = pro::make_proxy<storage_builer, memory_storage>();
    pro::proxy_view<storage_builer> memory_storager_view = memory_storager;
    auto raft_log = new_raft_log(memory_storager_view);
    raft_log->append(create_entries({{1, 1}, {2, 2}, {3, 3}}));
    raft_log->set_commit(COMMIT);
    if (iter_test.wpanic) {
      EXPECT_DEATH(raft_log->commit_to(iter_test.commit), "");
      continue;
    }
    raft_log->commit_to(iter_test.commit);
    ASSERT_EQ(raft_log->committed(), iter_test.wcommit);
  }
}

TEST_F(raft_log_test_suit, stable_to) {
  struct test_case {
    std::uint64_t stablei;
    std::uint64_t stablet;
    std::uint64_t wunstable;
  };

  std::vector<test_case> tests = {
      {1, 1, 2},
      {2, 2, 3},
      {2, 1, 1},  // bad term
      {3, 1, 1},  // bad index
  };
  for (auto &iter_test : tests) {
    pro::proxy<storage_builer> memory_storager = pro::make_proxy<storage_builer, memory_storage>();
    pro::proxy_view<storage_builer> memory_storager_view = memory_storager;
    auto raft_log = new_raft_log(memory_storager_view);
    raft_log->append(create_entries({{1, 1}, {2, 2}}));
    raft_log->stable_to({iter_test.stablet, iter_test.stablei});
    ASSERT_EQ(raft_log->unstable_view().offset(), iter_test.wunstable);
  }
}

TEST_F(raft_log_test_suit, stable_to_with_snapshot) {
  const uint64_t snapi = 5;
  const uint64_t snapt = 2;

  struct test_case {
    std::uint64_t stablei;
    std::uint64_t stablet;
    lepton::pb::repeated_entry newEnts;  // 使用类似命名风格
    std::uint64_t wunstable;
  };

  std::vector<test_case> tests = {
      // 原始测试组 1 (newEnts 为空)
      {snapi + 1, snapt, create_entries({}), snapi + 1},
      {snapi, snapt, create_entries({}), snapi + 1},
      {snapi - 1, snapt, create_entries({}), snapi + 1},

      // 原始测试组 2 (更新了 stablet)
      {snapi + 1, snapt + 1, create_entries({}), snapi + 1},
      {snapi, snapt + 1, create_entries({}), snapi + 1},
      {snapi - 1, snapt + 1, create_entries({}), snapi + 1},

      // 原始测试组 3 (包含新条目)
      {snapi + 1, snapt, create_entries({{snapi + 1, snapt}}), snapi + 2},
      {snapi, snapt, create_entries({{snapi + 1, snapt}}), snapi + 1},
      {snapi - 1, snapt, create_entries({{snapi + 1, snapt}}), snapi + 1},

      // 原始测试组 4 (混合 stablet 和新条目)
      {snapi + 1, snapt + 1, create_entries({{snapi + 1, snapt}}), snapi + 1},
      {snapi, snapt + 1, create_entries({{snapi + 1, snapt}}), snapi + 1},
      {snapi - 1, snapt + 1, create_entries({{snapi + 1, snapt}}), snapi + 1},
  };
  int test_index = -1;
  for (auto &iter_test : tests) {
    test_index++;
    printf("current test case index:%d\n", test_index);
    lepton::memory_storage mm_storage;
    mm_storage.apply_snapshot(create_snapshot(snapi, snapt));
    pro::proxy_view<storage_builer> memory_storager_view = &mm_storage;
    auto raft_log = new_raft_log(memory_storager_view);
    raft_log->append(std::move(iter_test.newEnts));

    raft_log->stable_to({iter_test.stablet, iter_test.stablei});
    ASSERT_EQ(raft_log->unstable_view().offset(), iter_test.wunstable);
  }
}

// TestCompaction ensures that the number of log entries is correct after
// compactions.
TEST_F(raft_log_test_suit, compactions) {
  struct test_case {
    std::uint64_t lastIndex;
    std::vector<std::uint64_t> compact;
    std::vector<int> wleft;
    bool wallow;
    bool has_panic;
  };

  std::vector<test_case> tests = {
      // 超出上界
      {1000, {1001}, {-1}, false, true},

      // 正常压缩序列
      {1000, {300, 500, 800, 900}, {700, 500, 200, 100}, true, false},

      // 包含超出下界
      {1000, {300, 299}, {700, -1}, false, false},
  };
  int test_index = -1;
  for (auto &iter_test : tests) {
    test_index++;
    printf("current test case index:%d\n", test_index);
    lepton::memory_storage mm_storage;
    std::vector<std::tuple<uint64_t, uint64_t>> entrie_params;
    for (std::uint64_t i = 1; i <= iter_test.lastIndex; ++i) {
      entrie_params.push_back({i, 0});
    }
    mm_storage.append(create_entries(entrie_params));
    pro::proxy_view<storage_builer> memory_storager_view = &mm_storage;
    auto raft_log = new_raft_log(memory_storager_view);
    raft_log->maybe_commit(lepton::pb::entry_id{.term = 0, .index = iter_test.lastIndex});
    raft_log->applied_to(raft_log->committed(), 0);

    int j = -1;
    for (const auto &compact_index : iter_test.compact) {
      ++j;
      if (iter_test.has_panic) {
        EXPECT_DEATH(mm_storage.compact(compact_index), "");
        continue;
      }
      auto result = mm_storage.compact(compact_index);
      if (!result) {
        if (iter_test.wallow) {
          ASSERT_FALSE(true);
        }
        continue;
      }
      ASSERT_EQ(raft_log->all_entries().size(), iter_test.wleft[j]);
    }
  }
}

TEST_F(raft_log_test_suit, log_restore) {
  constexpr std::uint64_t INDEX = 1000;
  constexpr std::uint64_t TERM = 1000;
  lepton::memory_storage mm_storage;
  mm_storage.apply_snapshot(create_snapshot(INDEX, TERM));
  pro::proxy_view<storage_builer> memory_storager_view = &mm_storage;
  auto raft_log = new_raft_log(memory_storager_view);
  ASSERT_EQ(raft_log->all_entries().size(), 0);
  ASSERT_EQ(raft_log->first_index(), INDEX + 1);
  ASSERT_EQ(raft_log->committed(), INDEX);
  ASSERT_EQ(raft_log->unstable_view().offset(), INDEX + 1);
  auto term = raft_log->term(INDEX);
  ASSERT_TRUE(term.has_value());
  ASSERT_EQ(term.value(), TERM);
}

TEST_F(raft_log_test_suit, is_out_of_bounds) {
  constexpr std::uint64_t offset = 100;
  constexpr std::uint64_t num = 100;
  constexpr std::uint64_t first = offset + 1;

  struct test_case {
    std::uint64_t lo;
    std::uint64_t hi;
    bool wpanic;
    bool wErrCompacted;
  };

  std::vector<test_case> tests = {
      // 低边界异常测试
      {first - 2, first + 1, false, true},  // 前界越界
      {first - 1, first + 1, false, true},  // 临界前界

      // 正常范围测试
      {first, first, false, false},                      // 单元素范围
      {first + num / 2, first + num / 2, false, false},  // 中间点
      {first + num - 1, first + num - 1, false, false},  // 有效上界

      // 高边界测试
      {first + num, first + num, false, false},         // 精确上界
      {first + num, first + num + 1, true, false},      // 上界溢出
      {first + num + 1, first + num + 1, true, false},  // 完全越界
  };

  lepton::memory_storage mm_storage;
  mm_storage.apply_snapshot(create_snapshot(offset, 0));
  pro::proxy_view<storage_builer> memory_storager_view = &mm_storage;
  auto raft_log = new_raft_log(memory_storager_view);
  std::vector<std::tuple<uint64_t, uint64_t>> entrie_params;
  for (std::uint64_t i = 1; i <= num; ++i) {
    entrie_params.push_back({i + offset, 0});
  }
  raft_log->append(create_entries(entrie_params));

  for (auto &iter_test : tests) {
    if (iter_test.wpanic) {
      EXPECT_DEATH(raft_log->must_check_out_of_bounds(iter_test.lo, iter_test.hi), "");
      continue;
    }
    std::error_code err_code;
    auto has_called_error = false;
    auto result = leaf::try_handle_some(
        [&]() -> leaf::result<void> {
          BOOST_LEAF_CHECK(raft_log->must_check_out_of_bounds(iter_test.lo, iter_test.hi));
          return {};
        },
        [&](const lepton::lepton_error &err) -> leaf::result<void> {
          has_called_error = true;
          err_code = err.err_code;
          return new_error(err);
        });
    ASSERT_FALSE(iter_test.wpanic);
    if (iter_test.wErrCompacted) {
      ASSERT_EQ(err_code, make_error_code(storage_error::COMPACTED));
    } else {
      if (!result) {
        ASSERT_FALSE(true);
      }
    }
  }
}

TEST_F(raft_log_test_suit, term) {
  constexpr std::uint64_t offset = 100;
  constexpr std::uint64_t num = 100;
  lepton::memory_storage mm_storage;
  mm_storage.apply_snapshot(create_snapshot(offset, 1));
  pro::proxy_view<storage_builer> memory_storager_view = &mm_storage;
  auto raft_log = new_raft_log(memory_storager_view);
  std::vector<std::tuple<uint64_t, uint64_t>> entrie_params;
  for (std::uint64_t i = 1; i < num; ++i) {
    entrie_params.push_back({i + offset, i});
  }
  raft_log->append(create_entries(entrie_params));

  struct test_case {
    std::uint64_t index;
    std::uint64_t w;
  };
  std::vector<test_case> tests = {
      {offset - 1, 0}, {offset, 1}, {offset + num / 2, num / 2}, {offset + num - 1, num - 1}, {offset + num, 0},
  };
  for (auto &iter_test : tests) {
    auto term = raft_log->term(iter_test.index);
    ASSERT_TRUE(term.has_value());
    ASSERT_EQ(term.value(), iter_test.w);
  }
}

TEST_F(raft_log_test_suit, term_with_unstable_snapshot) {
  constexpr std::uint64_t storagesnapi = 100;
  constexpr std::uint64_t unstablesnapi = storagesnapi + 5;

  lepton::memory_storage mm_storage;
  mm_storage.apply_snapshot(create_snapshot(storagesnapi, 1));
  pro::proxy_view<storage_builer> memory_storager_view = &mm_storage;
  auto raft_log = new_raft_log(memory_storager_view);
  raft_log->restore(create_snapshot(unstablesnapi, 1));

  struct test_case {
    std::uint64_t index;
    std::uint64_t w;
  };
  std::vector<test_case> tests = {
      // cannot get term from storage
      {storagesnapi, 0},
      // cannot get term from the gap between storage ents and unstable snapshot
      {storagesnapi + 1, 0},
      {unstablesnapi - 1, 0},
      // get term from unstable snapshot index
      {unstablesnapi, 1},
  };
  for (auto &iter_test : tests) {
    auto term = raft_log->term(iter_test.index);
    ASSERT_TRUE(term.has_value());
    ASSERT_EQ(term.value(), iter_test.w);
  }
}

TEST_F(raft_log_test_suit, slice) {
  struct test_case {
    std::uint64_t from;
    std::uint64_t to;
    std::uint64_t limit;
    lepton::pb::repeated_entry w;
    bool wpanic;
  };

  // 运行时计算条目大小
  const std::uint64_t offset = 100;
  const std::uint64_t num = 100;
  const std::uint64_t last = offset + num;
  const std::uint64_t half = offset + num / 2;
  raftpb::entry entry;
  entry.set_index(half);
  entry.set_term(half);
  const std::size_t base_entry_size = entry.ByteSizeLong();

  // 全局测试数据
  const std::vector<test_case> tests = {
      // test no limit
      {offset - 1, offset + 1, UINT64_MAX, create_entries({}), false},
      {offset, offset + 1, UINT64_MAX, create_entries({}), false},
      {half - 1, half + 1, UINT64_MAX, create_entries({{half - 1, half - 1}, {half, half}}), false},
      {half, half + 1, UINT64_MAX, create_entries({{half, half}}), false},
      {last - 1, last, UINT64_MAX, create_entries({{last - 1, last - 1}}), false},
      {last, last + 1, UINT64_MAX, create_entries({}), true},

      // test limit
      {half - 1, half + 1, 0, create_entries({{half - 1, half - 1}}), false},
      {half - 1, half + 1, base_entry_size + 1, create_entries({{half - 1, half - 1}}), false},
      {half - 2, half + 1, base_entry_size + 1, create_entries({{half - 2, half - 2}}), false},
      {half - 1, half + 1, base_entry_size * 2, create_entries({{half - 1, half - 1}, {half, half}}), false},
      {half - 1, half + 2, base_entry_size * 3,
       create_entries({{half - 1, half - 1}, {half, half}, {half + 1, half + 1}}), false},
      {half, half + 2, base_entry_size, create_entries({{half, half}}), false},
      {half, half + 2, base_entry_size * 2, create_entries({{half, half}, {half + 1, half + 1}}), false},
  };

  lepton::memory_storage mm_storage;
  mm_storage.apply_snapshot(create_snapshot(offset, 0));
  std::vector<std::tuple<uint64_t, uint64_t>> entrie_params;
  for (std::uint64_t i = 1; i < num / 2; ++i) {
    entrie_params.push_back({i + offset, i + offset});
  }
  mm_storage.append(create_entries(entrie_params));
  pro::proxy_view<storage_builer> memory_storager_view = &mm_storage;
  auto raft_log = new_raft_log(memory_storager_view);

  for (std::uint64_t i = num / 2; i < num; ++i) {
    entrie_params.push_back({i + offset, i + offset});
  }
  raft_log->append(create_entries(entrie_params));

  int test_index = -1;
  for (auto &iter_test : tests) {
    test_index++;
    printf("current test case index:%d\n", test_index);
    if (iter_test.wpanic) {
      EXPECT_DEATH(raft_log->slice(iter_test.from, iter_test.to, iter_test.limit), "");
      continue;
    }
    std::error_code err_code;
    auto has_called_error = false;
    auto result = leaf::try_handle_some(
        [&]() -> leaf::result<lepton::pb::repeated_entry> {
          BOOST_LEAF_AUTO(v, raft_log->slice(iter_test.from, iter_test.to, iter_test.limit));
          return v;
        },
        [&](const lepton::lepton_error &err) -> leaf::result<lepton::pb::repeated_entry> {
          has_called_error = true;
          err_code = err.err_code;
          return new_error(err);
        });
    ASSERT_FALSE(iter_test.wpanic);
    if (iter_test.from <= offset && err_code != make_error_code(storage_error::COMPACTED)) {
      ASSERT_TRUE(false);
    }
    if (iter_test.from > offset && has_called_error) {
      ASSERT_TRUE(false);
    }
    if (result.has_value()) {
      if (result.value() != iter_test.w) {
        ASSERT_TRUE(false);
      }
    } else {
      ASSERT_TRUE(iter_test.w.empty());
    }
  }
}

TEST_F(raft_log_test_suit, scan) {
  std::uint64_t offset = 47;
  std::uint64_t num = 20;
  auto last = offset + num;
  auto half = offset + num / 2;
  auto entries_func = [](std::uint64_t from, std::uint64_t to) -> lepton::pb::repeated_entry {
    return create_entries_with_term_range(from, from, to);
  };
  auto entry_size = lepton::pb::ent_size(entries_func(half, half + 1));

  lepton::memory_storage mm_storage;
  ASSERT_TRUE(mm_storage.apply_snapshot(create_snapshot(offset, 0)));
  ASSERT_TRUE(mm_storage.append(entries_func(offset + 1, half)));
  pro::proxy_view<storage_builer> memory_storager_view = &mm_storage;
  auto raft_log = new_raft_log(memory_storager_view);
  raft_log->append(entries_func(half, last));

  // Test that scan() returns the same entries as slice(), on all inputs.
  std::vector<lepton::pb::entry_encoding_size> page_size_list{0, 1, 10, 100, entry_size, entry_size + 1};
  for (const auto page_size : page_size_list) {
    for (auto lo = offset + 1; lo < last; lo++) {
      for (auto hi = lo; hi <= last; hi++) {
        lepton::pb::repeated_entry got;
        raft_log->scan(lo, hi, page_size, [&](const lepton::pb::repeated_entry &entries) -> leaf::result<void> {
          got.Add(entries.begin(), entries.end());
          auto result = ((entries.size() == 1) || (lepton::pb::ent_size(entries) <= page_size));
          assert(result);
          return {};
        });

        auto want = raft_log->slice(lo, hi, NO_LIMIT);
        ASSERT_TRUE(want.has_value());
        if (want.value() != got) {
          ASSERT_TRUE(false);
        }
      }
    }
  }

  // Test that the callback error is propagated to the caller.
  auto has_occured_error = false;
  auto iters = 0;
  auto result = leaf::try_handle_some(
      [&]() -> leaf::result<void> {
        BOOST_LEAF_CHECK(
            raft_log->scan(offset + 1, half, 0, [&](const lepton::pb::repeated_entry &entries) -> leaf::result<void> {
              iters++;
              if (iters == 2) {
                return new_error(logic_error::LOOP_BREAK);
              }
              return {};
            }));
        return {};
      },
      [&](const lepton::lepton_error &err) -> leaf::result<void> {
        has_occured_error = true;
        if (err == logic_error::LOOP_BREAK) {
          return {};
        }
        panic(fmt::format("error scanning unapplied entries "));
        return new_error(err);
      });
  ASSERT_EQ(iters, 2);
  ASSERT_TRUE(result);
  ASSERT_TRUE(has_occured_error);

  // Test that we max out the limit, and not just always return a single entry.
  // NB: this test works only because the requested range length is even.
  has_occured_error = false;
  result = leaf::try_handle_some(
      [&]() -> leaf::result<void> {
        BOOST_LEAF_CHECK(raft_log->scan(offset + 1, offset + 11, entry_size * 2,
                                        [&](const lepton::pb::repeated_entry &entries) -> leaf::result<void> {
                                          assert(entries.size() == 2);
                                          assert(lepton::pb::ent_size(entries) == entry_size * 2);
                                          return {};
                                        }));
        return {};
      },
      [&](const lepton::lepton_error &err) -> leaf::result<void> {
        has_occured_error = true;
        if (err == logic_error::LOOP_BREAK) {
          return {};
        }
        panic(fmt::format("error scanning unapplied entries "));
        return new_error(err);
      });
  ASSERT_TRUE(result);
}