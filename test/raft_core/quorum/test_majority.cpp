#include <fmt/core.h>
#include <gtest/gtest.h>
#include <proxy.h>

#include <cstdint>
#include <utility>

#include "raft_core/quorum/majority.h"
#include "raft_core/quorum/quorum.h"
#include "test_utility_macros.h"
using namespace lepton;
using namespace lepton::core;
using namespace lepton::core::quorum;

class majority_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

TEST_F(majority_test_suit, test_majority_config) {
  std::set<std::uint64_t> id_set = {1, 2, 3, 4, 5};
  majority_config config(std::move(id_set));

  // majority_config string
  SECTION("majority_config string") { ASSERT_EQ(config.string(), "(1 2 3 4 5)"); }

  using id_ack_map = std::map<std::uint64_t, log_index>;
  SECTION("no id has acked") {
    map_ack_indexer map_ack_indexer_{id_ack_map{}};
    pro::proxy<acked_indexer_builder> indexer = pro::make_proxy<acked_indexer_builder, map_ack_indexer>(id_ack_map{});
    pro::proxy_view<acked_indexer_builder> indexer_view = &map_ack_indexer_;
    fmt::print(stderr, "{}\n", config.describe(indexer_view));
    auto expected = R"(         idx
?          0    (id=1)
?          0    (id=2)
?          0    (id=3)
?          0    (id=4)
?          0    (id=5)
)";
    fmt::print(stderr, "{}", expected);
    ASSERT_EQ(config.describe(indexer_view), expected);
  }

  SECTION("partion id has acked") {
    map_ack_indexer indexer{id_ack_map{
        std::pair<std::uint64_t, log_index>(1, 10),
        std::pair<std::uint64_t, log_index>(3, 20),
    }};
    fmt::print(stderr, "{}\n", config.describe(&indexer));
    auto expected = R"(         idx
xxx>      10    (id=1)
?          0    (id=2)
xxxx>     20    (id=3)
?          0    (id=4)
?          0    (id=5)
)";
    fmt::print(stderr, "{}", expected);
    ASSERT_EQ(config.describe(&indexer), expected);
  }

  SECTION("slice") {
    std::vector<std::uint64_t> expected{1, 2, 3, 4, 5};
    ASSERT_EQ(expected, config.slice());
  }

  SECTION("committed_index") {
    map_ack_indexer indexer{id_ack_map{
        std::pair<std::uint64_t, log_index>(1, 10),
        std::pair<std::uint64_t, log_index>(3, 20),
    }};
    ASSERT_EQ(config.committed_index(&indexer), 0);
  }
}