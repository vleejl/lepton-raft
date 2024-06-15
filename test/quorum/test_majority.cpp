#include <fmt/core.h>
#include "proxy.h"
#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "majority.h"
#include "quorum.h"
using namespace lepton;
using namespace lepton::quorum;
TEST_CASE("test majority config", "[majority_config]") {
  std::set<std::uint64_t> id_set = {1, 2, 3, 4, 5};
  majority_config config(std::move(id_set));

  SECTION("majority_config string") {
    REQUIRE(config.string() == "(1 2 3 4 5)");
  }

  using id_ack_map = std::map<std::uint64_t, log_index>;
  SECTION("majority_config describle", "no id has acked") {
    map_ack_indexer map_ack_indexer_{id_ack_map{}};
    pro::proxy<acked_indexer_builer> indexer = pro::make_proxy<acked_indexer_builer, map_ack_indexer>(id_ack_map{});
    pro::proxy_view<acked_indexer_builer> indexer_view = &map_ack_indexer_;
    fmt::print(stderr, "{}\n", config.describe(indexer_view));
    auto expected = R"(         idx
?          0    (id=1)
?          0    (id=2)
?          0    (id=3)
?          0    (id=4)
?          0    (id=5)
)";
    fmt::print(stderr, "{}", expected);
    auto str = std::string(" ", 5);
    REQUIRE(config.describe(indexer_view) == expected);
  }

  SECTION("majority_config describle", "partion id has acked") {
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
    auto str = std::string(" ", 5);
    REQUIRE(config.describe(&indexer) == expected);
  }

  SECTION("majority_config slice") {
    REQUIRE(config.slice() == std::vector<std::uint64_t>{1, 2, 3, 4, 5});
  }

  SECTION("majority_config committed_index") {
    map_ack_indexer indexer{id_ack_map{
        std::pair<std::uint64_t, log_index>(1, 10),
        std::pair<std::uint64_t, log_index>(3, 20),
    }};
    REQUIRE(config.committed_index(&indexer) == 0);
  }
}