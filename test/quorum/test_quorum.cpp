#include <catch2/catch_test_macros.hpp>
#include <iostream>
#include <map>

#include "error.h"
#include "leaf.hpp"
#include "proxy.h"
#include "quorum.h"

using namespace lepton;
using namespace lepton::quorum;

TEST_CASE("test proxy", "proxy") {
  SECTION("test proxy", "proxy") {
    //    std::map<int, int> v;

    // pro::proxy_view<FMap<int, int>> p1 = &v;
    // static_assert(std::is_same_v<decltype(p1->at(1)), int&>);
    // p1->at(1) = 3;
    // printf("%d\n", v.at(1));  // Prints "3"

    // pro::proxy_view<const FMap<int, int>> p2 = &std::as_const(v);
    // static_assert(std::is_same_v<decltype(p2->at(1)), const int&>);
    // // p2->at(1) = 4; won't compile
    // printf("%d\n", p2->at(1));  // Prints "3"

    // Rectangle rectangle(3, 5);
    {
      //  pro::proxy<Drawable> p = pro::make_proxy<Drawable, Rectangle>(3, 5);
      //     pro::proxy_view<Drawable> p2 =  p;
      //   std::string str = PrintDrawableToString(p2);
      //   std::cout << str << "\n";  // Prints "entity = {Rectangle: width = 3,
      //   height = 5}, area = 15";
    }

    int a = 1;
    // pro::proxy<details::TestFacade> p1 =
    // pro::make_proxy<details::TestFacade>(123);
    // pro::proxy_view<details::TestFacade> p2 = p1;

    // map_ack_indexer map_ack_indexer_{id_ack_map{}};
    // using id_ack_map = std::map<std::uint64_t, log_index>;
    // pro::proxy<acked_indexer_builer> indexer =
    // pro::make_proxy<acked_indexer_builer,
    // map_ack_indexer>(std::map<std::uint64_t, log_index>{});
    // pro::proxy_view<acked_indexer_builer> indexer_view = indexer;

    {
      // pro::proxy<Drawable> indexer = pro::make_proxy<Drawable,
      // map_ack_indexer>(3, 5); pro::proxy_view<Drawable> p2 =  indexer;
    }

    {
      pro::proxy<acked_indexer_builer> indexer =
          pro::make_proxy<acked_indexer_builer, map_ack_indexer>(
              std::map<std::uint64_t, log_index>{});
      pro::proxy_view<acked_indexer_builer> p2 = indexer;
      // std::stringstream result;
      // p2->Draw(result);
      // std::cout << result.str() << std::endl;
    }
  }
}