#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <leaf.hpp>
#include <stdexcept>

#include "lepton_error.h"
using namespace lepton;
// 模拟存储类
class Storage {
 public:
  leaf::result<std::uint64_t> term(int i) {
    if (i < 0) {
      return new_error(storage_error::COMPACTED, "Compacted error");
    }
    if (i == 0) {
      return new_error(logic_error::INVALID_PARAM);
    }
    return static_cast<std::uint64_t>(i);
  }
};

// 测试函数
leaf::result<std::uint64_t> test_function(Storage& storage, int i, bool& has_repeat_error) {
  return leaf::try_handle_some(
      [&]() -> leaf::result<std::uint64_t> {
        BOOST_LEAF_AUTO(v, storage.term(i));
        return v;
      },
      [&](const lepton_error& e) -> leaf::result<std::uint64_t> {
        if (e.err_code == storage_error::COMPACTED || e.err_code == storage_error::UNAVAILABLE) {
          throw std::runtime_error("target error code");
        }
        has_repeat_error = true;
        printf("need new error\n");
        return leaf::new_error(e);
      });
}

// GTest 用例
TEST(boost_leaaf_try, ThrowsOnCompactedError) {
  Storage storage;
  auto has_repeat_error = false;
  EXPECT_THROW({ test_function(storage, -1, has_repeat_error).value(); }, std::runtime_error);
  EXPECT_FALSE(has_repeat_error);
}

TEST(boost_leaaf_try, unknown_error) {
  Storage storage;
  auto has_repeat_error = false;
  auto result = test_function(storage, 0, has_repeat_error);
  EXPECT_TRUE(result.has_error());
  EXPECT_TRUE(has_repeat_error);
}

TEST(boost_leaaf_try, ReturnsValidValue) {
  Storage storage;
  auto has_repeat_error = false;
  EXPECT_EQ(test_function(storage, 10, has_repeat_error).value(), 10);
  EXPECT_FALSE(has_repeat_error);
}
