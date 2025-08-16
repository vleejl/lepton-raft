#include <gtest/gtest.h>

#include <asio.hpp>
#include <cstdint>
#include <cstdio>
#include <expected>
#include <stdexcept>
#include <system_error>
#include <tl/expected.hpp>

#include "asio/awaitable.hpp"
#include "leaf.h"
#include "leaf_expected.h"
#include "lepton_error.h"
#include "raft.pb.h"
#include "storage_error.h"
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
TEST(boost_leaaf_try, throws_on_compacted_error) {
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

TEST(boost_leaaf_try, returns_valid_value) {
  Storage storage;
  auto has_repeat_error = false;
  EXPECT_EQ(test_function(storage, 10, has_repeat_error).value(), 10);
  EXPECT_FALSE(has_repeat_error);
}

TEST(boost_leaaf_try, test_normal_code_with_leaf) {
  auto expected_err = lepton::make_error_code(raft_error::STOPPED);
  auto func = []() -> leaf::result<void> { return lepton::new_error(lepton::raft_error::STOPPED); };

  {
    std::error_code step_err_code1;
    auto _ = leaf::try_handle_some(
        [&]() -> leaf::result<void> {
          LEPTON_LEAF_CHECK(func());
          assert(false);  // unreachable
          return {};
        },
        [&](const lepton_error& e) -> leaf::result<void> {
          step_err_code1 = e.err_code;
          return {};
        });
    EXPECT_EQ(expected_err, step_err_code1);
  }

  {
    // boost leaf 仅支持通过 try_handle_some 获取错误类型对象
    std::error_code step_err_code;
    auto step_result = func();
    EXPECT_FALSE(step_result);
    auto _ = leaf::try_handle_some(
        [&]() -> leaf::result<void> {
          LEPTON_LEAF_CHECK(step_result);
          assert(false);  // unreachable
          return {};
        },
        [&](const lepton_error& e) -> leaf::result<void> {
          step_err_code = e.err_code;
          return {};
        });
    EXPECT_NE(expected_err, step_err_code);
  }
}

// boost leaf 不支持协程
TEST(boost_leaaf_try, test_asio_error_code_with_leaf) {
  auto expected_err = lepton::make_error_code(raft_error::STOPPED);
  auto func = []() -> asio::awaitable<leaf::result<void>> { co_return lepton::new_error(lepton::raft_error::STOPPED); };
  asio::io_context io;
  auto expected_fut = asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        std::error_code step_err_code;
        auto result = co_await func();
        auto _ = leaf::try_handle_some(
            [&]() -> leaf::result<void> {
              if (!result) {
                return result.error();
              }
              assert(false);  // unreachable
              return {};
            },
            [&](const lepton_error& e) -> leaf::result<void> {
              step_err_code = e.err_code;
              return {};
            });
        EXPECT_NE(expected_err, step_err_code);
        co_return;
      },
      asio::use_future);
  io.run();
  expected_fut.get();
}

TEST(boost_leaaf_try, test_asio_error_code_with_tl_expected) {
  auto expected_err = lepton::make_error_code(raft_error::STOPPED);
  auto func = []() -> asio::awaitable<tl::expected<void, std::error_code>> {
    co_return tl::unexpected{lepton::raft_error::STOPPED};
  };
  asio::io_context io;
  auto expected_fut = asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        auto result = co_await func();
        EXPECT_FALSE(result);
        EXPECT_EQ(expected_err, result.error());
        co_return;
      },
      asio::use_future);
  io.run();
  expected_fut.get();
}

TEST(boost_leaaf_try, test_leaf_result_to_expected) {
  auto expected_err = lepton::make_error_code(raft_error::STOPPED);
  auto func = []() -> leaf::result<raftpb::message> { return lepton::new_error(lepton::raft_error::STOPPED); };

  auto result = lepton::leaf_to_expected([&]() -> leaf::result<raftpb::message> {
    BOOST_LEAF_AUTO(m, func());
    return m;
  });

  ASSERT_FALSE(result.has_value());
  ASSERT_EQ(expected_err, result.error());
}

TEST(boost_leaaf_try, test_leaf_result_to_expected_void) {
  auto expected_err = lepton::make_error_code(raft_error::STOPPED);
  auto func = []() -> leaf::result<void> { return lepton::new_error(lepton::raft_error::STOPPED); };

  auto result = lepton::leaf_to_expected_void([&]() -> leaf::result<void> {
    LEPTON_LEAF_CHECK(func());
    return {};
  });

  ASSERT_FALSE(result.has_value());
  ASSERT_EQ(expected_err, result.error());
}