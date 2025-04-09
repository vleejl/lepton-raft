#include <gtest/gtest.h>

#include <cstdint>
#include <deque>

#include "gtest/gtest.h"
#include "inflights.h"
#include "utility_macros_test.h"
using namespace lepton;
using namespace lepton::tracker;

class inflights_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

TEST_F(inflights_test_suit, add) {
  // no rotating case
  SECTION("no rotating case") {
    inflights in{10, 510};
    for (std::uint64_t i = 0; i < 5; ++i) {
      in.add(i, 100 + i);
    }
    ASSERT_EQ(5, in.count());
    ASSERT_EQ(10, in.capacity());
    std::deque<inflights_data> expect_buffer1 = {{0, 100}, {1, 101}, {2, 102}, {3, 103}, {4, 104}};
    ASSERT_EQ(expect_buffer1, in.buffer_view());

    for (std::uint64_t i = 5; i < 10; ++i) {
      in.add(i, 100 + i);
    }
    ASSERT_EQ(10, in.count());
    ASSERT_EQ(10, in.capacity());
    std::deque<inflights_data> expect_buffer2 = {{0, 100}, {1, 101}, {2, 102}, {3, 103}, {4, 104},
                                                 {5, 105}, {6, 106}, {7, 107}, {8, 108}, {9, 109}};
    ASSERT_EQ(expect_buffer2, in.buffer_view());
  }

  SECTION("deque is full") {
    inflights in2{10, 510};
    for (std::uint64_t i = 0; i < 5; ++i) {
      in2.add(0, 0);
    }
    for (std::uint64_t i = 0; i < 5; ++i) {
      in2.add(i, 100 + i);
    }
    ASSERT_EQ(10, in2.count());
    ASSERT_EQ(10, in2.capacity());
    std::deque<inflights_data> expect_buffer21 = {{0, 0},   {0, 0},   {0, 0},   {0, 0},   {0, 0},
                                                  {0, 100}, {1, 101}, {2, 102}, {3, 103}, {4, 104}};
    ASSERT_EQ(expect_buffer21, in2.buffer_view());

    for (std::uint64_t i = 5; i < 10; ++i) {
      EXPECT_DEATH(in2.add(i, 100 + i), "");
    }
    ASSERT_EQ(10, in2.count());
    ASSERT_EQ(10, in2.capacity());
    ASSERT_EQ(expect_buffer21, in2.buffer_view());
  }
}
