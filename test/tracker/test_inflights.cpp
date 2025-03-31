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
    inflights in{10};
    for (std::uint64_t i = 0; i < 5; ++i) {
      in.add(i);
    }
    ASSERT_EQ(5, in.count());
    ASSERT_EQ(10, in.capacity());
    std::deque<std::uint64_t> expect_buffer1 = {0, 1, 2, 3, 4};
    ASSERT_EQ(expect_buffer1, in.buffer_view());

    for (std::uint64_t i = 5; i < 10; ++i) {
      in.add(i);
    }
    ASSERT_EQ(10, in.count());
    ASSERT_EQ(10, in.capacity());
    std::deque<std::uint64_t> expect_buffer2 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    ASSERT_EQ(expect_buffer2, in.buffer_view());
  }

  SECTION("deque is full") {
    inflights in2{10};
    for (std::uint64_t i = 0; i < 5; ++i) {
      in2.add(0);
    }
    for (std::uint64_t i = 0; i < 5; ++i) {
      in2.add(i);
    }
    ASSERT_EQ(10, in2.count());
    ASSERT_EQ(10, in2.capacity());
    std::deque<std::uint64_t> expect_buffer21 = {0, 0, 0, 0, 0, 0, 1, 2, 3, 4};
    ASSERT_EQ(expect_buffer21, in2.buffer_view());

    for (std::uint64_t i = 5; i < 10; ++i) {
      EXPECT_DEATH(in2.add(i), "");
    }
    ASSERT_EQ(10, in2.count());
    ASSERT_EQ(10, in2.capacity());
    std::deque<std::uint64_t> expect_buffer23 = {0, 0, 0, 0, 0, 0, 1, 2, 3, 4};
    ASSERT_EQ(expect_buffer23, in2.buffer_view());
  }
}

// Test case 1: 测试 inflights 类的初始化和基本功能
TEST_F(inflights_test_suit, initialization_and_add) {
  inflights inf(3);  // 创建一个容量为 3 的 inflights

  // 模拟 Section 1
  SECTION("Section 1 - Initialization Check") {
    // 测试空容器
    EXPECT_TRUE(inf.empty());
    EXPECT_EQ(inf.count(), 0);
    EXPECT_EQ(inf.capacity(), 3);  // 测试容量
  }

  // 模拟 Section 2
  SECTION("Section 2 - Adding Elements") {
    // 测试 add() 方法
    inf.add(100);
    EXPECT_FALSE(inf.empty());
    EXPECT_EQ(inf.count(), 1);
    EXPECT_EQ(inf.buffer_view().back(), 100);

    // 测试添加到满的情况
    inf.add(101);
    inf.add(102);
    EXPECT_TRUE(inf.full());

    // 不能再添加更多元素
    EXPECT_DEATH(inf.add(103), "");
    EXPECT_EQ(inf.count(), 3);  // 没有增加新元素
  }
}

// Test case 2: 测试 free_le 方法
TEST_F(inflights_test_suit, free_le) {
  inflights inf(3);
  inf.add(100);
  inf.add(101);
  inf.add(102);

  // 模拟 Section 1
  SECTION("Section 1 - Freeing Elements Less than 101") {
    // 释放小于等于 100 的元素
    inf.free_le(100);
    EXPECT_EQ(inf.count(), 2);                  // 101、102 应该被保留
    EXPECT_EQ(inf.buffer_view().front(), 101);  // 应该是 101
  }

  // 模拟 Section 2
  SECTION("Section 2 - Freeing Elements Less than 102") {
    // 释放小于等于 101 的元素
    inf.free_le(101);
    EXPECT_EQ(inf.count(), 1);  // 只剩 102
    EXPECT_EQ(inf.buffer_view().front(), 102);
  }
}

// Test case 3: 测试 free_first_one 方法
TEST_F(inflights_test_suit, free_first_one) {
  inflights inf(3);
  inf.add(100);
  inf.add(101);
  inf.add(102);

  // 模拟 Section 1
  SECTION("Section 1 - Free First Element") {
    // 释放第一个元素
    inf.free_first_one();
    EXPECT_EQ(inf.count(), 2);
    EXPECT_EQ(inf.buffer_view().front(), 101);
  }

  // 模拟 Section 2
  SECTION("Section 2 - Free First Element Again") {
    // 再次释放第一个元素
    inf.free_first_one();
    EXPECT_EQ(inf.count(), 1);
    EXPECT_EQ(inf.buffer_view().front(), 102);
  }
}

// Test case 4: 测试 reset 方法
TEST_F(inflights_test_suit, reset) {
  inflights inf(3);
  inf.add(100);
  inf.add(101);

  // 模拟 Section 1
  SECTION("Section 1 - Reset the Buffer") {
    // 测试 reset 方法
    inf.reset();
    EXPECT_TRUE(inf.empty());
    EXPECT_EQ(inf.count(), 0);
  }
}
