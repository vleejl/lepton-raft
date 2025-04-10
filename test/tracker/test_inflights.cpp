#include <gtest/gtest.h>

#include <cstdint>
#include <deque>
#include <vector>

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
      in.add(i, 100 + i);
    }
    ASSERT_EQ(5, in.count());
    ASSERT_EQ(10, in.capacity());
    ASSERT_EQ(510, in.bytes());
    ASSERT_EQ(0, in.max_bytes());
    std::deque<inflights_data> expect_buffer1 = {{0, 100}, {1, 101}, {2, 102}, {3, 103}, {4, 104}};
    ASSERT_EQ(expect_buffer1, in.buffer_view());

    for (std::uint64_t i = 5; i < 10; ++i) {
      in.add(i, 100 + i);
    }
    ASSERT_EQ(10, in.count());
    ASSERT_EQ(10, in.capacity());
    ASSERT_EQ(1045, in.bytes());
    ASSERT_EQ(0, in.max_bytes());
    std::deque<inflights_data> expect_buffer2 = {{0, 100}, {1, 101}, {2, 102}, {3, 103}, {4, 104},
                                                 {5, 105}, {6, 106}, {7, 107}, {8, 108}, {9, 109}};
    ASSERT_EQ(expect_buffer2, in.buffer_view());
  }

  SECTION("deque is full") {
    inflights in2{10};
    for (std::uint64_t i = 0; i < 5; ++i) {
      in2.add(0, 0);
    }
    for (std::uint64_t i = 0; i < 5; ++i) {
      in2.add(i, 100 + i);
    }
    ASSERT_EQ(10, in2.count());
    ASSERT_EQ(10, in2.capacity());
    ASSERT_EQ(510, in2.bytes());
    ASSERT_EQ(0, in2.max_bytes());
    std::deque<inflights_data> expect_buffer21 = {{0, 0},   {0, 0},   {0, 0},   {0, 0},   {0, 0},
                                                  {0, 100}, {1, 101}, {2, 102}, {3, 103}, {4, 104}};
    ASSERT_EQ(expect_buffer21, in2.buffer_view());

    for (std::uint64_t i = 5; i < 10; ++i) {
      EXPECT_DEATH(in2.add(i, 100 + i), "");
    }
    ASSERT_EQ(10, in2.count());
    ASSERT_EQ(10, in2.capacity());
    ASSERT_EQ(510, in2.bytes());
    ASSERT_EQ(0, in2.max_bytes());
    ASSERT_EQ(expect_buffer21, in2.buffer_view());
  }
}

TEST_F(inflights_test_suit, free_to) {
  inflights in{10};
  for (std::uint64_t i = 0; i < 10; ++i) {
    in.add(i, 100 + i);
  }
  in.free_le(0);
  ASSERT_EQ(9, in.count());
  ASSERT_EQ(10, in.capacity());
  ASSERT_EQ(945, in.bytes());
  ASSERT_EQ(0, in.max_bytes());
  std::deque<inflights_data> expect_buffer1 = {{1, 101}, {2, 102}, {3, 103}, {4, 104}, {5, 105},
                                               {6, 106}, {7, 107}, {8, 108}, {9, 109}};
  ASSERT_EQ(expect_buffer1, in.buffer_view());

  in.free_le(4);
  ASSERT_EQ(5, in.count());
  ASSERT_EQ(10, in.capacity());
  ASSERT_EQ(535, in.bytes());
  ASSERT_EQ(0, in.max_bytes());
  std::deque<inflights_data> expect_buffer2 = {{5, 105}, {6, 106}, {7, 107}, {8, 108}, {9, 109}};
  ASSERT_EQ(expect_buffer2, in.buffer_view());

  in.free_le(8);
  ASSERT_EQ(1, in.count());
  ASSERT_EQ(10, in.capacity());
  ASSERT_EQ(109, in.bytes());
  ASSERT_EQ(0, in.max_bytes());
  std::deque<inflights_data> expect_buffer3 = {{9, 109}};
  ASSERT_EQ(expect_buffer3, in.buffer_view());

  for (std::uint64_t i = 10; i < 15; ++i) {
    in.add(i, 100 + i);
  }
  in.free_le(12);
  ASSERT_EQ(2, in.count());
  ASSERT_EQ(10, in.capacity());
  ASSERT_EQ(227, in.bytes());
  ASSERT_EQ(0, in.max_bytes());
  std::deque<inflights_data> expect_buffer4 = {{13, 113}, {14, 114}};
  ASSERT_EQ(expect_buffer4, in.buffer_view());

  in.free_le(14);
  ASSERT_EQ(0, in.count());
  ASSERT_EQ(10, in.capacity());
  ASSERT_EQ(0, in.bytes());
  ASSERT_EQ(0, in.max_bytes());
  std::deque<inflights_data> expect_buffer5 = {};
  ASSERT_EQ(expect_buffer5, in.buffer_view());
}

TEST_F(inflights_test_suit, full) {
  struct test_case {
    std::string name;
    std::size_t size;
    std::uint64_t max_bytes;
    std::size_t full_at;
    std::size_t free_le;
    std::size_t again_at;
  };
  std::vector<test_case> test_cases = {
      {"always-full", 0, 0, 0, 0, 0},
      {"single-entry", 1, 0, 1, 1, 2},
      {"single-entry-overflow", 1, 10, 1, 1, 2},
      {"multi-entry", 15, 0, 15, 6, 22},
      {"single-overflow", 8, 400, 4, 2, 7},
      {"exact-max-bytes", 8, 406, 4, 3, 8},
      {"larger-overflow", 15, 408, 5, 1, 6},
  };
  for (const auto &test : test_cases) {
    inflights in{test.size, test.max_bytes};

    auto add_until_full = [&in](std::size_t begin, std::size_t end) {
      for (std::uint64_t i = begin; i < end; ++i) {
        ASSERT_FALSE(in.full());
        in.add(i, 100 + i);
      }
      ASSERT_TRUE(in.full());
    };

    add_until_full(0, test.full_at);
    in.free_le(test.free_le);
    add_until_full(test.full_at, test.again_at);
    EXPECT_DEATH(in.add(100, 1024), "");
  }
}

TEST_F(inflights_test_suit, reset) {
  inflights in{10, 1000};
  // Imitate a semi-realistic flow during which the inflight tracker is
	// periodically reset to empty. Byte usage must not "leak" across resets.
  std::uint64_t index = 0;
  for (int i = 0; i < 100; ++i) {
    in.reset();
    // Add 5 messages. They should not max out the limit yet.
    for (int j = 0; j < 5; ++j) {
      ASSERT_FALSE(in.full());
      index++;
      in.add(index, 16);
    }
    // Ack all but last 2 indices.
    in.free_le(index - 2);
    ASSERT_FALSE(in.full());
    ASSERT_EQ(2, in.count());
  }
  in.free_le(index);
  ASSERT_EQ(0, in.count());
}