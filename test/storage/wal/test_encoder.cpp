#include <gtest/gtest.h>

#include "storage/wal/encoder.h"

using namespace lepton::storage::wal;

class encoder_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

TEST_F(encoder_test_suit, no_padding_when_aligned) {
  const std::uint64_t size = 16;
  const auto [len_field, pad] = encode_frame_size(size);

  EXPECT_EQ(pad, 0u);
  EXPECT_EQ(len_field, size);
}

TEST_F(encoder_test_suit, padding_when_unaligned) {
  const std::uint64_t size = 10;  // needs 6 bytes padding
  const auto [len_field, pad] = encode_frame_size(size);

  EXPECT_EQ(pad, 6u);

  const std::uint64_t payload_len = len_field & ((1ULL << 56) - 1);
  const std::uint8_t flag = static_cast<std::uint8_t>(len_field >> 56);

  EXPECT_EQ(payload_len, size);
  EXPECT_EQ(flag & 0x80, 0x80);  // padding flag
  EXPECT_EQ(flag & 0x07, pad);   // pad count
}

TEST_F(encoder_test_suit, encode_frame_size) {}