#include <gtest/gtest.h>

#include <array>
#include <string>
#include <vector>

#include "storage/ioutil/byte_span.h"

class byte_span_test : public ::testing::Test {
 protected:
  void SetUp() override {
    // 准备测试数据
    test_data_ = {std::byte{0x01}, std::byte{0x02}, std::byte{0x03}, std::byte{0x04}, std::byte{0x05}};

    uint8_data_ = {0x10, 0x20, 0x30, 0x40, 0x50};

    test_string_ = "Hello World";
    test_string_view_ = std::string_view("Test View");
  }

 protected:
  std::vector<std::byte> test_data_;
  std::vector<uint8_t> uint8_data_;
  std::string test_string_;
  std::string_view test_string_view_;
};

// 测试默认构造函数
TEST_F(byte_span_test, default_constructor) {
  lepton::storage::ioutil::byte_span span;
  EXPECT_EQ(span.size(), 0);
  EXPECT_EQ(span.data(), nullptr);
  EXPECT_TRUE(span.view().empty());
}

// 测试从 const void* 构造
TEST_F(byte_span_test, from_const_void_ptr) {
  lepton::storage::ioutil::byte_span span(test_data_.data(), test_data_.size());

  EXPECT_EQ(span.size(), test_data_.size());
  EXPECT_EQ(span.data(), test_data_.data());
  EXPECT_EQ(span.view().size(), test_data_.size());
}

// 测试从非 const void* 构造
TEST_F(byte_span_test, from_non_const_void_ptr) {
  std::vector<std::byte> mutable_data = test_data_;
  lepton::storage::ioutil::byte_span span(mutable_data.data(), mutable_data.size());

  EXPECT_EQ(span.size(), mutable_data.size());
  EXPECT_EQ(span.data(), mutable_data.data());
}

// 测试从 std::string 构造
TEST_F(byte_span_test, from_std_string) {
  lepton::storage::ioutil::byte_span span(test_string_);

  EXPECT_EQ(span.size(), test_string_.size());
  EXPECT_EQ(span.data(), reinterpret_cast<const std::byte*>(test_string_.data()));
  EXPECT_EQ(span.view()[0], std::byte{'H'});
}

// 测试从 std::string_view 构造
TEST_F(byte_span_test, from_std_string_view) {
  lepton::storage::ioutil::byte_span span(test_string_view_);

  EXPECT_EQ(span.size(), test_string_view_.size());
  EXPECT_EQ(span.data(), reinterpret_cast<const std::byte*>(test_string_view_.data()));
  EXPECT_EQ(span.view()[0], std::byte{'T'});
}

// 测试从 std::vector<std::byte> 构造
TEST_F(byte_span_test, from_std_vector_byte) {
  lepton::storage::ioutil::byte_span span(test_data_);

  EXPECT_EQ(span.size(), test_data_.size());
  EXPECT_EQ(span.data(), test_data_.data());

  // 验证数据内容
  for (size_t i = 0; i < test_data_.size(); ++i) {
    EXPECT_EQ(span.view()[i], test_data_[i]);
  }
}

// 测试从 std::vector<uint8_t> 构造
TEST_F(byte_span_test, from_std_vector_uint8) {
  lepton::storage::ioutil::byte_span span(uint8_data_);

  EXPECT_EQ(span.size(), uint8_data_.size());
  EXPECT_EQ(span.data(), reinterpret_cast<const std::byte*>(uint8_data_.data()));

  // 验证数据内容
  for (size_t i = 0; i < uint8_data_.size(); ++i) {
    EXPECT_EQ(span.view()[i], static_cast<std::byte>(uint8_data_[i]));
  }
}

// 测试从 std::array<std::byte> 构造
TEST_F(byte_span_test, from_std_array_byte) {
  std::array<std::byte, 3> arr = {std::byte{0xAA}, std::byte{0xBB}, std::byte{0xCC}};
  lepton::storage::ioutil::byte_span span(arr);

  EXPECT_EQ(span.size(), arr.size());
  EXPECT_EQ(span.data(), arr.data());
  EXPECT_EQ(span.view()[1], std::byte{0xBB});
}

// 测试从 std::array<uint8_t> 构造
TEST_F(byte_span_test, from_std_array_uint8) {
  std::array<uint8_t, 3> arr = {0x11, 0x22, 0x33};
  lepton::storage::ioutil::byte_span span(arr);

  EXPECT_EQ(span.size(), arr.size());
  EXPECT_EQ(span.data(), reinterpret_cast<const std::byte*>(arr.data()));
  EXPECT_EQ(span.view()[2], static_cast<std::byte>(0x33));
}

// 测试空容器的处理
TEST_F(byte_span_test, empty_containers) {
  // 空 vector
  std::vector<std::byte> empty_vec;
  lepton::storage::ioutil::byte_span span1(empty_vec);
  EXPECT_EQ(span1.size(), 0);

  // 空 string
  std::string empty_str;
  lepton::storage::ioutil::byte_span span2(empty_str);
  EXPECT_EQ(span2.size(), 0);

  // 使用显式类型转换来避免构造函数歧义
  const void* const_nullptr = nullptr;
  void* mutable_nullptr = nullptr;

  lepton::storage::ioutil::byte_span span3(const_nullptr, 0);  // 明确调用 const void* 版本
  EXPECT_EQ(span3.size(), 0);

  lepton::storage::ioutil::byte_span span4(mutable_nullptr, 0);  // 明确调用 void* 版本
  EXPECT_EQ(span4.size(), 0);
}

// 测试 view() 方法返回正确的 span
TEST_F(byte_span_test, view_method) {
  lepton::storage::ioutil::byte_span span(test_data_);
  auto view = span.view();

  EXPECT_EQ(view.data(), test_data_.data());
  EXPECT_EQ(view.size(), test_data_.size());
  EXPECT_FALSE(view.empty());
}

// 测试 data() 和 size() 方法
TEST_F(byte_span_test, data_and_size_methods) {
  lepton::storage::ioutil::byte_span span(test_string_);

  EXPECT_EQ(span.data(), reinterpret_cast<const std::byte*>(test_string_.data()));
  EXPECT_EQ(span.size(), test_string_.size());
}

// 测试常量性 - view() 返回 const span
TEST_F(byte_span_test, const_correctness) {
  std::vector<std::byte> mutable_data = test_data_;
  lepton::storage::ioutil::byte_span span(mutable_data.data(), mutable_data.size());

  // view() 应该返回 const span，不能修改
  auto view = span.view();
  static_assert(std::is_same_v<decltype(view)::element_type, const std::byte>);

  // 以下代码应该编译失败（取消注释测试）
  // view[0] = std::byte{0xFF}; // 应该编译错误
}

// 测试边界情况
TEST_F(byte_span_test, edge_cases) {
  // 单元素容器
  std::vector<std::byte> single = {std::byte{0x42}};
  lepton::storage::ioutil::byte_span span1(single);
  EXPECT_EQ(span1.size(), 1);
  EXPECT_EQ(span1.view()[0], std::byte{0x42});

  // 大尺寸容器（在实际测试中可能需要调整）
  std::vector<std::byte> large(1000, std::byte{0xAA});
  lepton::storage::ioutil::byte_span span2(large);
  EXPECT_EQ(span2.size(), 1000);
}

// 测试从不同字符串编码构造
TEST_F(byte_span_test, different_string_encodings) {
  // ASCII 字符串
  std::string ascii = "ASCII";
  lepton::storage::ioutil::byte_span span1(ascii);
  EXPECT_EQ(span1.size(), 5);

  // UTF-8 字符串
  std::string utf8 = "你好世界";
  lepton::storage::ioutil::byte_span span2(utf8);
  EXPECT_EQ(span2.size(), utf8.size());

  // 包含空字符的字符串
  std::string with_null = "test\0with\0null";
  with_null = std::string("test\0with\0null", 13);
  lepton::storage::ioutil::byte_span span3(with_null);
  EXPECT_EQ(span3.size(), 13);
}