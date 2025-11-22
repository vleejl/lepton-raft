#ifndef _LEPTON_FIXED_BYTE_BUFFER_H_
#define _LEPTON_FIXED_BYTE_BUFFER_H_
#include <cstddef>
#include <memory>
#include <span>

namespace lepton {

class fixed_byte_buffer {
 public:
  // 构造一个固定大小的 buffer
  explicit fixed_byte_buffer(std::size_t size) : size_(size), buf_(std::make_unique<std::byte[]>(size)) {}

  // 禁止拷贝
  fixed_byte_buffer(const fixed_byte_buffer&) = delete;
  fixed_byte_buffer& operator=(const fixed_byte_buffer&) = delete;

  // 允许移动
  fixed_byte_buffer(fixed_byte_buffer&&) noexcept = default;
  fixed_byte_buffer& operator=(fixed_byte_buffer&&) noexcept = default;

  // 返回 buffer 的大小
  std::size_t size() const noexcept { return size_; }

  // 返回可读写的 std::span
  std::span<std::byte> span() noexcept { return {buf_.get(), size_}; }
  std::span<const std::byte> span() const noexcept { return {buf_.get(), size_}; }

  // 访问裸指针（少用，主要给底层接口）
  std::byte* data() noexcept { return buf_.get(); }
  const std::byte* data() const noexcept { return buf_.get(); }

  // 清空 buffer（可选）
  void clear() noexcept { std::fill_n(buf_.get(), size_, std::byte{0}); }

 private:
  std::size_t size_;
  std::unique_ptr<std::byte[]> buf_;
};

}  // namespace lepton

#endif  // _LEPTON_FIXED_BYTE_BUFFER_H_
