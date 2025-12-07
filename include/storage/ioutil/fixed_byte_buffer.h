#ifndef _LEPTON_FIXED_BYTE_BUFFER_H_
#define _LEPTON_FIXED_BYTE_BUFFER_H_
#include <asio/buffer.hpp>
#include <cstddef>
#include <span>
#include <vector>

namespace lepton::storage::ioutil {

class fixed_byte_buffer {
 public:
  // 构造一个固定大小的 buffer
  explicit fixed_byte_buffer(std::size_t size) : buf_(size) {}

  // 禁止拷贝
  fixed_byte_buffer(const fixed_byte_buffer&) = delete;
  fixed_byte_buffer& operator=(const fixed_byte_buffer&) = delete;

  // 允许移动
  fixed_byte_buffer(fixed_byte_buffer&&) noexcept = default;
  fixed_byte_buffer& operator=(fixed_byte_buffer&&) noexcept = default;

  // 返回 buffer 的大小
  std::size_t size() const noexcept { return buf_.size(); }

  auto empty() const noexcept { return buf_.empty(); }

  // 返回可读写的 std::span
  std::span<std::byte> span() noexcept { return {buf_.data(), buf_.size()}; }
  std::span<const std::byte> span() const noexcept { return {buf_.data(), buf_.size()}; }

  // 访问裸指针（少用，主要给底层接口）
  std::byte* data() noexcept { return buf_.data(); }
  const std::byte* data() const noexcept { return buf_.data(); }

  // 清空 buffer（可选）
  void clear() noexcept { std::fill(buf_.begin(), buf_.end(), std::byte{0}); }

  // 实现类似数组的访问操作符 []
  std::byte& operator[](std::size_t index) { return buf_[index]; }

  const std::byte& operator[](std::size_t index) const { return buf_[index]; }

  // 可选：添加 at() 方法，提供边界检查（与标准库容器一致）
  std::byte& at(std::size_t index) { return buf_.at(index); }

  const std::byte& at(std::size_t index) const { return (*this)[index]; }

  asio::mutable_buffer asio_mutable_buffer() { return asio::mutable_buffer(buf_.data(), buf_.size()); }

  // 迭代器支持
  auto begin() noexcept { return buf_.begin(); }
  auto begin() const noexcept { return buf_.begin(); }
  auto cbegin() const noexcept { return buf_.cbegin(); }
  auto end() noexcept { return buf_.end(); }
  auto end() const noexcept { return buf_.end(); }
  auto cend() const noexcept { return buf_.cend(); }

  // 反向迭代器
  auto rbegin() noexcept { return buf_.rbegin(); }
  auto rbegin() const noexcept { return buf_.rbegin(); }
  auto crbegin() const noexcept { return buf_.crbegin(); }
  auto rend() noexcept { return buf_.rend(); }
  auto rend() const noexcept { return buf_.rend(); }
  auto crend() const noexcept { return buf_.crend(); }

 private:
  std::vector<std::byte> buf_;
};

}  // namespace lepton::storage::ioutil

#endif  // _LEPTON_FIXED_BYTE_BUFFER_H_
