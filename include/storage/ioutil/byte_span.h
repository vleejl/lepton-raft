#ifndef _LEPTON_BYTE_SPAN_H_
#define _LEPTON_BYTE_SPAN_H_
#include <array>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "fixed_byte_buffer.h"
namespace lepton {

class byte_span {
 public:
  byte_span() = default;

  // from const raw ptr
  byte_span(const void* data, size_t size) : view_(static_cast<const std::byte*>(data), size) {}

  // from non-const raw ptr
  byte_span(void* data, size_t size) : view_(static_cast<std::byte*>(data), size) {}

  // from std::string
  byte_span(const std::string& s) : view_(reinterpret_cast<const std::byte*>(s.data()), s.size()) {}

  // from std::string_view
  byte_span(std::string_view s) : view_(reinterpret_cast<const std::byte*>(s.data()), s.size()) {}

  // from std::vector<std::byte>
  byte_span(const std::vector<std::byte>& v) : view_(v.data(), v.size()) {}

  // from std::vector<uint8_t>
  byte_span(const std::vector<std::uint8_t>& v) : view_(reinterpret_cast<const std::byte*>(v.data()), v.size()) {}

  // from std::array<std::byte>
  template <size_t N>
  byte_span(std::array<std::byte, N>& arr) : view_(arr.data(), N) {}

  // from std::array<std::byte>
  template <size_t N>
  byte_span(const std::array<std::uint8_t, N>& arr) : view_(reinterpret_cast<const std::byte*>(arr.data()), N) {}

  byte_span(const fixed_byte_buffer& buf) : view_(buf.data(), buf.size()) {}

  std::span<const std::byte> view() const { return view_; }

  const std::byte* data() const { return view_.data(); }

  size_t size() const { return view_.size(); }

 private:
  std::span<const std::byte> view_;
};

}  // namespace lepton

#endif  // _LEPTON_BYTE_SPAN_H_
