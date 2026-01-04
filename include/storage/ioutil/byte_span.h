#pragma once
#ifndef _LEPTON_BYTE_SPAN_H_
#define _LEPTON_BYTE_SPAN_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "storage/ioutil/fixed_byte_buffer.h"

namespace lepton::storage::ioutil {

// 保留 std::span 原生语义
using byte_span = std::span<const std::byte>;

// ---------- free function 转换器 ----------

// raw pointer
inline byte_span to_bytes(const void* data, size_t size) { return {reinterpret_cast<const std::byte*>(data), size}; }

inline byte_span to_bytes(void* data, size_t size) { return {reinterpret_cast<std::byte*>(data), size}; }

// std::string / std::string_view / C-string
inline byte_span to_bytes(const std::string& s) { return {reinterpret_cast<const std::byte*>(s.data()), s.size()}; }

inline byte_span to_bytes(std::string_view s) { return {reinterpret_cast<const std::byte*>(s.data()), s.size()}; }

inline byte_span to_bytes(const char* s) { return s ? to_bytes(std::string_view(s)) : byte_span{}; }

// std::vector
inline byte_span to_bytes(const std::vector<std::byte>& v) { return {v.data(), v.size()}; }

inline byte_span to_bytes(const std::vector<std::uint8_t>& v) {
  return {reinterpret_cast<const std::byte*>(v.data()), v.size()};
}

// std::array
template <size_t N>
inline byte_span to_bytes(const std::array<std::byte, N>& arr) {
  return {arr.data(), N};
}

template <size_t N>
inline byte_span to_bytes(const std::array<std::uint8_t, N>& arr) {
  return {reinterpret_cast<const std::byte*>(arr.data()), N};
}

// fixed_byte_buffer
inline byte_span to_bytes(const fixed_byte_buffer& buf) { return {buf.data(), buf.size()}; }

}  // namespace lepton::storage::ioutil

#endif  // _LEPTON_BYTE_SPAN_H_
