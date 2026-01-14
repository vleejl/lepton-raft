#pragma once
#ifndef _LEPTON_ERROR_H_
#define _LEPTON_ERROR_H_
#include <asio.hpp>
#include <cassert>
#include <source_location>
#include <string>
#include <system_error>
#include <tl/expected.hpp>

#include "error/leaf.h"  // IWYU pragma: keep

namespace lepton {

std::error_code make_error_code();

template <typename T>
concept can_make_error_code = requires(T t) {
  // 关键：通过 ADL 找到自定义 std::error_code 里的 make_error_code 函数定义
  { make_error_code(t) } -> std::same_as<std::error_code>;
};

template <typename T>
concept lepton_err_types = can_make_error_code<T> || std::is_error_code_enum_v<T>;

template <typename T>
concept err_types = lepton_err_types<T> || std::is_same_v<T, std::error_code> || std::is_same_v<T, asio::error_code>;

struct lepton_error {
  std::error_code err_code;
  std::string message;
  std::source_location location;

  lepton_error(std::error_code code, std::source_location location) : err_code(code), location(location) {}

  lepton_error(std::error_code code, std::string msg, std::source_location location)
      : err_code(code), message(std::move(msg)), location(location) {}

  template <lepton_err_types err_type>
  lepton_error(err_type code, std::source_location location) : err_code(make_error_code(code)), location(location) {}

  template <lepton_err_types err_type>
  lepton_error(err_type code, std::string msg, std::source_location location)
      : err_code(make_error_code(code)), message(std::move(msg)), location(location) {}

  template <lepton_err_types err_type>
  auto operator<=>(err_type error_code) const {
    return err_code <=> make_error_code(error_code);
  }

  auto operator<=>(const std::error_code& rhs_err_code) const { return err_code <=> rhs_err_code; }
};

template <err_types err_type>
bool operator==(const lepton_error& error, const err_type& code) {
  return error.err_code == code;
}

template <err_types err_type>
bool operator==(const err_type& code, const lepton_error& error) {
  return code == error.err_code;
}

inline bool operator==(const lepton_error& error, const std::error_code& code) { return error.err_code == code; }

inline bool operator==(const std::error_code& code, const lepton_error& error) { return code == error.err_code; }

template <err_types err_type>
bool operator==(const std::error_code& err_code, const err_type& code) {
  return err_code == make_error_code(code);
}

template <err_types err_type>
bool operator==(const err_type& code, const std::error_code& err_code) {
  return err_code == make_error_code(code);
}

template <err_types err_type>
bool operator!=(const std::error_code& err_code, const err_type& code) {
  return err_code != make_error_code(code);
}

template <err_types err_type>
bool operator!=(const err_type& code, const std::error_code& err_code) {
  return err_code != make_error_code(code);
}

template <err_types error_code_type, typename error_msg_type>
auto new_error(error_code_type code, error_msg_type&& msg,
               std::source_location location = std::source_location::current()) {
  return boost::leaf::new_error(
      lepton_error{code, std::string(std::forward<error_msg_type>(msg)), std::move(location)});
}

template <err_types error_code_type>
auto new_error(error_code_type code, std::source_location location = std::source_location::current()) {
  return boost::leaf::new_error(lepton_error{code, std::move(location)});
}

inline auto new_error(const lepton::lepton_error& err) { return boost::leaf::new_error(err); }

template <err_types error_code_type>
inline auto unexpected(error_code_type error_code) {
  return tl::unexpected(make_error_code(error_code));
}

}  // namespace lepton

#endif  // _LEPTON_ERROR_H_