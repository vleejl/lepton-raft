#ifndef _LEPTON_ERROR_H_
#define _LEPTON_ERROR_H_
#include <asio.hpp>
#include <cassert>
#include <source_location>
#include <string>
#include <system_error>

#include "leaf.hpp"
#include "logic_error.h"
#include "raft_error.h"
#include "storage_error.h"
namespace lepton {
namespace leaf {
using namespace boost::leaf;
template <typename T>
using result = boost::leaf::result<T>;
}  // namespace leaf

template <typename error_code_type>
concept err_types =
    std::is_same_v<error_code_type, lepton::logic_error> || std::is_same_v<error_code_type, lepton::raft_error> ||
    std::is_same_v<error_code_type, lepton::storage_error> || std::is_same_v<error_code_type, asio::error_code>;
struct lepton_error {
  std::error_code err_code;
  std::string message;
  std::source_location location;

  template <err_types err_type>
  lepton_error(err_type code, std::source_location location) : err_code(make_error_code(code)), location(location) {}

  template <err_types err_type>
  lepton_error(err_type code, std::string msg, std::source_location location)
      : err_code(make_error_code(code)), message(std::move(msg)), location(location) {}

  template <err_types err_type>
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
  return err_code == make_error_code(code);
}

template <err_types err_type>
bool operator!=(const err_type& code, const std::error_code& err_code) {
  return err_code == make_error_code(code);
}

template <err_types error_code_type, typename error_msg_type>
auto new_error(error_code_type code, error_msg_type&& msg,
               std::source_location location = std::source_location::current()) {
  return leaf::new_error(lepton_error{code, std::string(std::forward<error_msg_type>(msg)), std::move(location)});
}

template <err_types error_code_type>
auto new_error(error_code_type code, std::source_location location = std::source_location::current()) {
  return leaf::new_error(lepton_error{code, std::move(location)});
}

inline auto new_error(const lepton::lepton_error& err) { return leaf::new_error(err); }

// template <typename T, typename F>
// expected<T> leaf_to_expected(F&& fn) {
//   std::error_code ec;

//   auto result = leaf::try_handle_some([&]() -> leaf::result<T> { return std::forward<F>(fn)(); },
//                                       [&](const lepton_error& e) -> leaf::result<T> {
//                                         ec = e.err_code;
//                                         return new_error(e);
//                                       });

//   if (!result) {
//     return tl::unexpected{ec ? ec : std::make_error_code(std::errc::invalid_argument)};
//   }

//   return *result;
// }

}  // namespace lepton

#endif  // _LEPTON_ERROR_H_