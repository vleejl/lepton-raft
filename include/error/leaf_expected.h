#include <system_error>
#include <tl/expected.hpp>
#include <type_traits>

#include "error/error.h"
#include "error/leaf.h"

#pragma once
#ifndef _LEPTON_LEAF_EXPECTED_H_
#define _LEPTON_LEAF_EXPECTED_H_

namespace lepton {

template <typename F>
auto leaf_to_expected(F&& f) -> tl::expected<typename decltype(std::declval<F>()())::value_type, std::error_code>
requires(!std::is_void_v<typename decltype(std::declval<F>()())::value_type>)
{
  using result_t = decltype(std::declval<F>()());
  using value_t = typename result_t::value_type;

  std::error_code ec;

  auto r = boost::leaf::try_handle_some([&]() -> boost::leaf::result<value_t> { return std::forward<F>(f)(); },
                                        [&](const lepton_error& e) -> boost::leaf::result<value_t> {
                                          ec = e.err_code;
                                          return boost::leaf::new_error(e);
                                        });

  if (!r) {
    assert(ec);  // 未映射的 LEAF 错误
    return tl::unexpected{ec};
  }

  return std::move(*r);
}

// void 特化版本
template <typename F>
auto leaf_to_expected(F&& f) -> tl::expected<void, std::error_code>
requires(std::is_void_v<typename decltype(std::declval<F>()())::value_type>)
{
  std::error_code ec;

  auto r = boost::leaf::try_handle_some([&]() -> boost::leaf::result<void> { return std::forward<F>(f)(); },
                                        [&](const lepton_error& e) -> boost::leaf::result<void> {
                                          ec = e.err_code;
                                          return boost::leaf::new_error(e);
                                        });

  if (!r) {
    assert(ec);
    return tl::unexpected{ec};
  }

  return {};
}

}  // namespace lepton

#endif  // _LEPTON_LEAF_EXPECTED_H_
