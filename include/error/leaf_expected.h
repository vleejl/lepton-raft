#include <system_error>
#include <tl/expected.hpp>
#include <type_traits>

#include "error/leaf.h"
#include "error/lepton_error.h"

#pragma once
#ifndef _LEPTON_LEAF_EXPECTED_H_
#define _LEPTON_LEAF_EXPECTED_H_

namespace lepton {

template <typename F, typename T = std::decay_t<decltype(*std::declval<F>()())>>
tl::expected<T, std::error_code> leaf_to_expected(F&& f) {
  std::error_code ec;

  auto r = boost::leaf::try_handle_some([&]() -> boost::leaf::result<T> { return std::forward<F>(f)(); },
                                        [&](const lepton_error& e) -> boost::leaf::result<T> {
                                          ec = e.err_code;
                                          return new_error(e);
                                        });

  if (!r) {
    assert(ec);  // 若 ec 是空的，说明漏处理了错误类型
    return tl::unexpected{ec};
  }

  return std::move(*r);
}

// void 特化版本
template <typename F>
tl::expected<void, std::error_code> leaf_to_expected_void(F&& f) {
  std::error_code ec;

  auto r = boost::leaf::try_handle_some([&]() -> boost::leaf::result<void> { return std::forward<F>(f)(); },
                                        [&](const lepton_error& e) -> boost::leaf::result<void> {
                                          ec = e.err_code;
                                          return new_error(e);
                                        });

  if (!r) {
    assert(ec);
    return tl::unexpected{ec};
  }

  return {};
}

}  // namespace lepton

#endif  // _LEPTON_LEAF_EXPECTED_H_
