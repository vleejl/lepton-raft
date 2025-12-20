#pragma once
#ifndef _LEPTON_EXPECTED_H_
#define _LEPTON_EXPECTED_H_
#include <system_error>
#include <tl/expected.hpp>

namespace lepton {
template <typename T>
using expected = tl::expected<T, std::error_code>;

inline expected<void> ok() { return expected<void>{}; }

}  // namespace lepton

#define CO_CHECK_EXPECTED(ec)                                \
  do {                                                       \
    if ((ec) == asio::error::operation_aborted) {            \
      co_return tl::unexpected(lepton::coro_error::STOPPED); \
    }                                                        \
    if (ec) {                                                \
      SPDLOG_ERROR("{}", (ec).message());                    \
      co_return tl::unexpected(ec);                          \
    }                                                        \
  } while (0)

#endif  // _LEPTON_EXPECTED_H_
