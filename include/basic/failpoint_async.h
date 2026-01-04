#pragma once
#ifndef _LEPTON_FAILPOINT_ASYNC_H_
#define _LEPTON_FAILPOINT_ASYNC_H_
#include <asio/awaitable.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <chrono>
#include <tl/expected.hpp>

#include "failpoint_core.h"

namespace lepton::failpoint::async {

inline asio::awaitable<void> sleep_for(asio::any_io_executor ex, std::chrono::milliseconds dur) {
  asio::steady_timer timer(ex);
  timer.expires_after(dur);
  co_await timer.async_wait(asio::use_awaitable);
}

inline asio::awaitable<void> point(asio::any_io_executor ex, const char* name) {
  auto fp = decide(name);

  if (fp.act == action::panic) {
    panic();
  }

  if (fp.act == action::sleep && fp.value > 0) {
    co_await sleep_for(ex, std::chrono::milliseconds(fp.value));
  }

  co_return;
}

template <typename E>
inline asio::awaitable<tl::expected<void, E>> point_expected(asio::any_io_executor ex, const char* name) {
  auto fp = decide(name);

  if (fp.act == action::panic) panic();

  if (fp.act == action::errno_inject) {
    co_return tl::unexpected(E(fp.value));
  }

  if (fp.act == action::sleep && fp.value > 0) {
    co_await sleep_for(ex, std::chrono::milliseconds(fp.value));
  }

  co_return {};
}

#define FAILPOINT_ASYNC(name, ex) co_await ::lepton::failpoint::async::point((ex), #name)

#define FAILPOINT_ASYNC_EXPECTED(name, ex, err_type) \
  co_await ::lepton::failpoint::async::point_expected<err_type>((ex), #name)

}  // namespace lepton::failpoint::async

#endif  // _LEPTON_FAILPOINT_ASYNC_H_
