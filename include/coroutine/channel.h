#ifndef _LEPTON_CHANNEL_H_
#define _LEPTON_CHANNEL_H_
#include <spdlog/spdlog.h>

#include <asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <asio/experimental/channel.hpp>

#include "expected.h"
#include "raft_error.h"

namespace lepton::coro {

template <typename T>
using channel = asio::experimental::channel<void(asio::error_code, T)>;

using signal_channel = asio::experimental::channel<void(asio::error_code)>;
using signal_channel_handle = signal_channel *;

template <typename async_func>
asio::awaitable<expected<void>> async_select_done(async_func &&main_op, signal_channel &done_chan) {
  auto [order, ec1, ec2] =
      co_await asio::experimental::make_parallel_group(std::forward<async_func>(main_op), [&](auto token) {
        return done_chan.async_receive(token);
      }).async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);
  switch (order[0]) {
    case 0:
      if (ec1) {
        SPDLOG_ERROR("main_op failed with error: {}", ec1.message());
        co_return tl::unexpected(ec1);
      }
      co_return ok();
    case 1:
      co_return tl::unexpected{raft_error::STOPPED};
    default:
      co_return tl::unexpected{raft_error::UNKNOWN_ERROR};
  }
}

template <typename AsyncFunc, typename Awaitable = std::invoke_result_t<AsyncFunc, asio::use_awaitable_t<>>,
          typename T = typename Awaitable::value_type>
asio::awaitable<expected<T>> async_select_done_with_value(AsyncFunc &&main_op, signal_channel &done_chan) {
  static_assert(!std::is_void_v<T>,
                "async_select_done_with_value does not support void result. Use async_select_done instead.");
  auto [order, ec1, result1, ec2] =
      co_await asio::experimental::make_parallel_group(std::forward<AsyncFunc>(main_op), [&](auto token) {
        return done_chan.async_receive(token);
      }).async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);

  switch (order[0]) {
    case 0: {
      if (ec1) {
        SPDLOG_ERROR("main_op failed: {}", ec1.message());
        co_return tl::unexpected(ec1);
      }
      co_return tl::expected<T, std::error_code>(std::move(result1));
    }
    case 1:
      co_return tl::unexpected(make_error_code(raft_error::STOPPED));
    default:
      co_return tl::unexpected(make_error_code(raft_error::UNKNOWN_ERROR));
  }
}

template <typename... Ops>
asio::awaitable<expected<std::size_t>> async_select_any_expected(Ops &&...ops) {
  auto result_tuple = co_await asio::experimental::make_parallel_group(std::forward<Ops>(ops)...)
                          .async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);

  // result_tuple is a tuple: (order, ec1, ec2, ...)
  constexpr std::size_t N = sizeof...(Ops);
  const auto &order = std::get<0>(result_tuple);

  // Collect error codes from the tuple
  std::array<std::error_code, N> error_codes{};
  [&]<std::size_t... I>(std::index_sequence<I...>) {
    ((error_codes[I] = std::get<I + 1>(result_tuple)), ...);
  }(std::make_index_sequence<N>{});

  std::size_t completed = order[0];

  if (error_codes[completed]) {
    SPDLOG_ERROR("op {} failed: {}", completed, error_codes[completed].message());
    co_return tl::unexpected{error_codes[completed]};
  }
  co_return expected<std::size_t>{completed};
}

}  // namespace lepton::coro

#endif  // _LEPTON_CHANNEL_H_
