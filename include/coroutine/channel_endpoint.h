#ifndef _LEPTON_CHANNEL_ENDPOINT_H_
#define _LEPTON_CHANNEL_ENDPOINT_H_
#include <spdlog/spdlog.h>

#include <asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <asio/experimental/channel.hpp>
#include <cstddef>

#include "asio/error_code.hpp"
#include "channel.h"
#include "expected.h"

namespace lepton::coro {
template <typename T>
class channel_endpoint {
 public:
  explicit channel_endpoint(asio::any_io_executor executor) : chan_(executor, 0), cancel_chan_(executor) {}

  channel_endpoint(asio::any_io_executor executor, std::size_t size) : chan_(executor, size), cancel_chan_(executor) {}

  asio::awaitable<lepton::expected<T>> async_receive() {
    auto result =
        co_await async_select_done_with_value([&](auto token) { return chan_.async_receive(token); }, cancel_chan_);
    co_return result;
  }

  asio::awaitable<lepton::expected<void>> async_send(T value) {
    auto v = std::move(value);
    auto result = co_await async_select_done(
        [&](auto token) { return chan_.async_send(asio::error_code{}, std::move(v), token); }, cancel_chan_);
    co_return result;
  }

  bool try_send(T value) { return chan_.try_send(asio::error_code{}, std::move(value)); }

  auto& raw_channel() { return chan_; }

  auto is_open() const { return chan_.is_open(); }

  void close() {
    cancel_chan_.close();
    chan_.close();
  }

 private:
  asio::experimental::channel<void(asio::error_code, T)> chan_;
  signal_channel cancel_chan_;
};

}  // namespace lepton::coro

#endif  // _LEPTON_CHANNEL_ENDPOINT_H_
