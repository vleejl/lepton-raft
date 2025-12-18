#ifndef _LEPTON_SIGNAL_CHANNEL_ENDPOINT_H_
#define _LEPTON_SIGNAL_CHANNEL_ENDPOINT_H_
#include <spdlog/spdlog.h>

#include <asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <asio/experimental/channel.hpp>
#include <cassert>
#include <cstddef>
#include <stop_token>

#include "asio/error_code.hpp"
#include "channel.h"
#include "expected.h"

namespace lepton::coro {

using signal_channel = asio::experimental::channel<void(asio::error_code)>;

class signal_channel_endpoint {
 public:
  explicit signal_channel_endpoint(asio::any_io_executor executor) : chan_(executor, 0), cancel_chan_(executor) {}
  signal_channel_endpoint(asio::any_io_executor executor, std::size_t max_buffer_size)
      : chan_(executor, max_buffer_size), cancel_chan_(executor) {}

  bool try_send() {
    if (stop_source_.stop_requested()) {
      return false;
    }
    return chan_.try_send(asio::error_code{});
  }

  // 发送信号
  asio::awaitable<lepton::expected<void>> async_send() {
    if (stop_source_.stop_requested()) {
      co_return tl::unexpected(raft_error::STOPPED);
    }
    auto result = co_await async_select_done([&](auto token) { return chan_.async_send(asio::error_code{}, token); },
                                             cancel_chan_);
    co_return result;
  }

  // 接收信号
  asio::awaitable<lepton::expected<void>> async_receive() {
    if (stop_source_.stop_requested()) {
      co_return tl::unexpected(raft_error::STOPPED);
    }
    auto result = co_await async_select_done([&](auto token) { return chan_.async_receive(token); }, cancel_chan_);
    co_return result;
  }

  void close() {
    stop_source_.request_stop();
    cancel_chan_.close();
    chan_.close();
  }

  auto is_open() const { return stop_source_.stop_requested(); }

  auto& raw_channel() { return chan_; }

 private:
  signal_channel chan_;
  signal_channel cancel_chan_;
  std::stop_source stop_source_;
};

using signal_channel_endpoint_handle = signal_channel_endpoint*;
}  // namespace lepton::coro

#endif  // _LEPTON_SIGNAL_CHANNEL_ENDPOINT_H_
