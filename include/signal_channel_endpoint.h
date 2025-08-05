#ifndef _LEPTON_SIGNAL_CHANNEL_ENDPOINT_H_
#define _LEPTON_SIGNAL_CHANNEL_ENDPOINT_H_
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

namespace lepton {

using signal_channel = asio::experimental::channel<void(asio::error_code)>;

class signal_channel_endpoint {
 public:
  explicit signal_channel_endpoint(asio::any_io_executor executor) : chan_(executor, 0), cancel_chan_(executor) {}
  signal_channel_endpoint(asio::any_io_executor executor, std::size_t max_buffer_size)
      : chan_(executor, max_buffer_size), cancel_chan_(executor) {}

  // 发送信号
  asio::awaitable<lepton::expected<void>> async_send() {
    auto result = co_await async_select_done([&](auto token) { return chan_.async_send(asio::error_code{}, token); },
                                             cancel_chan_);
    co_return result;
  }

  // 接收信号
  asio::awaitable<lepton::expected<void>> async_receive() {
    auto result = co_await async_select_done([&](auto token) { return chan_.async_receive(token); }, cancel_chan_);
    co_return result;
  }

  void close() {
    cancel_chan_.close();
    chan_.close();
  }

  auto is_open() const { return chan_.is_open(); }

  auto& raw_channel() { return chan_; }

 private:
  signal_channel chan_;
  signal_channel cancel_chan_;
};

using signal_channel_endpoint_handle = signal_channel_endpoint*;
}  // namespace lepton

#endif  // _LEPTON_SIGNAL_CHANNEL_ENDPOINT_H_
