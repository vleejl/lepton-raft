#ifndef _LEPTON_CHANNEL_H_
#define _LEPTON_CHANNEL_H_
#include <spdlog/spdlog.h>

#include <asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <asio/experimental/channel.hpp>
// using namespace asio::experimental::awaitable_operators;
// using namespace std::literals::chrono_literals;
namespace lepton {

using executor_handle = asio::any_io_executor*;

template <typename T>
using channel = asio::experimental::channel<void(asio::error_code, T)>;

using signal_channel = asio::experimental::channel<void(asio::error_code)>;

template <typename T>
class read_only_channel {
 public:
  explicit read_only_channel(std::shared_ptr<T> chan) : channel_(std::move(chan)) {}

  auto async_receive() {
    asio::error_code ec;
    auto result = co_await channel_.async_receive(asio::redirect_error(asio::use_awaitable, ec));
    if (ec != asio::error::operation_aborted && ec) {
      SPDLOG_ERROR(ec.message());
    }
    co_return result;
  }

 private:
  std::shared_ptr<T> channel_;
};

}  // namespace lepton

#endif  // _LEPTON_CHANNEL_H_
