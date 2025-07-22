#ifndef _LEPTON_CHANNEL_H_
#define _LEPTON_CHANNEL_H_
#include <asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <asio/experimental/channel.hpp>

// using namespace asio::experimental::awaitable_operators;
// using namespace std::literals::chrono_literals;
namespace lepton {
// using asio::as_tuple;
// using asio::awaitable;
// using asio::buffer;
// using asio::co_spawn;
// using asio::detached;
// using asio::io_context;
// using asio::steady_timer;
// using asio::use_awaitable;
// using asio::experimental::channel;
// using asio::ip::tcp;

template <typename T>
using channel = asio::experimental::channel<void(std::error_code, T)>;

using signal_channel = asio::experimental::channel<void(std::error_code)>;
}  // namespace lepton

#endif  // _LEPTON_CHANNEL_H_
