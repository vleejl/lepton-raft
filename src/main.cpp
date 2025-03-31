//
// throttling_proxy.cpp
// ~~~~~~~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2024 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include <raft.pb.h>

#include <asio.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <asio/experimental/channel.hpp>
#include <cstdint>
#include <format>
#include <iostream>
#include <magic_enum/magic_enum.hpp>
#include <vector>

#include "asio/awaitable.hpp"
#include "asio/co_spawn.hpp"
#include "node.h"
using asio::as_tuple;
using asio::awaitable;
using asio::buffer;
using asio::co_spawn;
using asio::detached;
using asio::io_context;
using asio::steady_timer;
using asio::use_awaitable;
using asio::experimental::channel;
using asio::ip::tcp;
namespace this_coro = asio::this_coro;
using namespace asio::experimental::awaitable_operators;
using namespace std::literals::chrono_literals;

// using token_channel = channel<void(asio::error_code, std::size_t)>;

// awaitable<void> produce_tokens(std::size_t bytes_per_token,
//                                steady_timer::duration token_interval,
//                                token_channel &tokens) {
//   steady_timer timer(co_await this_coro::executor);
//   for (;;) {
//     co_await tokens.async_send(asio::error_code{}, bytes_per_token,
//                                use_awaitable);

//     timer.expires_after(token_interval);
//     co_await timer.async_wait(use_awaitable);
//   }
// }

// awaitable<void> transfer(tcp::socket &from, tcp::socket &to,
//                          token_channel &tokens) {
//   std::array<unsigned char, 4096> data;
//   for (;;) {
//     std::size_t bytes_available = co_await
//     tokens.async_receive(use_awaitable); while (bytes_available > 0) {
//       std::size_t n = co_await from.async_read_some(
//           buffer(data, bytes_available), use_awaitable);

//       co_await async_write(to, buffer(data, n), use_awaitable);

//       bytes_available -= n;
//     }
//   }
// }

// awaitable<void> proxy(tcp::socket client, tcp::endpoint target) {
//   constexpr std::size_t number_of_tokens = 100;
//   constexpr size_t bytes_per_token = 20 * 1024;
//   constexpr steady_timer::duration token_interval = 100ms;

//   auto ex = client.get_executor();
//   tcp::socket server(ex);
//   token_channel client_tokens(ex, number_of_tokens);
//   token_channel server_tokens(ex, number_of_tokens);

//   co_await server.async_connect(target, use_awaitable);
//   co_await (produce_tokens(bytes_per_token, token_interval, client_tokens) &&
//             transfer(client, server, client_tokens) &&
//             produce_tokens(bytes_per_token, token_interval, server_tokens) &&
//             transfer(server, client, server_tokens));
// }

// awaitable<void> listen(tcp::acceptor &acceptor, tcp::endpoint target) {
//   for (;;) {
//     auto [e, client] = co_await
//     acceptor.async_accept(as_tuple(use_awaitable)); if (!e) {
//       auto ex = client.get_executor();
//       co_spawn(ex, proxy(std::move(client), target), detached);
//     } else {
//       std::cerr << "Accept failed: " << e.message() << "\n";
//       steady_timer timer(co_await this_coro::executor);
//       timer.expires_after(100ms);
//       co_await timer.async_wait(use_awaitable);
//     }
//   }
// }

enum class channel_message_type : std::uint8_t { TICK };

struct channel_message {
  channel_message_type message_type;
};

using message_channel = channel<void(asio::error_code, channel_message)>;

awaitable<void> product_tick(steady_timer::duration interval, message_channel &chann) {
  steady_timer timer(co_await this_coro::executor);
  for (;;) {
    co_await chann.async_send(asio::error_code{}, channel_message{channel_message_type::TICK}, use_awaitable);

    timer.expires_after(interval);
    co_await timer.async_wait(use_awaitable);
  }
}

awaitable<void> consume_message(message_channel &chann) {
  for (;;) {
    auto message = co_await chann.async_receive();
    auto message_type = message.message_type;
    switch (message_type) {
      case channel_message_type::TICK:
        std::cout << "tick is running ... " << std::endl;
        break;
      default:
        std::cerr << "can not recognize message type " << static_cast<std::uint8_t>(message_type) << std::endl;
    }
  }
}

awaitable<void> run(io_context &ctx) {
  constexpr steady_timer::duration interval = 1s;
  message_channel chann{ctx};
  co_spawn(ctx, product_tick(interval, chann), detached);
  co_await consume_message(chann);
}
int main(int argc, char *argv[]) {
  try {
    std::vector<int> arr;
    arr.push_back(1);
    io_context ctx;
    co_spawn(ctx, run(ctx), detached);
    ctx.run();
  } catch (std::exception &e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }
}