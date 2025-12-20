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
#include "coroutine/channel.h"
#include "raft_core/node.h"
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

enum class channel_message_type : std::uint8_t { TICK };

struct channel_message {
  channel_message_type message_type;
};

using message_channel = channel<void(asio::error_code, channel_message)>;

// 原子计数器
static int counter(0);

awaitable<void> product_tick(steady_timer::duration interval, message_channel& chann,
                             lepton::coro::signal_channel& close_chan) {
  steady_timer timer(co_await this_coro::executor);
  while (close_chan.is_open()) {
    asio::error_code ec;
    co_await chann.async_send(asio::error_code{}, channel_message{channel_message_type::TICK},
                              asio::redirect_error(asio::use_awaitable, ec));

    if (ec == asio::error::operation_aborted) {
      // 正常关闭
      break;
    } else if (ec) {
      std::cout << "[product_tick] Send error: " << ec.message() << "\n";
      break;
    }

    timer.expires_after(interval);
    co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));

    if (ec) {
      // 如果定时器被取消（如关闭信号），正常退出
      if (ec != asio::error::operation_aborted) {
        std::cout << "[product_tick] Timer error: " << ec.message() << "\n";
      }
      break;
    }
  }
  co_return;
}

awaitable<void> close_message(message_channel& chann, lepton::coro::signal_channel& close_chan) {
  // 等待关闭信号
  co_await close_chan.async_receive(use_awaitable);
  std::cout << "Closing message channel, counter: " << counter << std::endl;

  // 关闭主消息通道
  chann.close();
  close_chan.close();
  std::cout << "Message channel closed" << std::endl;
  co_return;
}

awaitable<void> consume_message(message_channel& chann) {
  while (true) {
    asio::error_code ec;
    auto message = co_await chann.async_receive(asio::redirect_error(asio::use_awaitable, ec));

    if (ec == asio::error::operation_aborted) {
      // 通道被关闭，正常退出
      break;
    } else if (ec) {
      std::cout << "[consume_message] Receive error: " << ec.message() << "\n";
      break;
    }

    // 原子增加计数器
    int current = counter++;

    switch (message.message_type) {
      case channel_message_type::TICK:
        std::cout << "Received tick (" << current << ")" << std::endl;
        break;
    }
  }
  co_return;
}

awaitable<void> check_counter(lepton::coro::signal_channel& close_chan) {
  while (true) {
    // 每秒检查一次计数器
    steady_timer timer(co_await this_coro::executor);
    timer.expires_after(1s);
    co_await timer.async_wait();

    std::cout << "Current counter value: " << counter << std::endl;

    // 如果计数器达到10，发送关闭信号
    if (counter >= 10) {
      // 发送关闭信号
      co_await close_chan.async_send(asio::error_code{});
      std::cout << "Sent close signal at counter: " << counter << std::endl;
      break;
    }
  }
  co_return;
}

awaitable<void> run(io_context& ctx) {
  message_channel chann{ctx.get_executor()};
  lepton::coro::signal_channel stop_chan{ctx.get_executor()};
  // co_spawn(ctx, product_tick(interval, chann, stop_chan), detached);
  co_spawn(ctx, check_counter(stop_chan), detached);
  // co_await consume_message(chann, stop_chan);
  co_await (product_tick(1s, chann, stop_chan) && consume_message(chann) && close_message(chann, stop_chan));
}

int main(int argc, char* argv[]) {
  LEPTON_UNUSED(argc);
  LEPTON_UNUSED(argv);
  try {
    std::vector<int> arr;
    arr.push_back(1);
    io_context ctx;
    co_spawn(ctx, run(ctx), detached);
    ctx.run();
  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }
}