#include <gtest/gtest.h>

#include <asio.hpp>
#include <asio/any_io_executor.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/io_context.hpp>
#include <asio/use_awaitable.hpp>

#include "async_mutex.h"
#include "basic/logger.h"
using asio::steady_timer;

class async_mutex_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

asio::awaitable<void> event_loop(asio::any_io_executor executor, std::string name, int called_times, int lock_times,
                                 async_mutex &mutex) {
  asio::steady_timer timer_(executor);
  auto loop = 0;
  while (loop < called_times) {
    loop++;
    // 每100ms执行一次 Raft tick 操作
    timer_.expires_after(std::chrono::milliseconds(100));  // 设置定时器
    co_await timer_.async_wait(asio::use_awaitable);       // 等待定时器到期

    LOG_INFO("[coroutine {}] current loop times: {}", name, loop);

    if (loop == lock_times) {
      co_await mutex.async_lock(asio::use_awaitable);
    }
  }
  co_return;
}

TEST_F(async_mutex_test_suit, lock_and_unlock) {
  asio::io_context io_context;
  async_mutex mutex(io_context.get_executor());

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        LOG_INFO("ready to start coroutine A event loop");
        co_await event_loop(io_context.get_executor(), "A", 10, 5, mutex);
        LOG_INFO("coroutine A event loop has exited");
        co_return;
      },
      asio::detached);

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        LOG_INFO("ready to start coroutine B event loop");
        co_await event_loop(io_context.get_executor(), "B", 10, 6, mutex);
        LOG_INFO("coroutine B event loop has exited");
        co_return;
      },
      asio::detached);

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        // 等待1秒后取消操作
        asio::steady_timer timer(io_context, std::chrono::seconds(5));
        co_await timer.async_wait(asio::use_awaitable);
        LOG_INFO("wait timeout and ready cancel channel....");
        mutex.unlock();
        LOG_INFO("cancel channel successful");
        co_return;
      },
      asio::detached);

  io_context.run();
}