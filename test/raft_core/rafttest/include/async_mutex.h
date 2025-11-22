#pragma once
#include <asio.hpp>
#include <asio/experimental/channel.hpp>

#include "channel.h"
#include "utility_macros.h"

class async_mutex {
  NOT_COPYABLE_NOT_MOVABLE(async_mutex)
 public:
  explicit async_mutex(asio::any_io_executor ex)
      : ch_(ex, 1)  // capacity = 1
  {
    // 预放入一个“令牌”表示“未加锁”
    asio::error_code ec;
    (void)ch_.try_send(ec);
  }

  // 释放锁：归还令牌
  void unlock() {
    asio::error_code ec;
    bool ok = ch_.try_send(ec);
    // 若 ok == false，说明重复 unlock 或并发错误；开发期可加 assert
    // assert(ok && !ec);
    (void)ok;
    (void)ec;
  }

  // 协程获取锁：co_await async_lock(use_awaitable)
  template <typename CompletionToken>
  auto async_lock(CompletionToken&& token) {
    // 取走令牌 -> 获得锁；若无令牌则挂起等待
    return ch_.async_receive(std::forward<CompletionToken>(token));
  }

  // （可选）尝试获取锁：成功返回 true，失败返回 false（不挂起）
  bool try_lock() {
    asio::error_code g_ec;
    // 取令牌（无令牌则返回 false）
    bool ok = ch_.try_receive([&](asio::error_code ec) { g_ec = ec; });
    return ok && !g_ec;
  }

 private:
  lepton::signal_channel ch_;
};
