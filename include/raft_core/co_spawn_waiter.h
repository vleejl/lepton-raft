#ifndef _LEPTON_CO_SPAWN_WAITER_H_
#define _LEPTON_CO_SPAWN_WAITER_H_
#include <memory>

#include "channel.h"

namespace lepton {
template <typename AwaitableFunc>
class co_spawn_waiter : public std::enable_shared_from_this<co_spawn_waiter<AwaitableFunc>> {
 public:
  co_spawn_waiter(asio::any_io_executor executor)
      : executor_(executor),
        exit_chan_(executor, /*capacity*/ 64)  // 支持多个 send
  {}

  void add(AwaitableFunc func) {
    auto self = this->shared_from_this();
    asio::co_spawn(
        executor_,
        [self, func = std::move(func)]() mutable -> asio::awaitable<void> {
          co_await func();
          co_await self->exit_chan_.async_send(asio::error_code{});
          co_return;
        },
        asio::detached);
    ++count_;
  }

  /// 等待所有子任务完成
  asio::awaitable<void> wait_all() {
    for (size_t i = 0; i < count_; ++i) {
      co_await exit_chan_.async_receive(asio::use_awaitable);
    }
    co_return;
  }

 private:
  asio::any_io_executor executor_;
  signal_channel exit_chan_;
  std::size_t count_{0};
};

// 工厂函数
template <typename AwaitableFunc>
std::shared_ptr<co_spawn_waiter<AwaitableFunc>> make_co_spawn_waiter(asio::any_io_executor exec) {
  return std::make_shared<co_spawn_waiter<AwaitableFunc>>(exec);
}
}  // namespace lepton

#endif  // _LEPTON_CO_SPAWN_WAITER_H_
