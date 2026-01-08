#pragma once
#ifndef _LEPTON_CORO_MACROS_H_
#define _LEPTON_CORO_MACROS_H_

namespace lepton::coro {
#define ASIO_ENSURE_STRAND(strand_member)                          \
  auto self_keep_alive = shared_from_this();                       \
  if (!(strand_member).running_in_this_thread()) {                 \
    co_await asio::dispatch((strand_member), asio::use_awaitable); \
  }
}  // namespace lepton::coro

#endif  // _LEPTON_CORO_MACROS_H_
