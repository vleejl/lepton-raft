#pragma once
#ifndef _LEPTON_CO_ENV_ADAPTER_H_
#define _LEPTON_CO_ENV_ADAPTER_H_
#include <asio/awaitable.hpp>
#include <asio/steady_timer.hpp>
#include <asio/thread_pool.hpp>
#include <asio/use_awaitable.hpp>

namespace lepton::storage::fs {

/**
 * @brief
 * Bridge a synchronous, blocking operation into an Asio coroutine.
 *
 * This function executes `fn` on the given blocking thread pool, suspends
 * the current coroutine, and resumes it once `fn` completes.
 *
 * ----------------------------------------------------------------------------
 * CONTRACT (MUST HOLD):
 *
 * 1. `fn` MUST be a synchronous, blocking function.
 * 2. `fn` MUST NOT throw exceptions.
 * 3. `fn` MUST return an expected-like type (e.g. std::expected<T, E>).
 * 4. `fn` MUST complete exactly once and MUST NOT access coroutine-local
 *    Asio objects (channels, sockets, timers, executors, etc).
 *
 * VIOLATING ANY OF THE ABOVE RESULTS IN UNDEFINED BEHAVIOR.
 *
 * ----------------------------------------------------------------------------
 * THREADING MODEL:
 *
 * - `fn` is executed on a worker thread from `pool`.
 * - The awaiting coroutine is resumed on the same executor it was suspended on.
 *
 * ----------------------------------------------------------------------------
 * LIFETIME GUARANTEES:
 *
 * - All local variables remain alive for the entire suspension period.
 * - Shared state is only accessed-before-resume / after-resume.
 *
 * ----------------------------------------------------------------------------
 * ERROR HANDLING:
 *
 * - This function does NOT handle errors.
 * - Errors MUST be represented in the return value of `fn`.
 *
 * ----------------------------------------------------------------------------
 */
template <typename Fn>
asio::awaitable<std::invoke_result_t<Fn>> co_env_blocking(asio::thread_pool& pool, Fn&& fn) {
  using R = std::invoke_result_t<Fn>;

  // ---- Compile-time contract enforcement ---------------------------------

  static_assert(std::is_invocable_v<Fn>, "co_env_blocking: Fn must be invocable with no arguments");

  static_assert(std::is_move_constructible_v<R>, "co_env_blocking: Fn return type must be move-constructible");

  static_assert(!std::is_reference_v<R>, "co_env_blocking: Fn must not return a reference type");

  // NOTE:
  // We intentionally do NOT attempt to detect exception-throwing behavior
  // here. The 'no-throw' requirement is a hard semantic contract.
  // ------------------------------------------------------------------------

  // Capture the executor of the awaiting coroutine.
  auto executor = co_await asio::this_coro::executor;

  // Used purely as a completion signal, not for timing.
  asio::steady_timer completion_timer(executor);

  // Storage for the result produced by `fn`.
  std::optional<R> result;

  // Dispatch the blocking operation onto the thread pool.
  asio::post(pool, [&, fn = std::forward<Fn>(fn)]() mutable {
    // CONTRACT:
    // - fn MUST NOT throw
    // - fn MUST complete exactly once
    result.emplace(fn());

    // Notify the awaiting coroutine.
    completion_timer.cancel();
  });

  // Suspend the coroutine until the blocking task completes.
  co_await completion_timer.async_wait(asio::use_awaitable);

  // At this point, `result` is guaranteed to be initialized.
  co_return std::move(*result);
}

}  // namespace lepton::storage::fs

#endif  // _LEPTON_CO_ENV_ADAPTER_H_
