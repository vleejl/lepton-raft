#include "storage/wal/file_pipeline.h"

#include <filesystem>
#include <memory>

#include "basic/logger.h"
#include "coroutine/co_spawn_waiter.h"
#include "error/io_error.h"
#include "error/leaf_expected.h"
#include "error/lepton_error.h"
#include "storage/fileutil/env_file_endpoint.h"
#include "storage/wal/wal_file.h"
#include "tl/expected.hpp"

namespace lepton::storage::wal {

asio::awaitable<void> file_pipeline::stop() {
  if (stop_source_.stop_requested()) {
    // has already been stopped - no need to do anything
    LOG_INFO(logger_, "file_pipeline already been stopped, just return");
    co_return;
  }
  // Not already stopped, so trigger it
  asio::error_code ec;
  co_await stop_chan_.async_send(asio::error_code{}, asio::redirect_error(asio::use_awaitable, ec));
  if (ec) {
    // Node has already been stopped - no need to do anything
    co_return;
  }
  // Block until the stop has been acknowledged by run()
  co_await done_chan_.async_receive();
  done_chan_.close();
  if (started_.load(std::memory_order_relaxed)) {
    co_await wait_run_exit_chan_.async_receive();
  }
  LOG_INFO(logger_, "file_pipeline stopped successfully");
  co_return;
}

asio::awaitable<expected<fileutil::env_file_endpoint>> file_pipeline::open() {
  if (!is_running()) {
    co_return unexpected(coro_error::COROUTINE_EXIST);
  }
  // it will 'block' if the tmp file lock is already taken.
  auto wal_file_handle_result = co_await file_chan_.async_receive();
  if (!wal_file_handle_result) {
    co_return tl::unexpected{wal_file_handle_result.error()};
  }
  co_return std::move(*wal_file_handle_result);
}

leaf::result<fileutil::env_file_endpoint> file_pipeline::alloc() {
  // count % 2 so this file isn't the same as the one last published
  auto wal_file_path = wal_file_dir_ / fmt::format("{}.tmp", count_ % 2);
  BOOST_LEAF_AUTO(wal_file_handle, create_new_wal_file(executor_, env_, wal_file_path.string(), false));
  LEPTON_LEAF_CHECK(wal_file_handle.pre_allocate(file_size_, true));
  count_++;
  return wal_file_handle;
}

asio::awaitable<void> file_pipeline::listen_stop() {
  co_await stop_chan_.async_receive();
  stop_source_.request_stop();
  file_chan_.close();
  assert(done_chan_.is_open());
  co_await done_chan_.async_send(asio::error_code{});
  SPDLOG_INFO("file pipeline all chaneels has closed, and has send stop_chan response");
  co_return;
}

asio::awaitable<void> file_pipeline::run() {
  started_.store(true, std::memory_order_relaxed);
  auto waiter = coro::make_co_spawn_waiter<std::function<asio::awaitable<void>()>>(executor_);
  waiter->add([&]() -> asio::awaitable<void> { co_await listen_stop(); });

  while (is_running()) {
    auto wal_file_handle_result = leaf_to_expected([&]() -> leaf::result<fileutil::env_file_endpoint> {
      BOOST_LEAF_AUTO(wal_file_handle, alloc());
      return wal_file_handle;
    });
    if (!wal_file_handle_result.has_value()) {
      LOG_ERROR(logger_, "Failed to allocate wal file, error: {}", wal_file_handle_result.error().message());
      co_await file_chan_.async_send(tl::unexpected{wal_file_handle_result.error()});
      break;
    }
    // 通过 file_chan_ 发布 wal 文件句柄，如果没有订阅者则会阻塞在这里；这样不会影响其他协程的运行
    auto ec = co_await file_chan_.async_send(std::move(wal_file_handle_result));
    if (!ec.has_value()) {
      LOG_ERROR(logger_, "Failed to send wal file handle, error: {}", ec.error().message());
      break;
    }
  }
  co_await waiter->wait_all();
  co_await wait_run_exit_chan_.async_send(asio::error_code{});
  co_return;
}

leaf::result<file_pipeline_handle> new_file_pipeline(asio::any_io_executor executor, rocksdb::Env* env,
                                                     const std::string& wal_file_dir, std::size_t file_size,
                                                     std::shared_ptr<lepton::logger_interface> logger) {
  if (!std::filesystem::exists(wal_file_dir)) {
    return new_error(io_error::PARH_NOT_EXIT);
  }
  auto handle = std::make_unique<file_pipeline>(executor, env, wal_file_dir, file_size, std::move(logger));
  handle->start_run();
  return handle;
}

}  // namespace lepton::storage::wal
