#pragma once
#ifndef _LEPTON_FILE_PIPELINE_H_
#define _LEPTON_FILE_PIPELINE_H_
#include <asio/any_io_executor.hpp>
#include <cstddef>
#include <filesystem>

#include "basic/logger.h"
#include "basic/utility_macros.h"
#include "coroutine/channel_endpoint.h"
#include "error/leaf.h"
#include "storage/fileutil/env_file_endpoint.h"
namespace lepton::storage::wal {

class file_pipeline {
  NOT_COPYABLE(file_pipeline)
 public:
  file_pipeline(asio::any_io_executor executor, rocksdb::Env* env, const std::string& wal_file_dir,
                std::size_t file_size, std::shared_ptr<lepton::logger_interface>&& logger)
      : executor_(executor),
        env_(env),
        done_chan_(executor),
        stop_chan_(executor),
        started_(false),
        wait_run_exit_chan_(executor),
        file_chan_(executor),
        wal_file_dir_(wal_file_dir),
        file_size_(file_size),
        count_(0),
        logger_(std::move(logger)) {}

  void start_run() { co_spawn(executor_, run(), asio::detached); }

  asio::awaitable<void> stop();

  // Open returns a fresh file for writing. Rename the file before calling
  // Open again or there will be file collisions.
  // it will 'block' if the tmp file lock is already taken.
  asio::awaitable<expected<fileutil::env_file_endpoint>> open();

 private:
  leaf::result<fileutil::env_file_endpoint> alloc();

  asio::awaitable<void> run();

  asio::awaitable<void> listen_stop();

  bool is_running() const { return !stop_source_.stop_requested(); }

 private:
  asio::any_io_executor executor_;
  rocksdb::Env* env_;

  coro::signal_channel done_chan_;
  coro::signal_channel stop_chan_;
  std::atomic<bool> started_{false};
  std::stop_source stop_source_;
  coro::signal_channel wait_run_exit_chan_;

  coro::channel_endpoint<expected<fileutil::env_file_endpoint>> file_chan_;
  // dir to put files
  std::filesystem::path wal_file_dir_;
  // size of files to make, in bytes
  const std::size_t file_size_;
  // count number of files generated
  std::size_t count_{0};
  std::shared_ptr<lepton::logger_interface> logger_;
};

using file_pipeline_handle = std::unique_ptr<file_pipeline>;

leaf::result<file_pipeline_handle> new_file_pipeline(asio::any_io_executor executor, rocksdb::Env* env,
                                                     const std::string& wal_file_dir, std::size_t file_size,
                                                     std::shared_ptr<lepton::logger_interface>&& logger);

}  // namespace lepton::storage::wal

#endif  // _LEPTON_FILE_PIPELINE_H_
