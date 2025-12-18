#ifndef _LEPTON_FILE_PIPELINE_H_
#define _LEPTON_FILE_PIPELINE_H_
#include <asio/any_io_executor.hpp>

#include "channel_endpoint.h"
#include "env_file_endpoint.h"
#include "utility_macros.h"
namespace lepton::storage::wal {

class file_pipeline {
  NOT_COPYABLE(file_pipeline)
 public:
 private:
  asio::any_io_executor executor_;
  std::stop_source stop_source_;
  signal_channel done_chan_;
  channel_endpoint<expected<fileutil::env_file_endpoint>> file_chan_;
};

}  // namespace lepton::storage::wal

#endif  // _LEPTON_FILE_PIPELINE_H_
