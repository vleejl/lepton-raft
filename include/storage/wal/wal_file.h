#pragma once
#ifndef _LEPTON_WAL_FILE_H_
#define _LEPTON_WAL_FILE_H_
#include "storage/fileutil/file_endpoint.h"
#include "storage/fileutil/locked_file_endpoint.h"
#include "storage/wal/encoder.h"

namespace lepton::storage::wal {

inline std::string wal_file_name(std::uint64_t seq, std::uint64_t index) {
  return fmt::format("{:016x}-{:016x}.wal", seq, index);
}

// createNewWALFile creates a WAL file.
// To create a locked file, use *fileutil.LockedFile type parameter.
// To create a standard file, use *os.File type parameter.
// If forceNew is true, the file will be truncated if it already exists.
leaf::result<fileutil::file_endpoint> create_new_wal_file(asio::any_io_executor executor, const std::string& filename,
                                                          bool force_new);

leaf::result<fileutil::locked_file_handle> create_new_wal_file(asio::any_io_executor executor, rocksdb::Env* env,
                                                               const std::string& filename, bool force_new);

leaf::result<std::unique_ptr<encoder>> new_file_encoder(asio::any_io_executor executor,
                                                        fileutil::locked_file_endpoint& file, std::uint32_t prev_crc,
                                                        std::shared_ptr<lepton::logger_interface> logger);
}  // namespace lepton::storage::wal

#endif  // _LEPTON_WAL_FILE_H_
