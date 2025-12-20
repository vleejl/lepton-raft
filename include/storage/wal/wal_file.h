#pragma once
#ifndef _LEPTON_WAL_FILE_H_
#define _LEPTON_WAL_FILE_H_
#include "storage/fileutil/env_file_endpoint.h"
#include "storage/wal/encoder.h"

namespace lepton::storage::wal {

// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.

inline std::string wal_file_name(std::uint64_t seq, std::uint64_t index) {
  return fmt::format("{:016x}-{:016x}.wal", seq, index);
}

// createNewWALFile creates a WAL file.
// To create a locked file, use *fileutil.LockedFile type parameter.
// To create a standard file, use *os.File type parameter.
// If forceNew is true, the file will be truncated if it already exists.
leaf::result<fileutil::env_file_endpoint> create_new_wal_file(asio::any_io_executor executor, rocksdb::Env* env,
                                                              const std::string& filename, bool force_new);

leaf::result<encoder> new_file_encoder(fileutil::env_file_endpoint& file, std::uint32_t prev_crc,
                                       std::shared_ptr<lepton::logger_interface>&& logger);
}  // namespace lepton::storage::wal

#endif  // _LEPTON_WAL_FILE_H_
