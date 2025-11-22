#ifndef _LEPTON_TEST_FILE_H_
#define _LEPTON_TEST_FILE_H_
#include <fmt/format.h>
#include <rocksdb/env.h>
#include <rocksdb/status.h>

#include <cstdint>
#include <filesystem>
#include <string>

#include "asio/error_code.hpp"
#include "defer.h"
#include "leaf.h"
#include "leaf.hpp"
#include "lepton_error.h"
#include "path.h"
#include "preallocate.h"
#include "read_dir.h"
#include "wal_file.h"
namespace fs = std::filesystem;

void delete_if_exists(const fs::path& file_path);

namespace lepton {

// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
class wal {
 private:
  // the living directory of the underlay files
  std::string dir_;

  std::string metadata_;
  wal_file file_handle_;
};

}  // namespace lepton

#endif  // _LEPTON_TEST_FILE_H_
