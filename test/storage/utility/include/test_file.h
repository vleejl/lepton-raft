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

#endif  // _LEPTON_TEST_FILE_H_
