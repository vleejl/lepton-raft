#pragma once
#ifndef _LEPTON_TEST_FILE_H_
#define _LEPTON_TEST_FILE_H_
#include <fmt/format.h>
#include <rocksdb/env.h>
#include <rocksdb/status.h>

#include <cstdint>
#include <filesystem>
#include <string>

#include "asio/error_code.hpp"
#include "basic/defer.h"
#include "error/leaf.h"
#include "error/lepton_error.h"
#include "leaf.hpp"
#include "storage/fileutil/path.h"
#include "storage/fileutil/preallocate.h"
#include "storage/fileutil/read_dir.h"
#include "storage/wal/wal_file.h"
namespace fs = std::filesystem;

void delete_if_exists(const fs::path& file_path);

#endif  // _LEPTON_TEST_FILE_H_
