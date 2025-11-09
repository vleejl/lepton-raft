#ifndef _LEPTON_TEST_FILE_H_
#define _LEPTON_TEST_FILE_H_
#include <filesystem>

namespace fs = std::filesystem;

void delete_if_exists(const fs::path& file_path);

#endif  // _LEPTON_TEST_FILE_H_
