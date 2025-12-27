#pragma once
#ifndef _LEPTON_PATH_H_
#define _LEPTON_PATH_H_
#include "error/leaf.h"
namespace lepton::storage::fileutil {
leaf::result<void> remove_all(const std::string& path);

leaf::result<void> rename(const std::string& old_path, const std::string& new_path);
}  // namespace lepton::storage::fileutil

#endif  // _LEPTON_PATH_H_
