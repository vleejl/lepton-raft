#pragma once
#ifndef _LEPTON_PREALLOCATE_H_
#define _LEPTON_PREALLOCATE_H_
#include <cstdint>
#include <system_error>

#include "storage/fileutil/types.h"
namespace lepton::storage::fileutil {
// Preallocate tries to allocate the space for given file. This
// operation is only supported on darwin and linux by a few
// filesystems (APFS, btrfs, ext4, etc.).
// If the operation is unsupported, no error will be returned.
// Otherwise, the error encountered will be returned.
std::error_code preallocate(native_handle_t fd, std::int64_t size_in_bytes, bool extend_file);

std::error_code prealloc_extend_trunc(native_handle_t fd, off_t size_in_bytes);
std::error_code prealloc_extend(native_handle_t fd, off_t size_in_bytes);
std::error_code prealloc_fixed(native_handle_t fd, off_t size_in_bytes);
}  // namespace lepton::storage::fileutil

#endif  // _LEPTON_PREALLOCATE_H_
