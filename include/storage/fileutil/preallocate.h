#pragma once
#ifndef _LEPTON_PREALLOCATE_H_
#define _LEPTON_PREALLOCATE_H_

#include <cstdint>
#include <system_error>
namespace lepton::storage::fileutil {
// Preallocate tries to allocate the space for given file. This
// operation is only supported on darwin and linux by a few
// filesystems (APFS, btrfs, ext4, etc.).
// If the operation is unsupported, no error will be returned.
// Otherwise, the error encountered will be returned.
std::error_code preallocate(int fd, std::int64_t size_in_bytes, bool extend_file);

std::error_code prealloc_extend_trunc(int fd, off_t size_in_bytes);
std::error_code prealloc_extend(int fd, off_t size_in_bytes);
std::error_code prealloc_fixed(int fd, off_t size_in_bytes);
}  // namespace lepton::storage::fileutil

#endif  // _LEPTON_PREALLOCATE_H_
