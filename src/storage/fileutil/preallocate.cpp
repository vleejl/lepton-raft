#include "storage/fileutil/preallocate.h"

namespace lepton::storage::fileutil {

std::error_code preallocate(native_handle_t fd, std::int64_t size_in_bytes, bool extend_file) {
  if (size_in_bytes == 0) {
    // fallocate will return EINVAL if length is 0; skip
    return {};
  }
  if (extend_file) {
    return prealloc_extend(fd, size_in_bytes);
  }
  return prealloc_fixed(fd, size_in_bytes);
}

}  // namespace lepton::storage::fileutil
