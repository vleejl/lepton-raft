#include "storage/fileutil/preallocate.h"

#if defined(__linux__)
#include <fcntl.h>
#include <linux/falloc.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <system_error>

namespace lepton::storage::fileutil {
std::error_code prealloc_extend_trunc(native_handle_t fd, off_t size_in_bytes) {
  // 1. save current offset
  off_t cur_off = ::lseek(fd, 0, SEEK_CUR);
  if (cur_off == static_cast<off_t>(-1)) {
    return std::error_code(errno, std::generic_category());
  }

  // 2. seek to end + size_in_bytes (may extend file)
  off_t new_size = ::lseek(fd, size_in_bytes, SEEK_END);
  if (new_size == static_cast<off_t>(-1)) {
    return std::error_code(errno, std::generic_category());
  }

  // 3. restore offset
  if (::lseek(fd, cur_off, SEEK_SET) == static_cast<off_t>(-1)) {
    return std::error_code(errno, std::generic_category());
  }

  // 4. truncate only if file is larger than expected
  if (size_in_bytes > new_size) {
    return {};
  }

  if (::ftruncate(fd, size_in_bytes) == 0) {
    return {};
  }

  return std::error_code(errno, std::generic_category());
}

// 尽最大努力分配磁盘空间并扩展文件大小；如果做不到，至少保证文件大小正确
std::error_code prealloc_extend(native_handle_t fd, off_t size_in_bytes) {
  // use mode = 0 to change size
  // 分配磁盘空间
  // 同时扩展文件逻辑大小 (st_size)

  // 若文件当前大小 < size_in_bytes
  // - 文件大小直接增长
  // - 中间区域读为零（逻辑零，不一定写磁盘）
  if (::fallocate(fd, 0, 0, size_in_bytes) == 0) {
    return {};
  }

  int err = errno;
  if (err == ENOTSUP || err == EINTR) {
    // not supported; fallback
    // fallocate EINTRs frequently in some environments; fallback
    return prealloc_extend_trunc(fd, size_in_bytes);
  }

  return std::error_code(err, std::generic_category());
}

// 分配磁盘块
// 不改变文件逻辑大小
// 如果文件系统支持，就提前分配磁盘；不支持就算了
std::error_code prealloc_fixed(native_handle_t fd, off_t size_in_bytes) {
  // use mode = 1 to keep size; see FALLOC_FL_KEEP_SIZE
  if (::fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, size_in_bytes) == 0) {
    return {};
  }

  int err = errno;
  // treat not supported as nil error
  if (err == ENOTSUP) {
    // optimization only
    return {};
  }

  return std::error_code(err, std::generic_category());
}
}  // namespace lepton::storage::fileutil
#endif