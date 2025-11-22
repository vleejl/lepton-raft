#include "preallocate.h"

#include <cerrno>
#include <cstdint>
#include <system_error>

#if defined(__linux__)
#include <fcntl.h>
#include <unistd.h>
#elif defined(__APPLE__) || defined(__FreeBSD__)
#include <unistd.h>
#elif defined(_WIN32)
#include <windows.h>
#else
#error "Unsupported platform"
#endif
namespace lepton {

std::error_code preallocate(int fd, uint64_t length) {
#if defined(__linux__)

  // 优先使用 posix_fallocate（不改变文件 offset，也不创建 sparse file）
  int rc = ::posix_fallocate(fd, 0, static_cast<off_t>(length));
  if (rc != 0) {
    // posix_fallocate 返回错误码本身，不走 errno
    return std::error_code(rc, std::generic_category());
  }
  return {};

#elif defined(__APPLE__) || defined(__FreeBSD__)

  // macOS 没有 fallocate，只能用 ftruncate（可能产生 sparse file）
  int rc = ::ftruncate(fd, static_cast<off_t>(length));
  if (rc != 0) {
    return std::error_code(errno, std::generic_category());
  }
  return {};

#elif defined(_WIN32)

  HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
  if (h == INVALID_HANDLE_VALUE) {
    return std::error_code(errno, std::generic_category());
  }

  LARGE_INTEGER size;
  size.QuadPart = length;

  if (!SetFileInformationByHandle(h, FileEndOfFileInfo, &size, sizeof(size))) {
    return std::error_code(GetLastError(), std::system_category());
  }
  return {};
#endif
}

}  // namespace lepton
