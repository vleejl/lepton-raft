#include "test_diff.h"

#include <fcntl.h>
#include <fmt/format.h>

#include <array>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>

#include "dtl/dtl.hpp"

#if defined(_WIN32)
#include <fcntl.h>
#include <io.h>
#include <sys/stat.h>
#include <windows.h>

#else
#include <fcntl.h>
#include <unistd.h>

#endif

// 使用 RAII 类管理临时文件
class TemporaryFile {
 public:
  TemporaryFile(const std::string& body) {
#if defined(_WIN32)
    wchar_t temp_path[MAX_PATH];
    if (GetTempPathW(MAX_PATH, temp_path) == 0) {
      throw std::runtime_error("Failed to get temp path");
    }

    wchar_t temp_file[MAX_PATH];
    if (GetTempFileNameW(temp_path, L"diff", 0, temp_file) == 0) {
      throw std::runtime_error("Failed to create temp file name");
    }

    // 转换到 UTF-8 存储
    int len = WideCharToMultiByte(CP_UTF8, 0, temp_file, -1, nullptr, 0, nullptr, nullptr);
    if (len <= 0) {
      throw std::runtime_error("Failed to convert filename to UTF-8");
    }
    std::string utf8_filename(len - 1, '\0');
    WideCharToMultiByte(CP_UTF8, 0, temp_file, -1, utf8_filename.data(), len, nullptr, nullptr);
    filename = utf8_filename;

    // 打开文件并写入
    int fd = _open(filename.c_str(), _O_WRONLY | _O_BINARY | _O_TRUNC, _S_IREAD | _S_IWRITE);
    if (fd == -1) {
      throw std::runtime_error("Failed to open temp file for writing");
    }
    if (_write(fd, body.data(), static_cast<unsigned>(body.size())) != static_cast<int>(body.size())) {
      _close(fd);
      throw std::runtime_error("Failed to write to temp file");
    }
    _close(fd);

#else
    std::string pattern = "/tmp/diff_XXXXXX";
    char buf[256];
    strncpy(buf, pattern.c_str(), sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';

    int fd = mkstemp(buf);
    if (fd == -1) {
      throw std::runtime_error("Failed to create temporary file");
    }

    if (write(fd, body.data(), body.size()) != static_cast<ssize_t>(body.size())) {
      close(fd);
      throw std::runtime_error("Failed to write to temporary file");
    }
    close(fd);

    filename = buf;
#endif
  }

  ~TemporaryFile() {
    if (!filename.empty()) {
#if defined(_WIN32)
      _unlink(filename.c_str());
#else
      unlink(filename.c_str());
#endif
    }
  }

  const std::string& path() const { return filename; }

  TemporaryFile(const TemporaryFile&) = delete;
  TemporaryFile& operator=(const TemporaryFile&) = delete;

 private:
  std::string filename;
};

// 比较两个字符串并返回 diff 结果的函数
std::string diffu(const std::string& a, const std::string& b) {
  if (a == b) {
    return "";
  }

  // 按行拆分
  std::vector<std::string> lines_a;
  std::vector<std::string> lines_b;

  {
    std::istringstream iss(a);
    std::string line;
    while (std::getline(iss, line)) {
      lines_a.push_back(line);
    }
  }

  {
    std::istringstream iss(b);
    std::string line;
    while (std::getline(iss, line)) {
      lines_b.push_back(line);
    }
  }

  // 使用 dtl diff
  dtl::Diff<std::string> diff(lines_a, lines_b);
  diff.compose();
  diff.composeUnifiedHunks();

  std::ostringstream oss;
  diff.printUnifiedFormat(oss);  // 输出 unified diff 格式

  return oss.str();
}
std::string ltoa(lepton::core::raft_log& raft_log_handle) {
  auto s = fmt::format("lastIndex: {}\n", raft_log_handle.last_index());
  s = s + fmt::format("applied:  {}\n", raft_log_handle.applied());
  s = s + fmt::format("applying:  {}\n", raft_log_handle.applying());
  auto entries = raft_log_handle.all_entries();
  for (auto i = 0; i < entries.size(); ++i) {
    s = s + fmt::format("#{}: {}\n", i, entries[i].DebugString());
  }
  return s;
}