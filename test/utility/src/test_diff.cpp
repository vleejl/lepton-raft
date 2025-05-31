#include "test_diff.h"

#include <fcntl.h>
#include <fmt/format.h>
#include <unistd.h>

#include <array>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <string>

// 使用 RAII 类管理临时文件
class TemporaryFile {
 public:
  TemporaryFile(const std::string& body) : filename() {
    // 创建临时文件模板
    std::string pattern = "/tmp/diff_XXXXXX";

    // 在栈上分配缓冲区
    char buf[256];
    strncpy(buf, pattern.c_str(), sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';

    // 创建临时文件
    int fd = mkstemp(buf);
    if (fd == -1) {
      throw std::runtime_error("Failed to create temporary file");
    }

    // 写入内容
    if (write(fd, body.data(), body.size()) != static_cast<ssize_t>(body.size())) {
      close(fd);
      throw std::runtime_error("Failed to write to temporary file");
    }
    close(fd);

    filename = buf;
  }

  ~TemporaryFile() {
    if (!filename.empty()) {
      unlink(filename.c_str());
    }
  }

  const std::string& path() const { return filename; }

  // 禁用复制
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

  // 创建临时文件，使用 RAII 确保自动删除
  TemporaryFile file1(a);
  TemporaryFile file2(b);

  // 构建 diff 命令
  std::string cmd = "diff -u " + file1.path() + " " + file2.path();

  // 执行命令并捕获输出
  std::array<char, 256> buffer;
  std::string result;

  FILE* pipe = popen(cmd.c_str(), "r");
  if (!pipe) {
    throw std::runtime_error("Failed to execute diff command");
  }

  // RAII 管道资源管理
  auto pipe_closer = [](FILE* f) {
    if (f) pclose(f);
  };
  std::unique_ptr<FILE, decltype(pipe_closer)> pipe_guard(pipe, pipe_closer);

  while (fgets(buffer.data(), buffer.size(), pipe)) {
    result += buffer.data();
  }

  return result;
}

std::string ltoa(lepton::raft_log& raft_log_handle) {
  auto s = fmt::format("lastIndex: {}\n", raft_log_handle.last_index());
  s = s + fmt::format("applied:  {}\n", raft_log_handle.applied());
  s = s + fmt::format("applying:  {}\n", raft_log_handle.applying());
  auto entries = raft_log_handle.all_entries();
  for (auto i = 0; i < entries.size(); ++i) {
    s = s + fmt::format("#{}: {}\n", i, entries[i].DebugString());
  }
  return s;
}