#include <cstddef>
#if defined(__linux__)
#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <string>

#include "storage/fileutil/preallocate.h"

using namespace lepton::storage::fileutil;

namespace fs = std::filesystem;

class preallocate_linux_test_suit : public ::testing::Test {
 protected:
  void SetUp() override {
    // 创建临时文件
    char mask[] = "/tmp/prealloc_test_XXXXXX";
    int fd = mkstemp(mask);
    ASSERT_NE(fd, -1);
    temp_path_ = mask;
    fd_ = fd;
  }

  void TearDown() override {
    if (fd_ != -1) {
      close(fd_);
    }
    if (!temp_path_.empty()) {
      fs::remove(temp_path_);
    }
  }

  int fd_ = -1;
  std::string temp_path_;
};

// 对应 Go 的 TestPreallocateExtend
TEST_F(preallocate_linux_test_suit, preallocate_extend) {
  off_t size = 64 * 1000;

  std::error_code ec = prealloc_extend(fd_, size);
  ASSERT_FALSE(ec) << "prealloc_extend failed: " << ec.message();

  struct stat st;
  ASSERT_EQ(fstat(fd_, &st), 0);

  // 验证逻辑大小是否扩展
  EXPECT_EQ(st.st_size, size);
}

// 对应 Go 的 TestPreallocateExtendTrunc
TEST_F(preallocate_linux_test_suit, preallocate_extend_trunc) {
  off_t size = 64 * 1000;

  // 直接测试回退函数
  std::error_code ec = prealloc_extend_trunc(fd_, size);
  ASSERT_FALSE(ec) << "prealloc_extend_trunc failed: " << ec.message();

  struct stat st;
  ASSERT_EQ(fstat(fd_, &st), 0);

  // 验证逻辑大小是否扩展
  EXPECT_EQ(st.st_size, size);
}

// 对应 Go 的 TestPreallocateFixed
TEST_F(preallocate_linux_test_suit, preallocate_fixed) {
  off_t size = 64 * 1000;

  // 使用 FALLOC_FL_KEEP_SIZE，逻辑大小不应改变
  std::error_code ec = prealloc_fixed(fd_, size);
  ASSERT_FALSE(ec) << "prealloc_fixed failed: " << ec.message();

  struct stat st;
  ASSERT_EQ(fstat(fd_, &st), 0);

  // 验证逻辑大小保持为 0
  EXPECT_EQ(st.st_size, 0);

  // 可选：如果文件系统支持，可以验证 st_blocks 物理块是否分配，
  // 但这取决于具体文件系统实现，通常 st_size 为 0 即可满足测试意图。
}

// 验证偏移量（Offset）在操作后是否被正确恢复 (针对 prealloc_extend_trunc)
TEST_F(preallocate_linux_test_suit, preserve_offset_after_truncate) {
  off_t initial_offset = 10;
  size_t write_size = 5;
  write(fd_, "hello", write_size);  // 写入 5 字节，此时 offset 为 5

  off_t target_size = 1024;
  std::error_code ec = prealloc_extend_trunc(fd_, target_size);
  ASSERT_FALSE(ec);

  off_t current_offset = lseek(fd_, 0, SEEK_CUR);
  EXPECT_EQ(current_offset, 5) << "Offset was not preserved after prealloc_extend_trunc";
}
#endif