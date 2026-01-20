#include <gtest/gtest.h>
#include <raft.pb.h>
#include <rocksdb/env.h>
#include <rocksdb/status.h>

#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "basic/utility_macros.h"
#include "error/error.h"
#include "raft_core/pb/types.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "storage/fileutil/path.h"
class rocksdb_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    lepton::storage::fileutil::remove(db_path);
    std::cout << "run before first case..." << std::endl;
  }

  static void TearDownTestSuite() {
    lepton::storage::fileutil::remove(db_path);
    std::cout << "run after last case..." << std::endl;
  }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }

  constexpr static auto db_path = "./testdb";
};

TEST_F(rocksdb_test_suit, basic_use) {
  ROCKSDB_NAMESPACE::DB* db;
  ROCKSDB_NAMESPACE::Options options;
  options.create_if_missing = true;

  // Open DB
  ROCKSDB_NAMESPACE::Status status = ROCKSDB_NAMESPACE::DB::Open(options, db_path, &db);
  ASSERT_TRUE(status.ok());

  // Put key-value
  status = db->Put(ROCKSDB_NAMESPACE::WriteOptions(), "key1", "value1");
  ASSERT_TRUE(status.ok());

  // Get value
  std::string value;
  status = db->Get(ROCKSDB_NAMESPACE::ReadOptions(), "key1", &value);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value, "value1");

  // Clean up
  delete db;
}

TEST_F(rocksdb_test_suit, rocks_file_opt) {
  rocksdb::Env* env = rocksdb::Env::Default();
  rocksdb::EnvOptions env_opts;

  // 1️⃣ 创建/打开文件
  constexpr auto temp_wal_file = "wal.tmp";
  constexpr auto temp_wal_log = "wal.log";
  lepton::storage::fileutil::remove(temp_wal_file);
  lepton::storage::fileutil::remove(temp_wal_log);
  std::unique_ptr<rocksdb::WritableFile> file;
  env->ReopenWritableFile(temp_wal_file, &file, env_opts);
  auto s = env->NewWritableFile(temp_wal_file, &file, env_opts);
  if (!s.ok()) {
    std::cerr << "Failed to create file: " << s.ToString() << std::endl;
    return;
  }
  std::cout << "File created: wal.tmp" << std::endl;

  // 2️⃣ 写入数据
  std::string data = "entry1\nentry2\n";
  s = file->Append(rocksdb::Slice(data));
  if (!s.ok()) {
    std::cerr << "Failed to write: " << s.ToString() << std::endl;
    return;
  }
  std::cout << "Data written" << std::endl;

  // 3️⃣ fsync（落盘）
  s = file->Sync();
  if (!s.ok()) {
    std::cerr << "Failed to sync: " << s.ToString() << std::endl;
    return;
  }
  std::cout << "File synced" << std::endl;

  // 4️⃣ 关闭文件
  s = file->Close();
  if (!s.ok()) {
    std::cerr << "Failed to close: " << s.ToString() << std::endl;
    return;
  }
  std::cout << "File closed" << std::endl;

  // 5️⃣ 重命名文件
  s = env->RenameFile(temp_wal_file, temp_wal_log);
  if (!s.ok()) {
    std::cerr << "Failed to rename: " << s.ToString() << std::endl;
    return;
  }
  std::cout << "File renamed to wal.log" << std::endl;

  // 6️⃣ 读取文件内容
  std::unique_ptr<rocksdb::SequentialFile> rfile;
  s = env->NewSequentialFile(temp_wal_log, &rfile, env_opts);
  if (!s.ok()) {
    std::cerr << "Failed to open for read: " << s.ToString() << std::endl;
    return;
  }

  char buffer[1024];
  rocksdb::Slice result;
  s = rfile->Read(sizeof(buffer), &result, buffer);
  if (!s.ok()) {
    std::cerr << "Failed to read: " << s.ToString() << std::endl;
    return;
  }

  std::cout << "Read data:\n" << std::string(result.data(), result.size()) << std::endl;

  std::unique_ptr<rocksdb::RandomAccessFile> rwfile;
  s = env->NewRandomAccessFile(temp_wal_log, &rwfile, env_opts);
  if (!s.ok()) {
    std::cerr << "Failed to open for read: " << s.ToString() << std::endl;
    return;
  }
}