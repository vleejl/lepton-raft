#include <gtest/gtest.h>
#include <rocksdb/env.h>
#include <rocksdb/status.h>

#include <filesystem>
#include <iostream>
#include <mutex>
#include <optional>
#include <string>

#include "lepton_error.h"
#include "raft.pb.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "test_file.h"
#include "types.h"
#include "utility_macros.h"
namespace lepton {

/*
// clang-format off
struct storage_builer : pro::facade_builder
  ::add_convention<storage_initial_state, leaf::result<std::tuple<raftpb::hard_state, raftpb::conf_state>>() const>
  ::add_convention<storage_entries, leaf::result<pb::repeated_entry>(std::uint64_t lo, std::uint64_t hi, std::uint64_t
max_size) const>
  ::add_convention<storage_term, leaf::result<std::uint64_t>(std::uint64_t i) const>
  ::add_convention<storage_last_index, leaf::result<std::uint64_t>() const>
  ::add_convention<storage_first_index, leaf::result<std::uint64_t>() const>
  ::add_convention<storage_snapshot, leaf::result<raftpb::snapshot>() const>
  ::add_skill<pro::skills::as_view>
  ::build{};
// clang-format on
 */

class rocksdb_storage {
 private:
  NOT_COPYABLE(rocksdb_storage)
  auto _first_index() const;
  auto _last_index() const;

 public:
  rocksdb_storage();
  rocksdb_storage(rocksdb_storage&& ms)
      : hard_state_(std::move(ms.hard_state_)), snapshot_(std::move(ms.snapshot_)), ents_(std::move(ms.ents_)){};

  leaf::result<std::tuple<raftpb::hard_state, raftpb::conf_state>> initial_state() const;

  leaf::result<pb::repeated_entry> entries(std::uint64_t lo, std::uint64_t hi, std::uint64_t max_size) const;

  leaf::result<std::uint64_t> term(std::uint64_t i) const;

  leaf::result<std::uint64_t> last_index() const;

  leaf::result<std::uint64_t> first_index() const;

  leaf::result<raftpb::snapshot> snapshot() const;

  //   const pb::repeated_entry& entries_view() const;

  leaf::result<void> set_hard_state(raftpb::hard_state&& hard_state);

  // ApplySnapshot overwrites the contents of this Storage object with
  // those of the given snapshot.
  leaf::result<void> apply_snapshot(raftpb::snapshot&& snapshot);

  // CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
  // can be used to reconstruct the state at that point.
  // If any configuration changes have been made since the last compaction,
  // the result of the last ApplyConfChange must be passed in.
  leaf::result<raftpb::snapshot> create_snapshot(std::uint64_t i, std::optional<raftpb::conf_state> cs,
                                                 std::string&& data);

  // Compact discards all log entries prior to compactIndex.
  // It is the application's responsibility to not attempt to compact an index
  // greater than raftLog.applied.
  leaf::result<void> compact(std::uint64_t compact_index);

  // Append the new entries to storage.
  // TODO (xiangli): ensure the entries are continuous and
  // entries[0].Index > ms.entries[0].Index
  leaf::result<void> append(pb::repeated_entry&& entries);

 private:
  // Protects access to all fields. Most methods of MemoryStorage are
  // run on the raft goroutine, but Append() is run on an application
  // goroutine.
  mutable std::mutex mutex_;

  raftpb::hard_state hard_state_;
  raftpb::snapshot snapshot_;

  // ents[i] has raft log position i+snapshot.Metadata.Index
  pb::repeated_entry ents_;
};
}  // namespace lepton

namespace lepton {
// leaf::result<std::tuple<raftpb::hard_state, raftpb::conf_state>> rocksdb_storage::initial_state() const {}
}  // namespace lepton

class rocksdb_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    delete_if_exists(db_path);
    std::cout << "run before first case..." << std::endl;
  }

  static void TearDownTestSuite() {
    delete_if_exists(db_path);
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
  delete_if_exists(temp_wal_file);
  delete_if_exists(temp_wal_log);
  std::unique_ptr<rocksdb::WritableFile> file;
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
}