#include <fmt/core.h>
#include <fmt/ranges.h>
#include <gtest/gtest.h>
#include <raft.pb.h>

#include <algorithm>
#include <asio/use_future.hpp>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>
#include <regex>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "asio/error.hpp"
#include "asio/error_code.hpp"
#include "basic/logger.h"
#include "basic/pbutil.h"
#include "basic/spdlog_logger.h"
#include "error/io_error.h"
#include "error/leaf_expected.h"
#include "error/logic_error.h"
#include "error/raft_error.h"
#include "error/wal_error.h"
#include "fmt/format.h"
#include "raft_core/pb/types.h"
#include "storage/fileutil/file_endpoint.h"
#include "storage/fileutil/path.h"
#include "storage/fileutil/read_dir.h"
#include "storage/ioutil/byte_span.h"
#include "storage/ioutil/io.h"
#include "storage/ioutil/reader.h"
#include "storage/pb/types.h"
#include "storage/wal/encoder.h"
#include "storage/wal/wal.h"
#include "storage/wal/wal_file.h"
#include "test_raft_protobuf.h"
#include "test_utility_macros.h"
#include "wal.pb.h"
using namespace lepton::storage::fileutil;
using namespace lepton::storage::wal;
using namespace lepton::storage;
using namespace std::string_literals;
namespace fs = std::filesystem;

constexpr int private_file_mode = 0'600;

enum class file_endpint_type {
  standard,
  locked,
};

struct write_buf {
  asio::awaitable<lepton::expected<std::size_t>> async_write(ioutil::byte_span data) {
    buf.insert(buf.end(), data.data(), data.data() + data.size());
    co_return data.size();
  }
  std::vector<std::byte> buf;
};

static auto walpb_record(::walpb::RecordType type, std::uint32_t crc, const std::string& data) {
  walpb::Record record;
  record.set_type(type);
  if (type == ::walpb::RecordType::CRC_TYPE) {
    record.set_crc(crc);
  }
  if (!data.empty()) {
    record.set_data(data);
  }
  return record;
}

static lepton::leaf::result<std::unique_ptr<file_endpoint>> create_readonly_file(const std::string& filename,
                                                                                 asio::any_io_executor executor) {
  asio::stream_file stream_file(executor);
  asio::file_base::flags open_flags = asio::random_access_file::read_only;

  asio::error_code ec;
  stream_file.open(filename, open_flags, ec);  // NOLINT(bugprone-unused-return-value)
  if (ec) {
    LOG_ERROR("Failed to create WAL file {}: {}", filename, ec.message());
    return lepton::new_error(ec, fmt::format("Failed to create WAL file {}: {}", filename, ec.message()));
  }
  BOOST_LEAF_AUTO(file_handle, fileutil::create_file_endpoint(executor, filename, open_flags));
  return std::make_unique<file_endpoint>(std::move(file_handle));
}

static lepton::leaf::result<void> write_file(asio::any_io_executor executor, const std::string& filename,
                                             ioutil::byte_span data) {
  BOOST_LEAF_AUTO(file_handle, create_new_wal_file(executor, filename, true));
  BOOST_LEAF_AUTO(size, file_handle.write(data));
  EXPECT_EQ(size, data.size());
  return {};
}

static lepton::leaf::result<std::string> read_file_content(asio::any_io_executor executor,
                                                           const std::string& filename) {
  BOOST_LEAF_AUTO(file_handle, create_readonly_file(filename, executor));
  std::vector<std::byte> buffer(1024);
  asio::mutable_buffer read_buffer = asio::buffer(buffer);
  BOOST_LEAF_AUTO(size, file_handle->read(read_buffer));
  return std::string(reinterpret_cast<char*>(buffer.data()), size);
}

/**
 * @brief 比较两个 std::vector<std::byte> 并打印详细的差异点
 * * @param v1 第一个向量
 * @param v2 第二个向量
 * @param name1 第一个向量的名字（用于打印）
 * @param name2 第二个向量的名字（用于打印）
 * @return true 内容完全一致
 * @return false 内容不一致
 */
static bool compare_and_dump_diff(const std::vector<std::byte>& v1, const std::vector<std::byte>& v2,
                                  const std::string& name1 = "vec1", const std::string& name2 = "vec2") {
  bool equal = true;

  // 1. 检查长度
  if (v1.size() != v2.size()) {
    std::cout << "[DIFF] Size mismatch: " << name1 << ".size() = " << v1.size() << ", " << name2
              << ".size() = " << v2.size() << std::endl;
    equal = false;
  }

  // 2. 找到第一个不一样的字节
  size_t min_size = std::min(v1.size(), v2.size());
  size_t first_diff_idx = min_size;

  for (size_t i = 0; i < min_size; ++i) {
    if (v1[i] != v2[i]) {
      first_diff_idx = i;
      equal = false;
      break;
    }
  }

  if (equal) {
    return true;
  }

  // 3. 打印详细差异
  if (first_diff_idx < min_size) {
    std::cout << "[DIFF] First difference at index: " << first_diff_idx << std::endl;

    // 打印上下文（前后各 8 个字节）
    auto print_context = [&](const std::vector<std::byte>& v, const std::string& name) {
      std::cout << name << " [" << first_diff_idx << "]: ";

      size_t start = (first_diff_idx > 8) ? first_diff_idx - 8 : 0;
      size_t end = std::min(v.size(), first_diff_idx + 8);

      std::cout << "... ";
      for (size_t i = start; i < end; ++i) {
        if (i == first_diff_idx) std::cout << ">";  // 高亮标记不同点
        std::cout << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(v[i]) << " ";
        if (i == first_diff_idx) std::cout << "<";
      }
      std::cout << std::dec << "..." << std::endl;
    };

    print_context(v1, name1);
    print_context(v2, name2);
  } else if (v1.size() != v2.size()) {
    std::cout << "[DIFF] Contents are identical up to the end of the shorter vector." << std::endl;
  }

  return false;
}

static void create_with_fstream(const std::string& name) {
  // std::ios::in | std::ios::out: 读写模式
  // std::ios::trunc: 如果存在则清空
  std::fstream file_handle(name, std::ios::in | std::ios::out | std::ios::trunc | std::ios::binary);

  if (!file_handle.is_open()) {
    // 如果文件不存在，上面的组合在某些实现下可能不会创建文件
    // 建议先用 out 模式创建
    file_handle.open(name, std::ios::out | std::ios::binary);
    file_handle.close();
    file_handle.open(name, std::ios::in | std::ios::out | std::ios::binary);
  }
}

static raftpb::ConfState create_conf_state() {
  raftpb::ConfState cs;
  cs.add_voters(0x00ffca74);
  cs.set_auto_leave(false);
  return cs;
}

static std::string make_test_data(std::size_t size) {
  std::string data(size, '\0');
  uint32_t x = 0xdeadbeef;
  for (auto& c : data) {
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    c = static_cast<char>(x & 0xff);
  }
  return data;
}

class wal_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override {
    remove_all(wal_dir_path);
    std::cout << "enter from SetUp" << std::endl;
  }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }

  constexpr static auto wal_dir_path = "./wal_test_dir";
};

TEST_F(wal_test_suit, test_new_wal) {
  remove_all(wal_dir_path);
  asio::io_context io_context;
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_dir_path, "somedata",
                                              SEGMENT_SIZE_BYTES, std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_result);
        auto& wal_handle = wal_result.value();
        EXPECT_TRUE(wal_handle);

        auto tail_wal_file_handle = wal_handle->tail();
        EXPECT_TRUE(tail_wal_file_handle);
        EXPECT_FALSE(tail_wal_file_handle->name().empty());
        EXPECT_EQ(fileutil::base_name(tail_wal_file_handle->name()), wal_file_name(0, 0));

        // file is preallocated to segment size; only read data written by wal
        auto seek_result = lepton::leaf_to_expected([&]() { return tail_wal_file_handle->seek_curr(); });
        EXPECT_TRUE(seek_result);
        auto seek_value = *seek_result;
        EXPECT_NE(seek_value, 0);

        auto tail_file_path = join_paths(wal_dir_path, fileutil::base_name(tail_wal_file_handle->name()));
        auto read_file_result = create_readonly_file(tail_file_path, io_context.get_executor());
        EXPECT_TRUE(read_file_result);
        auto& read_file = read_file_result.value();
        assert(seek_value > 0);
        std::vector<std::byte> buff(static_cast<std::size_t>(seek_value));
        asio::mutable_buffer read_buffer = asio::buffer(buff);
        pro::proxy_view<ioutil::reader> r = read_file.get();
        auto read_full_result = co_await ioutil::read_full(r, read_buffer);
        EXPECT_TRUE(read_full_result) << tail_file_path;
        auto read_len_res = *read_full_result;
        EXPECT_NE(0, read_len_res) << tail_file_path;

        write_buf wb;
        pro::proxy_view<ioutil::writer> w = &wb;
        encoder encoder_handle{io_context.get_executor(), w, 0, 0, std::make_shared<lepton::spdlog_logger>()};

        walpb::Record record = walpb_record(walpb::RecordType::CRC_TYPE, 0, "");
        auto encode_result = co_await encoder_handle.encode(record);
        EXPECT_TRUE(encode_result);

        record = walpb_record(walpb::RecordType::METADATA_TYPE, 0, "somedata");
        encode_result = co_await encoder_handle.encode(record);
        EXPECT_TRUE(encode_result);
        fmt::print("{:02x}\n", fmt::join(wb.buf, " "));

        record = walpb_record(walpb::RecordType::SNAPSHOT_TYPE, 0, lepton::storage::pb::snapshot().SerializeAsString());
        encode_result = co_await encoder_handle.encode(record);
        EXPECT_TRUE(encode_result);
        fmt::print("{:02x}\n", fmt::join(wb.buf, " "));

        auto flush_result = co_await encoder_handle.flush();
        EXPECT_TRUE(flush_result);

        EXPECT_TRUE(compare_and_dump_diff(wb.buf, buff, "wb.buf", "buff"));
        co_await wal_handle->close();
        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(wal_test_suit, test_create_new_wal_file) {
  asio::io_context io_context;
  rocksdb::Env* env = rocksdb::Env::Default();
  struct test_case {
    std::string name;
    file_endpint_type type;
    bool force_new;
  };

  std::vector<test_case> test_cases = {
      {"standard_no_force_new.wal", file_endpint_type::standard, false},
      {"standard_force_new.wal", file_endpint_type::standard, true},
      {"locked_no_force_new.wal", file_endpint_type::locked, false},
      {"locked_force_new.wal", file_endpint_type::locked, true},
  };

  for (const auto& tc : test_cases) {
    remove_all(wal_dir_path);
    EXPECT_TRUE(std::filesystem::create_directory(wal_dir_path));
    const auto file_path = join_paths(wal_dir_path, tc.name);
    // create initial file with some data to verify truncate behavior
    std::string initial_data = "test data";
    EXPECT_TRUE(write_file(io_context.get_executor(), file_path, ioutil::to_bytes(initial_data)));

    if (tc.type == file_endpint_type::standard) {
      auto result = create_new_wal_file(io_context.get_executor(), file_path, tc.force_new);
      ASSERT_TRUE(result);
    } else {
      auto result = create_new_wal_file(env, io_context.get_executor(), file_path, tc.force_new);
      ASSERT_TRUE(result);
    }

    std::error_code ec;
    fs::file_status s = fs::status(file_path, ec);
    ASSERT_FALSE(ec);
    auto actual_perms = s.permissions();
    uint32_t actual_mode = static_cast<uint32_t>(actual_perms & fs::perms::mask);
    ASSERT_EQ(private_file_mode, actual_mode);

    auto read_result =
        lepton::leaf_to_expected([&] { return read_file_content(io_context.get_executor(), file_path); });
    if (!read_result) {
      if (tc.force_new) {
        ASSERT_EQ(asio::error::eof, read_result.error());
      } else {
        ASSERT_TRUE(false) << read_result.error().message();
      }
    }
    if (!tc.force_new) {
      auto& content = read_result.value();
      ASSERT_EQ(initial_data, content);
    }
  }
}

TEST_F(wal_test_suit, test_create_fail_from_polluted_dir) {
  asio::io_context io_context;
  rocksdb::Env* env = rocksdb::Env::Default();

  EXPECT_TRUE(std::filesystem::create_directory(wal_dir_path));
  const auto file_path = join_paths(wal_dir_path, "test.wal");
  ASSERT_TRUE(write_file(io_context.get_executor(), file_path, ioutil::to_bytes("data")));

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_dir_path, "data", SEGMENT_SIZE_BYTES,
                                              std::make_shared<lepton::spdlog_logger>());
        EXPECT_FALSE(wal_result);
        EXPECT_EQ(wal_result.error(), lepton::io_error::PARH_HAS_EXIT);
        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(wal_test_suit, test_wal_cleanup) {
  asio::io_context io_context;
  rocksdb::Env* env = rocksdb::Env::Default();

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_root = wal_dir_path;
        EXPECT_TRUE(std::filesystem::create_directory(wal_root));

        auto wal_test_dir = join_paths(wal_root, "wal_test");
        auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_test_dir, "", SEGMENT_SIZE_BYTES,
                                              std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_result);

        auto& wal_handle = wal_result.value();
        EXPECT_TRUE(wal_handle);
        wal_handle->cleanup_wal();

        auto read_dir_result = fileutil::read_dir(wal_root);
        EXPECT_TRUE(read_dir_result);
        auto& entries = read_dir_result.value();
        EXPECT_EQ(entries.size(), 1);

        std::string base_name = fs::path(wal_test_dir).filename().string();
        std::string pattern_str = fmt::format(R"({}\.broken\.[0-9]{{8}}\.[0-9]{{6}}\.[0-9]{{1,6}}?)", base_name);
        std::regex pattern(pattern_str);
        EXPECT_TRUE(std::regex_match(entries[0], pattern)) << entries[0];

        co_await wal_handle->close();
        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(wal_test_suit, test_create_fail_from_no_space_left) {
  asio::io_context io_context;
  rocksdb::Env* env = rocksdb::Env::Default();

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_root = wal_dir_path;
        EXPECT_TRUE(std::filesystem::create_directory(wal_root));

        auto wal_test_dir = join_paths(wal_root, "wal_test");
        auto wal_result =
            co_await create_wal(env, io_context.get_executor(), wal_test_dir, "data",
                                std::numeric_limits<std::size_t>::max(), std::make_shared<lepton::spdlog_logger>());
        EXPECT_FALSE(wal_result);
        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(wal_test_suit, test_new_for_inited_dir) {
  asio::io_context io_context;
  rocksdb::Env* env = rocksdb::Env::Default();

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_root = wal_dir_path;
        EXPECT_TRUE(std::filesystem::create_directory(wal_root));
        auto wal_test_dir = join_paths(wal_root, "wal_test");
        EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));
        create_with_fstream(join_paths(wal_test_dir, wal_file_name(0, 0)));

        auto wal_result =
            co_await create_wal(env, io_context.get_executor(), wal_test_dir, "data",
                                std::numeric_limits<std::size_t>::max(), std::make_shared<lepton::spdlog_logger>());
        EXPECT_FALSE(wal_result);
        EXPECT_EQ(wal_result.error(), lepton::io_error::PARH_HAS_EXIT);
        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(wal_test_suit, test_open_at_index) {
  auto wal_root = wal_dir_path;
  EXPECT_TRUE(std::filesystem::create_directory(wal_root));
  auto wal_test_dir = join_paths(wal_root, "wal_test");
  EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));

  rocksdb::Env* env = rocksdb::Env::Default();

  SECTION("wal_file_0_0") {
    asio::io_context io_context;
    create_with_fstream(join_paths(wal_test_dir, wal_file_name(0, 0)));
    auto wal_handle_result = open(env, io_context.get_executor(), wal_test_dir, SEGMENT_SIZE_BYTES,
                                  lepton::storage::pb::snapshot{}, std::make_shared<lepton::spdlog_logger>());
    ASSERT_TRUE(wal_handle_result);
    auto& wal_handle = wal_handle_result.value();
    ASSERT_EQ(fileutil::base_name(wal_handle->tail()->name()), wal_file_name(0, 0));
    ASSERT_EQ(0, wal_handle->seq());
    asio::co_spawn(
        io_context,
        [&]() -> asio::awaitable<void> {
          co_await wal_handle->close();
          co_return;
        },
        asio::use_future);
    io_context.run();
  }

  SECTION("wal_file_2_10") {
    asio::io_context io_context;
    create_with_fstream(join_paths(wal_test_dir, wal_file_name(2, 10)));

    lepton::storage::pb::snapshot snap;
    snap.set_index(5);
    auto wal_handle_result = open(env, io_context.get_executor(), wal_test_dir, SEGMENT_SIZE_BYTES, std::move(snap),
                                  std::make_shared<lepton::spdlog_logger>());
    ASSERT_TRUE(wal_handle_result);
    auto& wal_handle = wal_handle_result.value();
    ASSERT_EQ(fileutil::base_name(wal_handle->tail()->name()), wal_file_name(2, 10));
    ASSERT_EQ(2, wal_handle->seq());
    asio::co_spawn(
        io_context,
        [&]() -> asio::awaitable<void> {
          co_await wal_handle->close();
          co_return;
        },
        asio::use_future);
    io_context.run();
  }

  SECTION("empty_dir") {
    asio::io_context io_context;
    wal_test_dir = join_paths(wal_root, "wal_test_2");
    EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));
    auto wal_handle_result = lepton::leaf_to_expected([&]() {
      return open(env, io_context.get_executor(), wal_test_dir, SEGMENT_SIZE_BYTES, lepton::storage::pb::snapshot{},
                  std::make_shared<lepton::spdlog_logger>());
    });
    ASSERT_FALSE(wal_handle_result);
    ASSERT_EQ(lepton::make_error_code(lepton::logic_error::ERR_FILE_NOT_FOUND), wal_handle_result.error());
  }
}

// TestVerify tests that Verify throws a non-nil error when the WAL is corrupted.
// The test creates a WAL directory and cuts out multiple WAL files. Then
// it corrupts one of the files by completely truncating it.
TEST_F(wal_test_suit, test_verify) {
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::io_context io_context;
  auto wal_root = wal_dir_path;
  EXPECT_TRUE(std::filesystem::create_directory(wal_root));
  auto wal_test_dir = join_paths(wal_root, "wal_test");
  EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_test_dir, "", SEGMENT_SIZE_BYTES,
                                              std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_result);
        auto wal_handle = wal_result.value();
        // make 5 separate files
        for (std::size_t i = 0; i < 5; ++i) {
          lepton::core::pb::repeated_entry entries;
          auto entry = entries.Add();
          entry->set_index(i);
          entry->set_data(fmt::format("waldata{}", i + 1));
          AWAIT_EXPECT_OK(wal_handle->save(entries, raftpb::HardState{}));
          AWAIT_EXPECT_OK(wal_handle->cut());
        }

        raftpb::HardState hs;
        hs.set_term(1);
        hs.set_vote(3);
        hs.set_commit(5);
        AWAIT_EXPECT_OK(wal_handle->save(lepton::core::pb::repeated_entry{}, raftpb::HardState{hs}));

        // to verify the WAL is not corrupted at this point
        auto hs_result = co_await verify(io_context.get_executor(), wal_test_dir, lepton::storage::pb::snapshot{},
                                         std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(hs_result);
        EXPECT_EQ(hs.DebugString(), hs_result->DebugString()) << hs.DebugString() << hs_result->DebugString();

        auto wal_files_result = fileutil::read_dir(wal_test_dir);
        EXPECT_TRUE(wal_files_result);
        auto& wal_files = wal_files_result.value();
        // 至少手动 cut了 5 次
        EXPECT_GE(wal_files.size(), 5);

        // corrupt the WAL by truncating one of the WAL files completely
        std::error_code ec;
        std::filesystem::resize_file(fileutil::join_paths(wal_test_dir, wal_files[2]), 0, ec);
        EXPECT_FALSE(ec);
        hs_result = co_await verify(io_context.get_executor(), wal_test_dir, lepton::storage::pb::snapshot{},
                                    std::make_shared<lepton::spdlog_logger>());
        EXPECT_FALSE(hs_result);

        co_await wal_handle->close();
        co_return;
      },
      asio::use_future);
  io_context.run();
}

// TestCut tests cut
TEST_F(wal_test_suit, test_cut) {
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::io_context io_context;
  auto wal_root = wal_dir_path;
  EXPECT_TRUE(std::filesystem::create_directory(wal_root));
  auto wal_test_dir = join_paths(wal_root, "wal_test");
  EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_test_dir, "", SEGMENT_SIZE_BYTES,
                                              std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_result);
        auto wal_handle = wal_result.value();

        raftpb::HardState hs;
        hs.set_term(1);
        AWAIT_EXPECT_OK(wal_handle->save(lepton::core::pb::repeated_entry{}, raftpb::HardState{hs}));
        AWAIT_EXPECT_OK(wal_handle->cut());

        EXPECT_TRUE(wal_handle->tail());
        EXPECT_FALSE(wal_handle->tail()->name().empty());
        EXPECT_EQ(fileutil::base_name(wal_handle->tail()->name()), wal_file_name(1, 1));

        lepton::core::pb::repeated_entry entries;
        auto entry = entries.Add();
        entry->set_index(1);
        entry->set_term(1);
        entry->set_data(fmt::format("waldata{}", 1));
        AWAIT_EXPECT_OK(wal_handle->save(entries, raftpb::HardState{}));
        AWAIT_EXPECT_OK(wal_handle->cut());
        EXPECT_TRUE(wal_handle->tail());
        EXPECT_FALSE(wal_handle->tail()->name().empty());
        auto wname = wal_file_name(2, 2);
        EXPECT_EQ(fileutil::base_name(wal_handle->tail()->name()), wname);

        lepton::storage::pb::snapshot snap;
        snap.set_index(2);
        snap.set_term(1);
        snap.mutable_conf_state()->CopyFrom(create_conf_state());
        AWAIT_EXPECT_OK(wal_handle->save_snapshot(snap));

        // check the state in the last WAL
        // We do check before closing the WAL to ensure that Cut syncs the data
        // into the disk.
        auto nw_handle = std::make_shared<lepton::storage::wal::wal>(io_context.get_executor(), env, SEGMENT_SIZE_BYTES,
                                                                     std::make_shared<lepton::spdlog_logger>());
        auto& nw = *nw_handle;
        nw.start_.CopyFrom(snap);
        std::vector<wal_segment> segments;
        auto filepath = fileutil::join_paths(wal_test_dir, wname);
        auto file_handle_result = create_readonly_file(filepath, io_context.get_executor());
        EXPECT_TRUE(file_handle_result) << filepath;
        segments.push_back(std::move(*file_handle_result));
        nw.decoder_ = std::make_unique<decoder>(io_context.get_executor(), nw.logger_, to_reader_views(segments));
        auto read_all_result = co_await nw.read_all();
        EXPECT_TRUE(read_all_result);
        EXPECT_EQ(std::error_code{}, read_all_result->ec);
        EXPECT_EQ(read_all_result->state.DebugString(), hs.DebugString())
            << read_all_result->state.DebugString() << hs.DebugString();

        co_await wal_handle->close();
        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(wal_test_suit, test_save_with_cut) {
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::io_context io_context;
  auto wal_root = wal_dir_path;
  EXPECT_TRUE(std::filesystem::create_directory(wal_root));
  auto wal_test_dir = join_paths(wal_root, "wal_test");
  EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_test_dir, "metadata",
                                              SEGMENT_SIZE_BYTES, std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_result);
        auto wal_handle = wal_result.value();

        raftpb::HardState hs;
        hs.set_term(1);
        AWAIT_EXPECT_OK(wal_handle->save(lepton::core::pb::repeated_entry{}, raftpb::HardState{hs}));

        auto seek_curr_result = lepton::leaf_to_expected(
            [&]() -> lepton::leaf::result<std::uint64_t> { return wal_handle->tail()->seek_curr(); });
        EXPECT_TRUE(seek_curr_result);

        std::string big_data(500, '\0');
        std::string strdata = "Hello World!!";
        // 从索引 0 开始，替换 strdata 长度的内容
        big_data.replace(0, strdata.length(), strdata);
        constexpr auto entry_size = 500uz + 8 + 8;
        // set a lower value for SegmentSizeBytes, else the test takes too long to complete
        std::size_t segment_size_bytes = 2 * 1024 + 300;  // 需要大于等于 4 个 entri size 大小
        wal_handle->segment_size_bytes_ = segment_size_bytes;
        auto index = 0uz;
        for (auto total_size = 0uz; total_size < segment_size_bytes; total_size += entry_size) {
          lepton::core::pb::repeated_entry entries;
          auto entry = entries.Add();
          entry->set_index(index);
          entry->set_term(1);
          entry->set_data(big_data);
          AWAIT_EXPECT_OK(wal_handle->save(entries, raftpb::HardState{}));
          ++index;
        }
        co_await wal_handle->close();

        auto new_wal_handle_result = open(env, io_context.get_executor(), wal_test_dir, segment_size_bytes,
                                          lepton::storage::pb::snapshot{}, std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(new_wal_handle_result);
        auto& new_wal_handle = new_wal_handle_result.value();
        EXPECT_EQ(fileutil::base_name(new_wal_handle->tail()->name()), wal_file_name(1, index));
        auto read_all_result = co_await new_wal_handle->read_all();
        EXPECT_TRUE(read_all_result) << read_all_result.error().message();
        EXPECT_EQ(std::error_code{}, read_all_result->ec);
        EXPECT_EQ(read_all_result->state.DebugString(), hs.DebugString())
            << read_all_result->state.DebugString() << hs.DebugString();
        EXPECT_EQ(read_all_result->entries.size(), segment_size_bytes / entry_size);
        for (const auto& entry : read_all_result->entries) {
          EXPECT_EQ(entry.data(), big_data);
        }

        co_await new_wal_handle->close();
        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(wal_test_suit, test_recover) {
  rocksdb::Env* env = rocksdb::Env::Default();

  struct test_case {
    std::string name;
    std::size_t size;
  };
  std::vector<test_case> cases = {
      {"10MB", 10 * 1024 * 1024},
      {"20MB", 20 * 1024 * 1024},
      {"40MB", 40 * 1024 * 1024},
  };
  std::vector<std::string> test_payloads;
  for (const auto& tc : cases) {
    test_payloads.push_back(make_test_data(tc.size));
  }

  for (std::size_t i = 0; i < cases.size(); ++i) {
    const auto& tc = cases[i];
    asio::io_context io_context;
    remove_all(wal_dir_path);
    auto wal_root = wal_dir_path;
    EXPECT_TRUE(std::filesystem::create_directory(wal_root));
    auto wal_test_dir = join_paths(wal_root, "wal_test");
    EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));

    asio::co_spawn(
        io_context,
        [&]() -> asio::awaitable<void> {
          auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_test_dir, "metadata",
                                                SEGMENT_SIZE_BYTES, std::make_shared<lepton::spdlog_logger>());
          EXPECT_TRUE(wal_result);
          auto wal_handle = wal_result.value();
          AWAIT_EXPECT_OK(wal_handle->save_snapshot(lepton::storage::pb::snapshot{}));

          const auto& data = test_payloads[i];
          EXPECT_EQ(data.size(), tc.size) << "Case: " << tc.name;

          lepton::core::pb::repeated_entry entries;
          {
            auto entry = entries.Add();
            entry->set_index(1);
            entry->set_term(1);
            entry->set_data(data);
          }
          {
            auto entry = entries.Add();
            entry->set_index(2);
            entry->set_term(2);
            entry->set_data(data);
          }
          AWAIT_EXPECT_OK(wal_handle->save(entries, raftpb::HardState{}));

          {
            raftpb::HardState hs;
            hs.set_term(1);
            hs.set_vote(1);
            hs.set_commit(1);
            AWAIT_EXPECT_OK(wal_handle->save(lepton::core::pb::repeated_entry{}, raftpb::HardState{hs}));
          }
          raftpb::HardState hs;
          hs.set_term(2);
          hs.set_vote(2);
          hs.set_commit(2);
          AWAIT_EXPECT_OK(wal_handle->save(lepton::core::pb::repeated_entry{}, raftpb::HardState{hs}));
          co_await wal_handle->close();

          auto new_wal_handle_result = open(env, io_context.get_executor(), wal_test_dir, SEGMENT_SIZE_BYTES,
                                            lepton::storage::pb::snapshot{}, std::make_shared<lepton::spdlog_logger>());
          EXPECT_TRUE(new_wal_handle_result);
          auto& new_wal_handle = new_wal_handle_result.value();

          auto read_all_result = co_await new_wal_handle->read_all();
          EXPECT_TRUE(read_all_result) << read_all_result.error().message();
          EXPECT_EQ(std::error_code{}, read_all_result->ec);
          EXPECT_EQ(read_all_result->metadata, "metadata");
          EXPECT_TRUE(entries == read_all_result->entries);
          // only the latest state is recorded
          EXPECT_EQ(read_all_result->state.DebugString(), hs.DebugString())
              << read_all_result->state.DebugString() << hs.DebugString();

          co_await new_wal_handle->close();
          co_return;
        },
        asio::use_future);
    io_context.run();
  }
}

TEST_F(wal_test_suit, search_index) {
  struct test_case {
    std::vector<std::string> names;
    uint64_t index;
    int widx;
    bool wok;
  };

  std::vector<test_case> tests = {
      {{"0000000000000000-0000000000000000.wal", "0000000000000001-0000000000001000.wal",
        "0000000000000002-0000000000002000.wal"},
       0x1000,
       1,
       true},
      {{"0000000000000001-0000000000004000.wal", "0000000000000002-0000000000003000.wal",
        "0000000000000003-0000000000005000.wal"},
       0x4000,
       1,
       true},
      {{"0000000000000001-0000000000002000.wal", "0000000000000002-0000000000003000.wal",
        "0000000000000003-0000000000005000.wal"},
       0x1000,
       -1,
       false},
  };

  // 3. 遍历执行测试
  for (size_t i = 0; i < tests.size(); ++i) {
    const auto& tt = tests[i];

    auto search_result = lepton::leaf_to_expected([&]() { return search_index(tt.names, tt.index); });
    if (tt.wok) {
      ASSERT_TRUE(search_result);
      ASSERT_EQ(*search_result, tt.widx);
    } else {
      ASSERT_FALSE(search_result);
    }
  }
}

TEST_F(wal_test_suit, test_scan_wal_name) {
  struct test_case {
    std::string str;
    uint64_t wseq;
    uint64_t windex;
    bool wok;
  };
  std::vector<test_case> tests = {
      {"0000000000000000-0000000000000000.wal", 0, 0, true},
      {"0000000000000000.wal", 0, 0, false},
      {"0000000000000000-0000000000000000.snap", 0, 0, false},
      {"0000000000000001-0000000000001000.wal", 1, 0x1000, true},  // 额外增加一个 case 验证十六进制
  };

  for (size_t i = 0; i < tests.size(); ++i) {
    const auto& tt = tests[i];
    auto parse_result = lepton::leaf_to_expected([&]() { return parse_wal_name(tt.str); });
    if (tt.wok) {
      auto [seq, index] = *parse_result;
      ASSERT_TRUE(parse_result);
      ASSERT_EQ(tt.wseq, seq);
      ASSERT_EQ(tt.windex, index);
    } else {
      ASSERT_FALSE(parse_result);
    }
  }
}

TEST_F(wal_test_suit, test_recover_after_cut) {
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::io_context io_context;
  auto wal_root = wal_dir_path;
  EXPECT_TRUE(std::filesystem::create_directory(wal_root));
  auto wal_test_dir = join_paths(wal_root, "wal_test");
  EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_test_dir, "metadata",
                                              SEGMENT_SIZE_BYTES, std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_result);
        auto wal_handle = wal_result.value();
        for (std::size_t i = 0; i < 10; ++i) {
          lepton::storage::pb::snapshot snap;
          snap.set_index(i);
          snap.set_term(1);
          snap.mutable_conf_state()->CopyFrom(create_conf_state());
          AWAIT_EXPECT_OK(wal_handle->save_snapshot(snap));

          lepton::core::pb::repeated_entry entries;
          auto entry = entries.Add();
          entry->set_index(i);
          AWAIT_EXPECT_OK(wal_handle->save(entries, raftpb::HardState{}));
          AWAIT_EXPECT_OK(wal_handle->cut());
        }
        co_await wal_handle->close();

        const auto wal_index4_file = join_paths(wal_test_dir, wal_file_name(4, 4));
        EXPECT_TRUE(fileutil::path_exist(wal_index4_file));
        remove_all(wal_index4_file);
        EXPECT_FALSE(fileutil::path_exist(wal_index4_file));

        for (std::size_t i = 0; i < 10; ++i) {
          lepton::storage::pb::snapshot snap;
          snap.set_index(i);
          snap.set_term(1);
          auto new_wal_handle_result = lepton::leaf_to_expected([&]() {
            return open(env, io_context.get_executor(), wal_test_dir, SEGMENT_SIZE_BYTES, std::move(snap),
                        std::make_shared<lepton::spdlog_logger>());
          });
          if (!new_wal_handle_result) {
            if (i <= 4) {
              EXPECT_EQ(new_wal_handle_result.error(),
                        lepton::make_error_code(lepton::wal_error::ERR_INCONTINUOUS_SEQUENCE));
            } else {
              EXPECT_TRUE(false) << fmt::format("failed index: {}", i);
            }
            continue;
          }
          EXPECT_TRUE(new_wal_handle_result);
          auto& new_wal_handle = new_wal_handle_result.value();
          auto read_all_result = co_await new_wal_handle->read_all();
          EXPECT_TRUE(read_all_result) << fmt::format("failed index: {}; ", i) << read_all_result.error().message();
          EXPECT_EQ(std::error_code{}, read_all_result->ec);
          EXPECT_EQ(read_all_result->metadata, "metadata");
          for (auto [j, entry] : read_all_result->entries | std::views::enumerate) {
            EXPECT_EQ(entry.index(), i + static_cast<std::uint64_t>(j) + 1);
          }
          co_await new_wal_handle->close();
        }
        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(wal_test_suit, test_open_at_uncommitted_index) {
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::io_context io_context;
  auto wal_root = wal_dir_path;
  EXPECT_TRUE(std::filesystem::create_directory(wal_root));
  auto wal_test_dir = join_paths(wal_root, "wal_test");
  EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_test_dir, "", SEGMENT_SIZE_BYTES,
                                              std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_result);
        auto wal_handle = wal_result.value();
        AWAIT_EXPECT_OK(wal_handle->save_snapshot(lepton::storage::pb::snapshot{}));
        lepton::core::pb::repeated_entry entries;
        auto entry = entries.Add();
        entry->set_index(0);
        AWAIT_EXPECT_OK(wal_handle->save(entries, raftpb::HardState{}));
        co_await wal_handle->close();

        auto new_wal_handle_result = lepton::leaf_to_expected([&]() {
          return open(env, io_context.get_executor(), wal_test_dir, SEGMENT_SIZE_BYTES, lepton::storage::pb::snapshot{},
                      std::make_shared<lepton::spdlog_logger>());
        });
        EXPECT_TRUE(new_wal_handle_result);
        auto& new_wal_handle = new_wal_handle_result.value();
        // commit up to index 0, try to read index 1
        auto read_all_result = co_await new_wal_handle->read_all();
        EXPECT_TRUE(read_all_result) << read_all_result.error().message();
        EXPECT_EQ(std::error_code{}, read_all_result->ec);
        co_await new_wal_handle->close();
        co_return;
      },
      asio::use_future);
  io_context.run();
}

// TestOpenForRead tests that OpenForRead can load all files.
// The tests creates WAL directory, and cut out multiple WAL files. Then
// it releases the lock of part of data, and excepts that OpenForRead
// can read out all files even if some are locked for write.
TEST_F(wal_test_suit, test_open_for_read) {
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::io_context io_context;
  auto wal_root = wal_dir_path;
  EXPECT_TRUE(std::filesystem::create_directory(wal_root));
  auto wal_test_dir = join_paths(wal_root, "wal_test");
  EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_test_dir, "", SEGMENT_SIZE_BYTES,
                                              std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_result);
        auto wal_handle = wal_result.value();

        for (std::size_t i = 0; i < 5; ++i) {
          lepton::core::pb::repeated_entry entries;
          auto entry = entries.Add();
          entry->set_index(i);
          AWAIT_EXPECT_OK(wal_handle->save(entries, raftpb::HardState{}));
          AWAIT_EXPECT_OK(wal_handle->cut());
        }
        // release the lock to 5
        AWAIT_EXPECT_OK(wal_handle->release_lock_to(5));
        // All are available for read
        auto wal_result2 = open_for_read(env, io_context.get_executor(), wal_test_dir, SEGMENT_SIZE_BYTES,
                                         lepton::storage::pb::snapshot{}, std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_result2);
        auto wal_handle2 = wal_result2.value();
        auto read_all_result = co_await wal_handle2->read_all();
        EXPECT_TRUE(read_all_result);
        EXPECT_EQ(std::error_code{}, read_all_result->ec);
        EXPECT_FALSE(read_all_result->entries.empty());
        EXPECT_NE(9, read_all_result->entries.at(read_all_result->entries.size() - 1).index());

        co_await wal_handle2->close();
        co_await wal_handle->close();
        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(wal_test_suit, test_open_with_max_index) {
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::io_context io_context;

  SECTION("uint64_t_max_index") {
    auto wal_root = wal_dir_path;
    EXPECT_TRUE(std::filesystem::create_directory(wal_root));
    auto wal_test_dir = join_paths(wal_root, "wal_test");
    EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));

    asio::co_spawn(
        io_context,
        [&]() -> asio::awaitable<void> {
          auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_test_dir, "", SEGMENT_SIZE_BYTES,
                                                std::make_shared<lepton::spdlog_logger>());
          EXPECT_TRUE(wal_result);
          auto wal_handle = wal_result.value();

          lepton::core::pb::repeated_entry entries;
          auto entry = entries.Add();
          entry->set_index(std::numeric_limits<uint64_t>::max());
          AWAIT_EXPECT_OK(wal_handle->save(entries, raftpb::HardState{}));
          co_await wal_handle->close();

          // release the lock to 5
          AWAIT_EXPECT_OK(wal_handle->release_lock_to(5));
          // All are available for read
          auto wal_result2 = open(env, io_context.get_executor(), wal_test_dir, SEGMENT_SIZE_BYTES,
                                  lepton::storage::pb::snapshot{}, std::make_shared<lepton::spdlog_logger>());
          EXPECT_TRUE(wal_result2);
          auto wal_handle2 = wal_result2.value();
          auto read_all_result = co_await wal_handle2->read_all();
          EXPECT_FALSE(read_all_result);
          EXPECT_EQ(lepton::make_error_code(lepton::wal_error::ERR_SLICE_OUT_OF_RANGE), read_all_result.error());
          co_await wal_handle2->close();
          co_return;
        },
        asio::use_future);
    io_context.run();
  }

  SECTION("bigger_than_entries_index") {
    remove_all(wal_dir_path);
    auto wal_root = wal_dir_path;
    EXPECT_TRUE(std::filesystem::create_directory(wal_root));
    auto wal_test_dir = join_paths(wal_root, "wal_test");
    EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));

    asio::co_spawn(
        io_context,
        [&]() -> asio::awaitable<void> {
          auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_test_dir, "", SEGMENT_SIZE_BYTES,
                                                std::make_shared<lepton::spdlog_logger>());
          EXPECT_TRUE(wal_result);
          auto wal_handle = wal_result.value();

          lepton::core::pb::repeated_entry entries;
          auto entry = entries.Add();
          // 只要大于当前 entry size 的 index 即可
          entry->set_index(10);
          AWAIT_EXPECT_OK(wal_handle->save(entries, raftpb::HardState{}));
          co_await wal_handle->close();

          // release the lock to 5
          AWAIT_EXPECT_OK(wal_handle->release_lock_to(5));
          // All are available for read
          auto wal_result2 = open(env, io_context.get_executor(), wal_test_dir, SEGMENT_SIZE_BYTES,
                                  lepton::storage::pb::snapshot{}, std::make_shared<lepton::spdlog_logger>());
          EXPECT_TRUE(wal_result2);
          auto wal_handle2 = wal_result2.value();
          auto read_all_result = co_await wal_handle2->read_all();
          EXPECT_FALSE(read_all_result);
          EXPECT_EQ(lepton::make_error_code(lepton::wal_error::ERR_SLICE_OUT_OF_RANGE), read_all_result.error());
          co_await wal_handle2->close();
          co_return;
        },
        asio::use_future);
    io_context.run();
  }
}

TEST_F(wal_test_suit, test_save_empty) {
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::io_context io_context;
  auto wal_root = wal_dir_path;
  EXPECT_TRUE(std::filesystem::create_directory(wal_root));
  auto wal_test_dir = join_paths(wal_root, "wal_test");
  EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));

  auto wal_handle = std::make_shared<lepton::storage::wal::wal>(io_context.get_executor(), env, SEGMENT_SIZE_BYTES,
                                                                std::make_shared<lepton::spdlog_logger>());
  write_buf wb;
  pro::proxy_view<ioutil::writer> w = &wb;
  wal_handle->encoder_ = std::make_unique<encoder>(wal_handle->executor_, w, 0, 0, wal_handle->logger_);
  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        co_await wal_handle->save_state(raftpb::HardState{});
        EXPECT_TRUE(wb.buf.empty());
        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(wal_test_suit, test_release_lock_to) {
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::io_context io_context;
  auto wal_root = wal_dir_path;
  EXPECT_TRUE(std::filesystem::create_directory(wal_root));
  auto wal_test_dir = join_paths(wal_root, "wal_test");
  EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_test_dir, "", SEGMENT_SIZE_BYTES,
                                              std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_result);
        auto wal_handle = wal_result.value();

        // release nothing if no files
        AWAIT_EXPECT_OK(wal_handle->release_lock_to(10));

        for (std::size_t i = 0; i < 10; ++i) {
          lepton::core::pb::repeated_entry entries;
          auto entry = entries.Add();
          entry->set_index(i);
          AWAIT_EXPECT_OK(wal_handle->save(entries, raftpb::HardState{}));
          AWAIT_EXPECT_OK(wal_handle->cut());
        }
        // release the lock to 5
        AWAIT_EXPECT_OK(wal_handle->release_lock_to(5));
        // expected remaining are 4,5,6,7,8,9,10
        EXPECT_EQ(7, wal_handle->segments_.size());
        for (auto [i, file_handle] : wal_handle->segments_ | std::views::enumerate) {
          auto result = std::visit(
              [](auto& handle) {
                return lepton::leaf_to_expected([&]() { return parse_wal_name(fileutil::base_name(handle->name())); });
              },
              file_handle);
          EXPECT_TRUE(result);
          auto [_, lock_index] = *result;
          EXPECT_EQ(i + 4, lock_index);
        }

        // release the lock to 15
        AWAIT_EXPECT_OK(wal_handle->release_lock_to(15));
        // expected remaining is 10
        EXPECT_EQ(1, wal_handle->segments_.size());
        auto result = std::visit(
            [](auto& handle) {
              return lepton::leaf_to_expected([&]() { return parse_wal_name(fileutil::base_name(handle->name())); });
            },
            wal_handle->segments_[0]);
        EXPECT_TRUE(result) << result.error().message();
        auto [_, lock_index] = *result;
        EXPECT_EQ(10, lock_index);

        co_await wal_handle->close();
        co_return;
      },
      asio::use_future);
  io_context.run();
}

// TestTailWriteNoSlackSpace ensures that tail writes append if there's no preallocated space.
TEST_F(wal_test_suit, test_tail_write_no_slack_space) {
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::io_context io_context;
  auto wal_root = wal_dir_path;
  EXPECT_TRUE(std::filesystem::create_directory(wal_root));
  auto wal_test_dir = join_paths(wal_root, "wal_test");
  EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_test_dir, "metadata",
                                              SEGMENT_SIZE_BYTES, std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_result);
        auto wal_handle = wal_result.value();

        for (std::size_t i = 0; i <= 5; ++i) {
          lepton::core::pb::repeated_entry entries;
          auto entry = entries.Add();
          entry->set_index(i);
          entry->set_term(1);
          entry->set_data(fmt::format("{}", i));
          AWAIT_EXPECT_OK(wal_handle->save(entries, raftpb::HardState{}));
        }
        // get rid of slack space by truncating file
        auto seek_result = lepton::leaf_to_expected([&]() { return wal_handle->tail()->seek_curr(); });
        EXPECT_TRUE(seek_result);
        auto offset = *seek_result;
        EXPECT_NE(offset, 0);
        auto trunc_result = lepton::leaf_to_expected(
            [&]() { return wal_handle->tail()->truncate(static_cast<std::uintmax_t>(offset)); });
        EXPECT_TRUE(trunc_result);
        co_await wal_handle->close();

        {
          auto wal_result2 = open(env, io_context.get_executor(), wal_test_dir, SEGMENT_SIZE_BYTES,
                                  lepton::storage::pb::snapshot{}, std::make_shared<lepton::spdlog_logger>());
          EXPECT_TRUE(wal_result2);
          auto wal_handle2 = wal_result2.value();
          auto read_all_result = co_await wal_handle2->read_all();
          EXPECT_TRUE(read_all_result);
          EXPECT_EQ(5, read_all_result->entries.size());
          for (std::size_t i = 6; i <= 10; ++i) {
            lepton::core::pb::repeated_entry entries;
            auto entry = entries.Add();
            entry->set_index(i);
            entry->set_term(1);
            entry->set_data(fmt::format("{}", i));
            AWAIT_EXPECT_OK(wal_handle->save(entries, raftpb::HardState{}));
          }
          co_await wal_handle2->close();
        }

        {
          auto wal_result2 = open(env, io_context.get_executor(), wal_test_dir, SEGMENT_SIZE_BYTES,
                                  lepton::storage::pb::snapshot{}, std::make_shared<lepton::spdlog_logger>());
          EXPECT_TRUE(wal_result2);
          auto wal_handle2 = wal_result2.value();
          auto read_all_result = co_await wal_handle2->read_all();
          EXPECT_TRUE(read_all_result);
          EXPECT_EQ(10, read_all_result->entries.size());
          co_await wal_handle2->close();
        }

        co_return;
      },
      asio::use_future);
  io_context.run();
}

// TestRestartCreateWal ensures that an interrupted WAL initialization is clobbered on restart
TEST_F(wal_test_suit, test_restart_create_wal) {
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::io_context io_context;
  auto wal_root = wal_dir_path;
  EXPECT_TRUE(std::filesystem::create_directory(wal_root));
  auto wal_test_dir = join_paths(wal_root, "wal_test");
  // make temporary directory so it looks like initialization is interrupted
  auto wal_tmp_dir = join_paths(wal_root, "wal_test.tmp");
  EXPECT_TRUE(std::filesystem::create_directory(wal_tmp_dir));
  EXPECT_TRUE(fileutil::path_exist(wal_tmp_dir));

  create_with_fstream(join_paths(wal_tmp_dir, "test"));
  EXPECT_TRUE(fileutil::path_exist(join_paths(wal_tmp_dir, "test")));

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_test_dir, "abc", SEGMENT_SIZE_BYTES,
                                              std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_result);
        auto wal_handle = wal_result.value();
        co_await wal_handle->close();
        EXPECT_FALSE(fileutil::path_exist(wal_tmp_dir));

        auto wal_result2 = open_for_read(env, io_context.get_executor(), wal_test_dir, SEGMENT_SIZE_BYTES,
                                         lepton::storage::pb::snapshot{}, std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_result2);
        auto wal_handle2 = wal_result2.value();
        auto read_all_result = co_await wal_handle2->read_all();
        EXPECT_TRUE(read_all_result);
        EXPECT_EQ(std::error_code{}, read_all_result->ec);
        EXPECT_EQ("abc", read_all_result->metadata);
        co_await wal_handle2->close();

        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(wal_test_suit, test_open_on_torn_write) {
  constexpr auto MAX_ENTRIES = 40;
  constexpr auto CLOBBER_IDX = 20;
  constexpr auto OVERWRITE_ENTRIES = 5;
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::io_context io_context;
  auto wal_root = wal_dir_path;
  EXPECT_TRUE(std::filesystem::create_directory(wal_root));
  auto wal_test_dir = join_paths(wal_root, "wal_test");
  EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_test_dir, "", SEGMENT_SIZE_BYTES,
                                              std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_result);
        auto wal_handle = wal_result.value();

        std::vector<std::int64_t> offsets(MAX_ENTRIES, 0);
        for (auto i = 0uz; i < offsets.size(); ++i) {
          lepton::core::pb::repeated_entry entries;
          auto entry = entries.Add();
          entry->set_index(i);
          AWAIT_EXPECT_OK(wal_handle->save(entries, raftpb::HardState{}));
          auto seek_result = lepton::leaf_to_expected([&]() { return wal_handle->tail()->seek_curr(); });
          EXPECT_TRUE(seek_result);
          offsets[i] = *seek_result;
        }
        auto fn = fileutil::join_paths(wal_test_dir, fileutil::base_name(wal_handle->tail()->name()));
        AWAIT_EXPECT_OK(wal_handle->close());

        // clobber some entry with 0's to simulate a torn write
        auto rw_file_result = create_file_endpoint(io_context.get_executor(), fn, asio::file_base::read_write);
        EXPECT_TRUE(rw_file_result);
        auto& rw_file = rw_file_result.value();
        EXPECT_TRUE(rw_file.seek_start(offsets[CLOBBER_IDX]));
        std::string zeros(static_cast<std::size_t>(offsets[CLOBBER_IDX + 1] - offsets[CLOBBER_IDX]), '\0');
        rw_file.write(ioutil::to_bytes(zeros));
        EXPECT_TRUE(rw_file.close());

        auto open_wal_handle_result = open(env, io_context.get_executor(), wal_test_dir, SEGMENT_SIZE_BYTES,
                                           lepton::storage::pb::snapshot{}, std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(open_wal_handle_result);
        auto& open_wal_handle = open_wal_handle_result.value();
        // seek up to clobbered entry
        auto read_all_result = co_await open_wal_handle->read_all();
        EXPECT_TRUE(read_all_result);
        EXPECT_EQ(std::error_code{}, read_all_result->ec);
        EXPECT_EQ(read_all_result->entries.size(), CLOBBER_IDX);
        // write a few entries past the clobbered entry
        for (auto i = 0uz; i < OVERWRITE_ENTRIES; ++i) {
          lepton::core::pb::repeated_entry entries;
          auto entry = entries.Add();
          entry->set_index(i + CLOBBER_IDX);
          entry->set_data("new");
          // Index is different from old, truncated entries
          AWAIT_EXPECT_OK(open_wal_handle->save(entries, raftpb::HardState{}));
        }
        AWAIT_EXPECT_OK(open_wal_handle->close());

        // read back the entries, confirm number of entries matches expectation
        auto wal_result2 = open_for_read(env, io_context.get_executor(), wal_test_dir, SEGMENT_SIZE_BYTES,
                                         lepton::storage::pb::snapshot{}, std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_result2);
        auto wal_handle2 = wal_result2.value();
        auto read_all_result2 = co_await wal_handle2->read_all();
        EXPECT_TRUE(read_all_result2);
        EXPECT_EQ(std::error_code{}, read_all_result2->ec);
        EXPECT_EQ(read_all_result2->entries.size(), (CLOBBER_IDX - 1) + OVERWRITE_ENTRIES);
        AWAIT_EXPECT_OK(wal_handle2->close());
        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(wal_test_suit, test_rename_fail) {
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::io_context io_context;
  auto wal_root = wal_dir_path;
  EXPECT_TRUE(std::filesystem::create_directory(wal_root));
  auto wal_test_dir = join_paths(wal_root, "wal_test");
  EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));

  auto nw_handle = std::make_shared<lepton::storage::wal::wal>(io_context.get_executor(), env,
                                                               std::numeric_limits<std::size_t>::max(),
                                                               std::make_shared<lepton::spdlog_logger>());
  auto& nw = *nw_handle;
  nw.dir_ = wal_test_dir;
  auto result = nw.rename_wal(join_paths(wal_test_dir, "tmp"));
  ASSERT_FALSE(result);
}

TEST_F(wal_test_suit, test_valid_snapshot_entries) {
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::io_context io_context;
  auto wal_root = wal_dir_path;
  EXPECT_TRUE(std::filesystem::create_directory(wal_root));
  auto wal_test_dir = join_paths(wal_root, "wal_test");
  EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));
  lepton::storage::pb::repeated_snapshot snaps;

  // 辅助函数：快速填充 Snapshot
  auto fill_snap = [&](lepton::storage::pb::snapshot* s, uint64_t index, uint64_t term) {
    s->set_index(index);
    s->set_term(term);
    s->mutable_conf_state()->CopyFrom(create_conf_state());
  };

  // snap0: 空 Snapshot (Go: walpb.Snapshot{})
  auto snap0 = snaps.Add();

  // snap1: Index 1, Term 1 (Go: Index: 1, Term: 1)
  auto snap1 = snaps.Add();
  fill_snap(snap1, 1, 1);

  // state1: Commit 1, Term 1
  raftpb::HardState state1;
  state1.set_commit(1);
  state1.set_term(1);

  // snap2: Index 2, Term 1
  auto snap2 = snaps.Add();
  fill_snap(snap2, 2, 1);

  // snap3: Index 3, Term 2
  auto snap3 = snaps.Add();
  fill_snap(snap3, 3, 2);

  // state2: Commit 3, Term 2
  raftpb::HardState state2;
  state2.set_commit(3);
  state2.set_term(2);

  // will be orphaned since the last committed entry will be snap3
  // snap4: Index 4, Term 2 (将被视为孤儿快照，因为 commit 只到了 3)
  lepton::storage::pb::snapshot snap4;
  fill_snap(&snap4, 4, 2);

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_test_dir, "metadata",
                                              SEGMENT_SIZE_BYTES, std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_result);
        auto wal_handle = wal_result.value();

        AWAIT_EXPECT_OK(wal_handle->save_snapshot(*snap1));
        AWAIT_EXPECT_OK(wal_handle->save(lepton::core::pb::repeated_entry{}, raftpb::HardState{state1}));
        AWAIT_EXPECT_OK(wal_handle->save_snapshot(*snap2));
        AWAIT_EXPECT_OK(wal_handle->save_snapshot(*snap3));
        AWAIT_EXPECT_OK(wal_handle->save(lepton::core::pb::repeated_entry{}, raftpb::HardState{state2}));
        AWAIT_EXPECT_OK(wal_handle->save_snapshot(snap4));
        auto wal_snaps_result = co_await valid_snapshot_entries(io_context.get_executor(), wal_test_dir,
                                                                std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_snaps_result);
        auto& wal_snaps = wal_snaps_result.value();
        EXPECT_TRUE(compare_repeated_snap_meta(wal_snaps, snaps));
        co_await wal_handle->close();
        co_return;
      },
      asio::use_future);
  io_context.run();
}

// TestValidSnapshotEntriesAfterPurgeWal ensure that there are many wal files, and after cleaning the first wal file,
// it can work well.
TEST_F(wal_test_suit, test_valid_snapshot_entries_after_purge_wal) {
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::io_context io_context;
  auto wal_root = wal_dir_path;
  EXPECT_TRUE(std::filesystem::create_directory(wal_root));
  auto wal_test_dir = join_paths(wal_root, "wal_test");
  EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));
  lepton::storage::pb::repeated_snapshot snaps;

  // 辅助函数：快速填充 Snapshot
  auto fill_snap = [&](lepton::storage::pb::snapshot* s, uint64_t index, uint64_t term) {
    s->set_index(index);
    s->set_term(term);
    s->mutable_conf_state()->CopyFrom(create_conf_state());
  };

  // snap0: 空 Snapshot (Go: walpb.Snapshot{})
  auto snap0 = snaps.Add();

  // snap1: Index 1, Term 1 (Go: Index: 1, Term: 1)
  auto snap1 = snaps.Add();
  fill_snap(snap1, 1, 1);

  // state1: Commit 1, Term 1
  raftpb::HardState state1;
  state1.set_commit(1);
  state1.set_term(1);

  // snap2: Index 2, Term 1
  auto snap2 = snaps.Add();
  fill_snap(snap2, 2, 1);

  // snap3: Index 3, Term 2
  auto snap3 = snaps.Add();
  fill_snap(snap3, 3, 2);

  // state2: Commit 3, Term 2
  raftpb::HardState state2;
  state2.set_commit(3);
  state2.set_term(2);

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_test_dir, "metadata", 64,
                                              std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_result);
        auto wal_handle = wal_result.value();
        // snap0 is implicitly created at index 0, term 0
        AWAIT_EXPECT_OK(wal_handle->save_snapshot(*snap1));
        AWAIT_EXPECT_OK(wal_handle->save(lepton::core::pb::repeated_entry{}, raftpb::HardState{state1}));
        AWAIT_EXPECT_OK(wal_handle->save_snapshot(*snap2));
        AWAIT_EXPECT_OK(wal_handle->save_snapshot(*snap3));
        for (int i = 0; i < 128; ++i) {
          AWAIT_EXPECT_OK(wal_handle->save(lepton::core::pb::repeated_entry{}, raftpb::HardState{state2}));
        }

        auto select_result = lepton::leaf_to_expected([&]() { return select_wal_files(wal_test_dir, 0); });
        EXPECT_TRUE(select_result);
        auto& [wal_names, start_index] = *select_result;
        std::string remove_file = wal_test_dir + "/" + wal_names[0];
        EXPECT_TRUE(path_exist(remove_file));
        EXPECT_TRUE(fileutil::remove(remove_file));
        auto wal_snaps_result = co_await valid_snapshot_entries(io_context.get_executor(), wal_test_dir,
                                                                std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(wal_snaps_result) << remove_file;

        co_await wal_handle->close();
        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(wal_test_suit, test_last_record_length_exceed_file_end) {
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::io_context io_context;
  auto wal_root = wal_dir_path;
  EXPECT_TRUE(std::filesystem::create_directory(wal_root));
  auto wal_test_dir = join_paths(wal_root, "wal_test");
  EXPECT_TRUE(std::filesystem::create_directory(wal_test_dir));

  std::string data =
      "\x04\x00\x00\x00\x00\x00\x00\x84\x08\x04\x10\x00\x00"
      "\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x84\x08\x01\x10\x00\x00"
      "\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x82\x08\x05\x10\xa0\xb3"
      "\x9b\x8f\x08\x1a\x04\x08\x00\x10\x00\x00\x00\x1a\x00\x00\x00\x00"
      "\x00\x00\x86\x08\x02\x10\xba\x8b\xdc\x85\x0f\x1a\x10\x08\x00\x10"
      "\x00\x18\x01\x22\x08\x77\x61\x6c\x64\x61\x74\x61\x31\x00\x00\x00"
      "\x00\x00\x00\x1a\x00\x00\x00\x00\x00\x00\x86\x08\x02\x10\xa1\xe8"
      "\xff\x9c\x02\x1a\x10\x08\x00\x10\x00\x18\x02\x22\x08\x77\x61\x6c"
      "\x64\x61\x74\x61\x32\x00\x00\x00\x00\x00\x00\xe8\x03\x00\x00\x00"
      "\x00\x00\x86\x08\x02\x10\xa1\x9c\xa1\xaa\x04\x1a\x10\x08\x00\x10"
      "\x00\x18\x03\x22\x08\x77\x61\x6c\x64\x61\x74\x61\x33\x00\x00\x00"
      "\x00\x00\x00"s;
  auto write_file_path = join_paths(wal_test_dir, "wal3306");
  LOG_INFO("corrupted_wal_data size: {}", data.size());
  EXPECT_TRUE(write_file(io_context.get_executor(), write_file_path, ioutil::to_bytes(data)));
  std::vector<wal_segment> files;
  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto reader_result = fileutil::create_file_reader(io_context.get_executor(), write_file_path);
        EXPECT_TRUE(reader_result);
        std::vector<wal_segment> segments;
        segments.push_back(std::move(*reader_result));
        auto logger = std::make_shared<lepton::spdlog_logger>();
        auto decoder = std::make_unique<lepton::storage::wal::decoder>(io_context.get_executor(), logger,
                                                                       to_reader_views(segments));
        walpb::Record rec;
        std::error_code ec;
        raftpb::HardState state;
        auto parse_count = 0uz;
        while (!ec) {
          auto result = co_await decoder->decode_record(rec);
          if (!result) {
            ec = result.error();
            EXPECT_EQ(lepton::make_error_code(lepton::io_error::UNEXPECTED_EOF), ec);
            continue;
          }

          switch (rec.type()) {
            case walpb::ENTRY_TYPE: {
              parse_count++;
              raftpb::Entry entry;
              lepton::protobuf_must_parse(rec.data(), entry, *logger);
              auto rec_data = fmt::format("waldata{}", entry.index());
              EXPECT_EQ(raftpb::EntryType::ENTRY_NORMAL, entry.type());
              EXPECT_EQ(rec_data, entry.data());
              break;
            }
            default: {
              break;
            }
          }

          rec.Clear();
        }
        EXPECT_EQ(parse_count, 2);

        fs::path p(write_file_path);
        auto new_file_name = fileutil::join_paths(fs::path{write_file_path}.parent_path().string(),
                                                  "0000000000000000-0000000000000000.wal");
        EXPECT_TRUE(fileutil::rename(write_file_path, new_file_name));

        lepton::storage::pb::snapshot snap;
        snap.set_term(0);
        snap.set_term(0);
        auto open_wal_handle_result =
            open(env, io_context.get_executor(), fs::path{write_file_path}.parent_path().string(), SEGMENT_SIZE_BYTES,
                 std::move(snap), std::make_shared<lepton::spdlog_logger>());
        EXPECT_TRUE(open_wal_handle_result);
        auto& open_wal_handle = open_wal_handle_result.value();
        auto read_all_result = co_await open_wal_handle->read_all();
        EXPECT_FALSE(read_all_result);
        EXPECT_EQ(lepton::make_error_code(lepton::io_error::UNEXPECTED_EOF), read_all_result.error());
        AWAIT_EXPECT_OK(open_wal_handle->close());
        co_return;
      },
      asio::use_future);
  io_context.run();
}