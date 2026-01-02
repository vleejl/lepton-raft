#include <fmt/core.h>
#include <fmt/ranges.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <memory>
#include <regex>
#include <string>
#include <system_error>
#include <vector>

#include "asio/error.hpp"
#include "asio/error_code.hpp"
#include "asio/use_future.hpp"
#include "basic/defer.h"
#include "basic/logger.h"
#include "basic/spdlog_logger.h"
#include "error/leaf_expected.h"
#include "leaf.hpp"
#include "storage/fileutil/path.h"
#include "storage/fileutil/read_dir.h"
#include "storage/ioutil/io.h"
#include "storage/ioutil/reader.h"
#include "storage/pb/types.h"
#include "storage/wal/encoder.h"
#include "storage/wal/wal.h"
#include "storage/wal/wal_file.h"
#include "wal.pb.h"
using namespace lepton::storage::fileutil;
using namespace lepton::storage::wal;
using namespace lepton::storage;
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

auto walpb_record(::walpb::record_type type, std::uint32_t crc, const std::string& data) {
  walpb::record record;
  record.set_type(type);
  if (type == ::walpb::record_type::CRC_TYPE) {
    record.set_crc(crc);
  }
  if (!data.empty()) {
    record.set_data(data);
  }
  return record;
}

lepton::leaf::result<std::unique_ptr<file_endpoint>> create_readonly_file(const std::string& filename,
                                                                          asio::any_io_executor executor) {
  asio::stream_file stream_file(executor);
  asio::file_base::flags open_flags = asio::random_access_file::read_only;

  asio::error_code ec;
  stream_file.open(filename, open_flags, ec);  // NOLINT(bugprone-unused-return-value)
  if (ec) {
    LOG_ERROR("Failed to create WAL file {}: {}", filename, ec.message());
    return lepton::new_error(ec, fmt::format("Failed to create WAL file {}: {}", filename, ec.message()));
  }
  return std::make_unique<file_endpoint>(filename, std::move(stream_file));
}

lepton::leaf::result<void> write_file(asio::any_io_executor executor, const std::string& filename,
                                      ioutil::byte_span data) {
  BOOST_LEAF_AUTO(file_handle, create_new_wal_file(executor, filename, true));
  BOOST_LEAF_AUTO(size, file_handle.write(data));
  EXPECT_EQ(size, data.size());
  return {};
}

lepton::leaf::result<std::string> read_file_content(asio::any_io_executor executor, const std::string& filename) {
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
bool compare_and_dump_diff(const std::vector<std::byte>& v1, const std::vector<std::byte>& v2,
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

void create_with_fstream(const std::string& name) {
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
        std::vector<std::byte> buff(seek_value);
        asio::mutable_buffer read_buffer = asio::buffer(buff);
        pro::proxy_view<ioutil::reader> r = read_file.get();
        auto read_full_result = co_await ioutil::read_full(r, read_buffer);
        EXPECT_TRUE(read_full_result) << tail_file_path;
        auto read_len_res = *read_full_result;
        EXPECT_NE(0, read_len_res) << tail_file_path;

        write_buf wb;
        pro::proxy_view<ioutil::writer> w = &wb;
        encoder encoder_handle{io_context.get_executor(), w, 0, 0, std::make_shared<lepton::spdlog_logger>()};

        walpb::record record = walpb_record(walpb::record_type::CRC_TYPE, 0, "");
        auto encode_result = co_await encoder_handle.encode(record);
        EXPECT_TRUE(encode_result);

        record = walpb_record(walpb::record_type::METADATA_TYPE, 0, "somedata");
        encode_result = co_await encoder_handle.encode(record);
        EXPECT_TRUE(encode_result);
        fmt::print("{:02x}\n", fmt::join(wb.buf, " "));

        record =
            walpb_record(walpb::record_type::SNAPSHOT_TYPE, 0, lepton::storage::pb::snapshot().SerializeAsString());
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
    EXPECT_TRUE(write_file(io_context.get_executor(), file_path, initial_data));

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
  ASSERT_TRUE(write_file(io_context.get_executor(), file_path, "data"));

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