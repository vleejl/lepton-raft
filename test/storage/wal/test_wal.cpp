#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "asio/use_future.hpp"
#include "basic/defer.h"
#include "basic/logger.h"
#include "basic/spdlog_logger.h"
#include "error/leaf_expected.h"
#include "storage/fileutil/env_file_endpoint.h"
#include "storage/fileutil/path.h"
#include "storage/ioutil/file_buf_reader.h"
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

lepton::leaf::result<std::unique_ptr<env_file_endpoint>> create_readonly_file(const std::string& filename,
                                                                              asio::any_io_executor executor) {
  asio::stream_file stream_file(executor);
  asio::file_base::flags open_flags = asio::random_access_file::read_only;

  asio::error_code ec;
  stream_file.open(filename, open_flags, ec);  // NOLINT(bugprone-unused-return-value)
  if (ec) {
    LOG_ERROR("Failed to create WAL file {}: {}", filename, ec.message());
    return lepton::new_error(ec, fmt::format("Failed to create WAL file {}: {}", filename, ec.message()));
  }
  return std::make_unique<env_file_endpoint>(filename, std::move(stream_file));
}

class wal_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    remove_all(wal_file_path);
    EXPECT_TRUE(std::filesystem::create_directory(wal_file_path));
    std::cout << "run before first case..." << std::endl;
  }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }

  constexpr static auto wal_file_path = "./wal_test_dir";
};

TEST_F(wal_test_suit, create_new_wal_file) {
  asio::io_context io_context;
  rocksdb::Env* env = rocksdb::Env::Default();
  std::filesystem::path wal_file_dir = wal_file_path;
  auto file_path = wal_file_dir / wal_file_name(0, 0);
  auto result = create_new_wal_file(io_context.get_executor(), env, file_path.string(), false);
  ASSERT_TRUE(result);
  io_context.run();
}

TEST_F(wal_test_suit, new_wal) {
  remove_all(wal_file_path);
  asio::io_context io_context;
  rocksdb::Env* env = rocksdb::Env::Default();
  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto wal_result = co_await create_wal(env, io_context.get_executor(), wal_file_path, "somedata",
                                              std::make_shared<lepton::spdlog_logger>());
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

        auto read_file_result = create_readonly_file(
            join_paths(wal_file_path, fileutil::base_name(tail_wal_file_handle->name())), io_context.get_executor());
        EXPECT_TRUE(read_file_result);
        auto& read_file = read_file_result.value();
        pro::proxy_view<ioutil::reader> r = read_file.get();
        std::vector<std::byte> buff;
        asio::mutable_buffer read_buffer = asio::buffer(buff);
        auto read_full_result = co_await ioutil::read_full(r, read_buffer);
        EXPECT_TRUE(read_full_result);

        write_buf wb;
        pro::proxy_view<ioutil::writer> w = &wb;
        encoder encoder_handle{w, 0, 0, std::make_shared<lepton::spdlog_logger>()};

        walpb::record record = walpb_record(walpb::record_type::CRC_TYPE, 0, "");
        auto encode_result = co_await encoder_handle.encode(record);
        EXPECT_TRUE(encode_result);

        record = walpb_record(walpb::record_type::METADATA_TYPE, 0, "somedata");
        encode_result = co_await encoder_handle.encode(record);
        EXPECT_TRUE(encode_result);
        record =
            walpb_record(walpb::record_type::SNAPSHOT_TYPE, 0, lepton::storage::pb::snapshot().SerializeAsString());
        encode_result = co_await encoder_handle.encode(record);
        EXPECT_TRUE(encode_result);

        auto flush_result = co_await encoder_handle.flush();
        EXPECT_TRUE(flush_result);

        // EXPECT_EQ(wb.buf, buff);
        co_await wal_handle->close();
        co_return;
      },
      asio::use_future);
  io_context.run();
}
