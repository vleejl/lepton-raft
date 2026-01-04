#include "storage/fileutil/file_reader.h"

#include <cstdint>
#include <memory>

#include "asio/error_code.hpp"
#include "basic/logger.h"
#include "error/leaf.h"
#include "error/lepton_error.h"
#include "storage/fileutil/path.h"
#include "tl/expected.hpp"
namespace lepton::storage::fileutil {

leaf::result<std::size_t> file_reader::size() const { return fileutil::file_size(file_name_); }

leaf::result<void> file_reader::seek_start(std::int64_t offset) {
  asio::error_code ec;
  auto _ = raw_file().seek(offset, asio::file_base::seek_set, ec);
  if (ec) {
    return new_error(ec, fmt::format("Failed to seek to end of WAL file: {}", ec.message()));
  }
  return {};
}

leaf::result<std::int64_t> file_reader::seek_curr() {
  asio::error_code ec;
  auto offset = raw_file().seek(0, asio::file_base::seek_cur, ec);
  if (ec) {
    return new_error(ec, fmt::format("Failed to seek to end of WAL file: {}", ec.message()));
  }
  return offset;
}

leaf::result<std::int64_t> file_reader::seek_end() {
  asio::error_code ec;
  auto offset = raw_file().seek(0, asio::file_base::seek_end, ec);
  if (ec) {
    return new_error(ec, fmt::format("Failed to seek to end of WAL file: {}", ec.message()));
  }
  return offset;
}

leaf::result<std::size_t> file_reader::read(asio::mutable_buffer buffer) {
  std::error_code ec;
  auto read_size = raw_file().read_some(asio::buffer(buffer.data(), buffer.size()), ec);
  if (ec) {
    LOG_ERROR("failed to read file: {}, error:{}", file_name_, ec.message());
    return new_error(ec, fmt::format("Failed to read from file: {}", ec.message()));
  }
  return read_size;
}

asio::awaitable<expected<std::size_t>> file_reader::async_read(asio::mutable_buffer buffer) {
  std::error_code ec;
  auto read_size = co_await raw_file().async_read_some(buffer, asio::redirect_error(asio::use_awaitable, ec));
  if (ec) {
    co_return tl::unexpected(ec);
  }
  co_return read_size;
}

expected<void> file_reader::close() {
  asio::error_code ec;
  ec = raw_file().close(ec);
  if (!ec) {
    return {};
  }
  return tl::unexpected(ec);
}

leaf::result<file_reader_handle> create_file_reader(asio::any_io_executor executor, const std::string& filename) {
  asio::error_code ec;
  asio::stream_file file_stream(executor);
  asio::file_base::flags open_flags = asio::random_access_file::read_only;

  file_stream.open(filename, open_flags, ec);  // NOLINT(bugprone-unused-return-value)
  if (ec) {
    LOG_ERROR("Failed to create WAL file {}: {}", filename, ec.message());
    return lepton::new_error(ec, fmt::format("Failed to create WAL file {}: {}", filename, ec.message()));
  }
  return std::make_unique<file_reader>(filename, std::move(file_stream));
}
}  // namespace lepton::storage::fileutil
