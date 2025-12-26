#include "storage/fileutil/env_file_endpoint.h"

#include "asio/error_code.hpp"
#include "error/leaf.h"
#include "error/lepton_error.h"
#include "leaf.hpp"
#include "storage/fileutil/preallocate.h"
#include "tl/expected.hpp"
namespace lepton::storage::fileutil {

leaf::result<std::size_t> env_file_endpoint::size() const {
  std::size_t file_size = 0;
  if (auto s = env_->GetFileSize(file_name_, &file_size); !s.ok()) {
    return new_error(s, fmt::format("Failed to get size of WAL file {}: {}", file_name_, s.ToString()));
  }
  return file_size;
}

leaf::result<void> env_file_endpoint::seek_start(std::int64_t offset) {
  asio::error_code ec;
  auto _ = raw_file().seek(offset, asio::file_base::seek_set, ec);
  if (ec) {
    return new_error(ec, fmt::format("Failed to seek to end of WAL file: {}", ec.message()));
  }
  return {};
}

leaf::result<std::uint64_t> env_file_endpoint::seek_curr() {
  asio::error_code ec;
  auto offset = raw_file().seek(0, asio::file_base::seek_cur, ec);
  if (ec) {
    return new_error(ec, fmt::format("Failed to seek to end of WAL file: {}", ec.message()));
  }
  return offset;
}

leaf::result<std::uint64_t> env_file_endpoint::seek_end() {
  asio::error_code ec;
  auto offset = raw_file().seek(0, asio::file_base::seek_end, ec);
  if (ec) {
    return new_error(ec, fmt::format("Failed to seek to end of WAL file: {}", ec.message()));
  }
  return offset;
}

leaf::result<void> env_file_endpoint::pre_allocate(uint64_t length, bool extend_file) {
  if (auto ec = fileutil::preallocate(raw_file().native_handle(), static_cast<std::int64_t>(length), extend_file); ec) {
    return new_error(ec);
  }
  return {};
}

leaf::result<std::size_t> env_file_endpoint::read(asio::mutable_buffer buffer) {
  std::error_code ec;
  auto read_size = raw_file().read_some(asio::buffer(buffer.data(), buffer.size()), ec);
  if (ec) {
    return new_error(ec, fmt::format("Failed to read from WAL file: {}", ec.message()));
  }
  return read_size;
}

asio::awaitable<expected<std::size_t>> env_file_endpoint::async_read(asio::mutable_buffer buffer) {
  std::error_code ec;
  auto read_size = co_await raw_file().async_read_some(buffer, asio::redirect_error(asio::use_awaitable, ec));
  if (ec) {
    co_return tl::unexpected(ec);
  }
  co_return read_size;
}

leaf::result<std::size_t> env_file_endpoint::write(ioutil::byte_span data) {
  std::error_code ec;
  auto write_size = raw_file().write_some(asio::buffer(data.data(), data.size()), ec);
  if (ec) {
    return new_error(ec, fmt::format("Failed to write to WAL file: {}", ec.message()));
  }
  return write_size;
}

asio::awaitable<expected<std::size_t>> env_file_endpoint::async_write(ioutil::byte_span data) {
  std::error_code ec;
  auto write_size = co_await raw_file().async_write_some(asio::buffer(data.data(), data.size()),
                                                         asio::redirect_error(asio::use_awaitable, ec));
  if (ec) {
    co_return tl::unexpected(ec);
  }
  co_return write_size;
}

asio::awaitable<expected<std::size_t>> env_file_endpoint::async_write_vectored_asio(
    std::span<const std::span<const std::byte>> spans) {
  std::vector<asio::const_buffer> buffers;
  buffers.reserve(spans.size());
  for (auto s : spans) {
    if (s.size() == 0) continue;
    buffers.emplace_back(static_cast<const void*>(s.data()), s.size());
  }
  if (buffers.empty()) co_return std::size_t{0};

  std::size_t bytes_transferred = co_await raw_file().async_write_some(buffers, asio::use_awaitable);
  co_return bytes_transferred;
}

expected<void> env_file_endpoint::fdatasync() {
  std::error_code ec;
  ec = raw_file().sync_data(ec);
  return tl::unexpected(ec);
}

}  // namespace lepton::storage::fileutil
