#include "wal_file.h"

#include "asio/error_code.hpp"
#include "preallocate.h"
namespace lepton::storage::wal {

leaf::result<std::size_t> wal_file::size() const {
  std::size_t file_size = 0;
  if (auto s = env_->GetFileSize(file_name_, &file_size); !s.ok()) {
    return new_error(s, fmt::format("Failed to get size of WAL file {}: {}", file_name_, s.ToString()));
  }
  return file_size;
}

leaf::result<void> wal_file::seek_to_end() {
  asio::error_code ec;
  file_.seek(0, asio::file_base::seek_end, ec);
  if (ec) {
    return new_error(ec, fmt::format("Failed to seek to end of WAL file: {}", ec.message()));
  }
  return {};
}

leaf::result<void> wal_file::pre_allocate(uint64_t length) {
  if (auto ec = fileutil::preallocate(file_.native_handle(), length); ec) {
    return new_error(ec);
  }
  return {};
}

leaf::result<std::size_t> wal_file::read(asio::mutable_buffer buffer) {
  std::error_code ec;
  auto read_size = file_.read_some(asio::buffer(buffer.data(), buffer.size()), ec);
  if (ec) {
    return new_error(ec, fmt::format("Failed to read from WAL file: {}", ec.message()));
  }
  return read_size;
}

asio::awaitable<expected<std::size_t>> wal_file::async_read(asio::mutable_buffer buffer) {
  std::error_code ec;
  auto read_size = co_await file_.async_read_some(buffer, asio::redirect_error(asio::use_awaitable, ec));
  if (ec) {
    co_return tl::unexpected(ec);
  }
  co_return read_size;
}

leaf::result<std::size_t> wal_file::write(ioutil::byte_span data) {
  std::error_code ec;
  auto write_size = file_.write_some(asio::buffer(data.data(), data.size()), ec);
  if (ec) {
    return new_error(ec, fmt::format("Failed to write to WAL file: {}", ec.message()));
  }
  return write_size;
}

asio::awaitable<expected<std::size_t>> wal_file::async_write(ioutil::byte_span data) {
  std::error_code ec;
  auto write_size = co_await file_.async_write_some(asio::buffer(data.data(), data.size()),
                                                    asio::redirect_error(asio::use_awaitable, ec));
  if (ec) {
    co_return tl::unexpected(ec);
  }
  co_return write_size;
}

}  // namespace lepton::storage::wal
