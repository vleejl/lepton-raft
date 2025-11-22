#include "wal_file.h"

#include "asio/error_code.hpp"
#include "preallocate.h"
namespace lepton {

leaf::result<void> wal_file::seek_to_end() {
  asio::error_code ec;
  file_.seek(0, asio::file_base::seek_end, ec);
  if (ec) {
    return new_error(ec, fmt::format("Failed to seek to end of WAL file: {}", ec.message()));
  }
  return {};
}

leaf::result<void> wal_file::pre_allocate(uint64_t length) {
  if (auto ec = preallocate(file_.native_handle(), length); ec) {
    return new_error(ec);
  }
  return {};
}

leaf::result<std::size_t> wal_file::write(byte_span data) {
  std::error_code ec;
  auto write_size = file_.write_some(asio::buffer(data.data(), data.size()), ec);
  if (ec) {
    return new_error(ec, fmt::format("Failed to write to WAL file: {}", ec.message()));
  }
  return write_size;
}

asio::awaitable<expected<std::size_t>> wal_file::async_write(byte_span data) {
  std::error_code ec;
  auto write_size = co_await file_.async_write_some(asio::buffer(data.data(), data.size()),
                                                    asio::redirect_error(asio::use_awaitable, ec));
  if (ec) {
    co_return tl::unexpected(ec);
  }
  co_return write_size;
}

}  // namespace lepton
