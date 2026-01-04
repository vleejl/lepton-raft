#include "storage/fileutil/file_endpoint.h"

#include <cassert>
#include <system_error>

#include "basic/logger.h"
#include "error/leaf.h"
#include "error/lepton_error.h"
#include "storage/fileutil/path.h"
#include "storage/fileutil/preallocate.h"
#include "tl/expected.hpp"
namespace lepton::storage::fileutil {

leaf::result<void> file_endpoint::preallocate(std::uint64_t length, bool extend_file) {
  if (auto ec = fileutil::preallocate(raw_file().native_handle(), static_cast<std::int64_t>(length), extend_file); ec) {
    LOG_ERROR("pre allocate length: {} failed, error:{}", length, ec.message());
    return new_error(ec);
  }
  return {};
}

leaf::result<void> file_endpoint::truncate(std::uintmax_t size) {
  if (auto ec = fileutil::truncate(raw_file().native_handle(), file_name_, size); ec) {
    LOG_ERROR("resize file: {} failed, error: {}", file_name_, ec.message());
    return new_error(ec);
  }
  return {};
}

leaf::result<void> file_endpoint::zero_to_end() {
  BOOST_LEAF_AUTO(offset, seek_curr());
  BOOST_LEAF_AUTO(lenf, seek_end());
  assert(offset > 0);
  LEPTON_LEAF_CHECK(truncate(static_cast<std::uintmax_t>(offset)));
  // make sure blocks remain allocated
  LEPTON_LEAF_CHECK(preallocate(static_cast<std::uint64_t>(lenf), true));
  return seek_start(offset);
}

leaf::result<std::size_t> file_endpoint::write(ioutil::byte_span data) {
  std::error_code ec;
  auto write_size = raw_file().write_some(asio::buffer(data.data(), data.size()), ec);
  if (ec) {
    return new_error(ec, fmt::format("Failed to write to WAL file: {}", ec.message()));
  }
  return write_size;
}

asio::awaitable<expected<std::size_t>> file_endpoint::async_write(ioutil::byte_span data) {
  std::error_code ec;
  auto write_size = co_await raw_file().async_write_some(asio::buffer(data.data(), data.size()),
                                                         asio::redirect_error(asio::use_awaitable, ec));
  if (ec) {
    co_return tl::unexpected(ec);
  }
  co_return write_size;
}

asio::awaitable<expected<std::size_t>> file_endpoint::async_write_vectored_asio(
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

expected<void> file_endpoint::fdatasync() {
  std::error_code ec;
  ec = raw_file().sync_data(ec);
  if (!ec) {
    return {};
  }
  return tl::unexpected(ec);
}

}  // namespace lepton::storage::fileutil
