#include "storage/ioutil/io.h"

#include "asio/error.hpp"
#include "proxy.h"
#include "storage/ioutil/reader.h"
#include "tl/expected.hpp"

namespace lepton::storage::ioutil {

asio::awaitable<expected<std::size_t>> read_at_least(pro::proxy_view<reader> r, asio::mutable_buffer buf,
                                                     std::size_t min) {
  if (buf.size() < min) {
    co_return tl::unexpected(io_error::SHORT_BUFFER);
  }
  std::error_code ec;
  std::size_t total_read = 0;
  while (total_read < min) {
    asio::mutable_buffer current_buf =
        asio::buffer(static_cast<std::byte*>(buf.data()) + total_read, buf.size() - total_read);
    auto read_result = co_await r->async_read(current_buf);
    if (!read_result) {
      ec = read_result.error();
      break;
    }
    auto read_size = read_result.value();
    total_read += read_size;
  }
  if (total_read >= min) {
    // Read at least min bytes successfully, ignore any error
    co_return total_read;
  } else if ((total_read > 0) && (ec == asio::error::eof)) {
    // unexpected EOF after reading fewer than min bytes
    co_return tl::unexpected(io_error::UNEXPECTED_EOF);
  }
  if (ec) {
    co_return tl::unexpected(ec);
  }
  co_return total_read;
}

asio::awaitable<expected<std::size_t>> read_full(pro::proxy_view<reader> r, asio::mutable_buffer buf) {
  co_return co_await read_at_least(r, buf, buf.size());
}
}  // namespace lepton::storage::ioutil
