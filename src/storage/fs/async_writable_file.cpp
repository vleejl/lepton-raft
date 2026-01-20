#include "storage/fs/async_writable_file.h"

#include "error/error.h"
#include "error/leaf.h"
#include "error/rocksdb_err.h"
#include "storage/fs/co_env_blocking.h"
#include "tl/expected.hpp"

namespace lepton::storage::fs {

asio::awaitable<expected<std::size_t>> asio_writable_file::async_write(const char* data, std::size_t size) {
  auto r = co_await co_env_blocking(pool_, [this, data, size]() -> expected<std::size_t> {
    rocksdb::Slice slice(data, size);
    auto s = file_->Append(slice);
    if (!s.ok()) {
      return tl::unexpected(lepton::make_error_code(s));
    }
    // 更新 offset
    offset_ += size;
    return size;
  });
  co_return r;
}

leaf::result<void> asio_writable_file::truncate(std::uint64_t size) {
  auto s = file_->Truncate(size);
  if (!s.ok()) {
    return new_error(s);
  }

  // 调整 offset，如果 offset > size
  if (offset_ > size) {
    offset_ = size;
  }
  return {};
}

leaf::result<void> asio_writable_file::preallocate(std::uint64_t size, bool _) {
  auto s = file_->Allocate(0, size);
  if (!s.ok()) {
    return new_error(s);
  }

  return {};
}

expected<void> asio_writable_file::fdatasync() {
  auto s = file_->Fsync();
  if (!s.ok()) {
    return unexpected(s);
  }
  return {};
}
}  // namespace lepton::storage::fs
