#pragma once
#ifndef _LEPTON_FILE_ENDPOINT_H_
#define _LEPTON_FILE_ENDPOINT_H_

#include <fmt/format.h>

#include <asio/stream_file.hpp>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>

#include "basic/utility_macros.h"
#include "error/expected.h"
#include "error/leaf.h"
#include "storage/fileutil/file_reader.h"
#include "storage/ioutil/byte_span.h"

namespace lepton::storage::fileutil {

class file_endpoint : public file_reader {
  NOT_COPYABLE(file_endpoint)

 public:
  file_endpoint() = default;

  file_endpoint(const std::string& filename, asio::stream_file&& file) : file_reader(filename, std::move(file)) {}

  file_endpoint(file_endpoint&& lhs) : file_reader(std::move(lhs)) {}

  leaf::result<void> preallocate(std::uint64_t length, bool extend_file);

  leaf::result<void> truncate(std::uintmax_t size);

  // ZeroToEnd zeros a file starting from SEEK_CUR to its SEEK_END. May temporarily
  // shorten the length of the file.
  leaf::result<void> zero_to_end();

  leaf::result<std::size_t> write(ioutil::byte_span data);

  asio::awaitable<expected<std::size_t>> async_write(ioutil::byte_span data);

  asio::awaitable<expected<std::size_t>> async_write_vectored_asio(std::span<const std::span<const std::byte>> spans);

  expected<void> fdatasync();
};

lepton::leaf::result<file_endpoint> create_file_endpoint(asio::any_io_executor executor, const std::string& filename,
                                                         asio::file_base::flags flags);

}  // namespace lepton::storage::fileutil

#endif  // _LEPTON_FILE_ENDPOINT_H_
