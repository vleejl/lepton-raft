#pragma once
#ifndef _LEPTON_READER_H_
#define _LEPTON_READER_H_

#include <proxy.h>

#include <asio/awaitable.hpp>
#include <asio/buffer.hpp>
#include <cstddef>
#include <ranges>
#include <vector>

#include "error/expected.h"
#include "error/lepton_error.h"

namespace lepton::storage::ioutil {

PRO_DEF_MEM_DISPATCH(reader_read, read);

PRO_DEF_MEM_DISPATCH(reader_name, name);

PRO_DEF_MEM_DISPATCH(reader_size, size);

PRO_DEF_MEM_DISPATCH(reader_async_read, async_read);

// clang-format off
struct reader : pro::facade_builder 
  // ::add_convention<reader_read, leaf::result<std::size_t>(asio::mutable_buffer buffer)>
  ::add_convention<reader_name, const std::string&() const>
  ::add_convention<reader_size, leaf::result<std::size_t>() const>
  ::add_convention<reader_async_read, asio::awaitable<expected<std::size_t>>(asio::mutable_buffer buffer)> 
  ::add_skill<pro::skills::as_view>
  ::build{};
// clang-format on

}  // namespace lepton::storage::ioutil

#endif  // _LEPTON_READER_H_
