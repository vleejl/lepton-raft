#ifndef _LEPTON_READER_H_
#define _LEPTON_READER_H_

#include <proxy.h>

#include <asio/awaitable.hpp>
#include <cstddef>

#include "expected.h"
#include "fixed_byte_buffer.h"
#include "lepton_error.h"

namespace lepton {

PRO_DEF_MEM_DISPATCH(reader_read, read);

PRO_DEF_MEM_DISPATCH(reader_async_read, async_read);

// clang-format off
struct reader : pro::facade_builder 
  ::add_convention<reader_read, leaf::result<std::size_t>(fixed_byte_buffer& buffer)> 
  ::add_convention<reader_async_read, asio::awaitable<expected<std::size_t>>(fixed_byte_buffer& buffer)> 
  ::add_skill<pro::skills::as_view>
  ::build{};
// clang-format on

}  // namespace lepton

#endif  // _LEPTON_READER_H_
