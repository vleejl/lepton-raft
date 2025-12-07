#ifndef _LEPTON_WRITER_H_
#define _LEPTON_WRITER_H_
#include <proxy.h>

#include <asio/awaitable.hpp>
#include <cstddef>

#include "byte_span.h"
#include "expected.h"

namespace lepton::storage::ioutil {

PRO_DEF_MEM_DISPATCH(writer_write, write);

PRO_DEF_MEM_DISPATCH(writer_async_write, async_write);

// clang-format off
struct writer : pro::facade_builder 
  // ::add_convention<writer_write, leaf::result<std::size_t>(byte_span data)> 
  ::add_convention<writer_async_write, asio::awaitable<expected<std::size_t>>(byte_span data)> 
  ::add_skill<pro::skills::as_view>
  ::build{};
// clang-format on

}  // namespace lepton::storage::ioutil

#endif  // _LEPTON_WRITER_H_
