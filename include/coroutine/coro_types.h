#pragma once
#ifndef _LEPTON_CORO_TYPES_H_
#define _LEPTON_CORO_TYPES_H_
#include <asio/experimental/channel.hpp>
namespace lepton::coro {
template <typename T>
using channel = asio::experimental::channel<void(asio::error_code, T)>;

using signal_channel = asio::experimental::channel<void(asio::error_code)>;
}  // namespace lepton::coro

#endif  // _LEPTON_CORO_TYPES_H_
