#ifndef _LEPTON_CHANNEL_H_
#define _LEPTON_CHANNEL_H_
#include <asio.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <asio/experimental/channel.hpp>
#include <cstdint>
#include <iostream>

#include "asio/awaitable.hpp"
#include "asio/co_spawn.hpp"
#include "raft.pb.h"
using asio::as_tuple;
using asio::awaitable;
using asio::buffer;
using asio::co_spawn;
using asio::detached;
using asio::io_context;
using asio::steady_timer;
using asio::use_awaitable;
using asio::experimental::channel;
using asio::ip::tcp;
namespace this_coro = asio::this_coro;
using namespace asio::experimental::awaitable_operators;
using namespace std::literals::chrono_literals;
namespace lepton {}  // namespace lepton

#endif  // _LEPTON_CHANNEL_H_
