#pragma once
#ifndef _LEPTON_STREAM_FILE_H_
#define _LEPTON_STREAM_FILE_H_

#if defined(__linux__)
#include <asio/stream_file.hpp>
#else
#include <asio/posix/stream_descriptor.hpp>
#endif

namespace lepton::storage::fileutil {

#if defined(__linux__)
using stream_file = asio::stream_file;
#else
using stream_file = asio::posix::stream_descriptor;
#endif

}  // namespace lepton::storage::fileutil

#endif  // _LEPTON_STREAM_FILE_H_
