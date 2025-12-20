#pragma once
#ifndef _LEPTON_LEAF_H_
#define _LEPTON_LEAF_H_

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wimplicit-fallthrough"
#endif

#include <leaf.hpp>

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif

// clang-format off
#if defined(__GNUC__) || defined(__clang__)
#  define LEPTON_LEAF_CHECK(expr)                          \
     do {                                                  \
         _Pragma("GCC diagnostic push")                    \
         _Pragma("GCC diagnostic ignored \"-Wpedantic\"")  \
         BOOST_LEAF_CHECK(expr);                           \
         _Pragma("GCC diagnostic pop")                     \
     } while (0)
#else
#  define LEPTON_LEAF_CHECK(expr) BOOST_LEAF_CHECK(expr)
#endif
// clang-format on

namespace lepton {
namespace leaf {
using namespace boost::leaf;
template <typename T>
using result = boost::leaf::result<T>;
}  // namespace leaf

template <typename T>
inline void discard(leaf::result<T> &&) noexcept {}
}  // namespace lepton

#endif  // _LEPTON_LEAF_H_