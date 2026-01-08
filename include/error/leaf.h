#pragma once
#ifndef _LEPTON_LEAF_H_
#define _LEPTON_LEAF_H_

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wimplicit-fallthrough"
#pragma GCC diagnostic ignored "-Wshadow"
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Wsign-conversion"
#endif

// IWYU pragma: begin_exports
#include <leaf.hpp>
// IWYU pragma: end_exports

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif

// clang-format off
#if defined(__GNUC__) || defined(__clang__)
#  define LEPTON_LEAF_CHECK(expr)                                 \
     do {                                                         \
         _Pragma("GCC diagnostic push")                           \
         _Pragma("GCC diagnostic ignored \"-Wpedantic\"")        \
         _Pragma("GCC diagnostic ignored \"-Wunused-parameter\"")\
         BOOST_LEAF_CHECK(expr);                                  \
         _Pragma("GCC diagnostic pop")                            \
     } while (false)
#else
#  define LEPTON_LEAF_CHECK(expr)                                 \
     do { BOOST_LEAF_CHECK(expr); } while (false)
#endif
// clang-format on

namespace lepton {

namespace leaf {
using namespace boost::leaf;

template <typename T>
using result = boost::leaf::result<T>;
}  // namespace leaf

template <typename T>
inline void discard(leaf::result<T>&&) noexcept {}

}  // namespace lepton

#endif  // _LEPTON_LEAF_H_
