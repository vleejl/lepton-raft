#pragma once
#ifndef _LEPTON_DEFER_H_
#define _LEPTON_DEFER_H_
#include <utility>

#include "basic/utility_macros.h"
namespace lepton {

template <typename F>
class defer_template {
  NOT_COPYABLE(defer_template);
  F func_;

 public:
  defer_template(F&& func) : func_(std::forward<F>(func)) {}
  ~defer_template() { func_(); }

  defer_template(defer_template&&) = default;
  defer_template& operator=(defer_template&&) = default;
};

// 辅助函数
template <typename F>
defer_template<F> make_defer(F&& f) {
  return defer_template<F>(std::forward<F>(f));
}

}  // namespace lepton

#define CONCAT_IMPL(a, b) a##b
#define CONCAT(a, b) CONCAT_IMPL(a, b)
#define DEFER(...) auto CONCAT(_defer_, __LINE__) = lepton::make_defer([&]() { __VA_ARGS__; })

#endif  // _LEPTON_DEFER_H_
