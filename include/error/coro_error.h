#pragma once
#ifndef _LEPTON_CORO_ERROR_H_
#define _LEPTON_CORO_ERROR_H_

#include <cassert>
#include <string>
#include <system_error>

#include "error/base_error_category.h"
namespace lepton {

enum class coro_error {
  COROUTINE_EXIST = 1,
  STOPPED,
  UNKNOWN_ERROR,
};

// coro_error_category
class coro_error_category : public base_error_category {
 public:
  const char* name() const noexcept override { return "coro_error"; }

  std::string message(int ev) const override {
    switch (static_cast<coro_error>(ev)) {
      case coro_error::COROUTINE_EXIST:
        return "Coroutine has existed";
      case coro_error::STOPPED:
        return "Coroutine has stopped";
      case coro_error::UNKNOWN_ERROR:
        return "Coroutine: unknown error";
      default:
        assert(false);
        return "Unrecognized logic error";
    }
  }
};

// 全局实例
inline const coro_error_category& get_coro_error_category() {
  static coro_error_category instance;
  return instance;
}

// make_error_code 实现
inline std::error_code make_error_code(coro_error e) { return {static_cast<int>(e), get_coro_error_category()}; }
}  // namespace lepton

namespace std {

template <>
struct is_error_code_enum<lepton::coro_error> : true_type {};
}  // namespace std

#endif  // _LEPTON_CORO_ERROR_H_
