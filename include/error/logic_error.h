#pragma once
#ifndef _LEPTON_LOGIC_ERROR_H_
#define _LEPTON_LOGIC_ERROR_H_
#include <cassert>
#include <string>
#include <system_error>

#include "error/base_error_category.h"
namespace lepton {

enum class logic_error {
  NULL_POINTER = 1,
  OUT_OF_BOUNDS,
  KEY_NOT_FOUND,
  INVALID_PARAM,
  EMPTY_ARRAY,
  LOOP_BREAK,  // 特殊错误码, errBreak is a sentinel error used to break a callback-based loop.
  COROUTINE_EXIST,
};

// logic_error_category
class logic_error_category : public base_error_category {
 public:
  const char* name() const noexcept override { return "logic_error"; }

  std::string message(int ev) const override {
    switch (static_cast<logic_error>(ev)) {
      case logic_error::NULL_POINTER:
        return "Null pointer error";
      case logic_error::OUT_OF_BOUNDS:
        return "Out of bounds error";
      case logic_error::KEY_NOT_FOUND:
        return "Key not found error";
      case logic_error::INVALID_PARAM:
        return "Invalid param error";
      case logic_error::EMPTY_ARRAY:
        return "Empty array error";
      case logic_error::LOOP_BREAK:
        return "Loop break error";
      default:
        assert(false);
        return "Unrecognized logic error";
    }
  }
};

// 全局实例
inline const logic_error_category& get_logic_error_category() {
  static logic_error_category instance;
  return instance;
}

// make_error_code 实现
inline std::error_code make_error_code(logic_error e) { return {static_cast<int>(e), get_logic_error_category()}; }
}  // namespace lepton

namespace std {

template <>
struct is_error_code_enum<lepton::logic_error> : true_type {};
}  // namespace std

#endif  // _LEPTON_LOGIC_ERROR_H_
