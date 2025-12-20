#pragma once
#ifndef _LEPTON_IO_ERROR_H_
#define _LEPTON_IO_ERROR_H_

#include <cassert>
#include <string>
#include <system_error>

#include "error/base_error_category.h"
namespace lepton {

enum class io_error {
  // EOF is the error returned by Read when no more input is available.
  // (Read must return EOF itself, not an error wrapping EOF,
  // because callers will test for EOF using ==.)
  // Functions should return EOF only to signal a graceful end of input.
  // If the EOF occurs unexpectedly in a structured data stream,
  // the appropriate error is either [ErrUnexpectedEOF] or some other error
  // giving more detail.
  IO_EOF = 1,

  // ErrUnexpectedEOF means that EOF was encountered in the
  // middle of reading a fixed-size block or data structure.
  UNEXPECTED_EOF,

  // ErrShortBuffer means that a read required a longer buffer than was provided.
  SHORT_BUFFER,

  PARH_NOT_EXIT,
};

// io_error_category
class io_error_category : public base_error_category {
 public:
  const char* name() const noexcept override { return "io_error"; }

  std::string message(int ev) const override {
    switch (static_cast<io_error>(ev)) {
      case io_error::IO_EOF:
        return "IO.EOF";
      case io_error::UNEXPECTED_EOF:
        return "Unexpected EOF error";
      case io_error::SHORT_BUFFER:
        return "Short buffer error";
      case io_error::PARH_NOT_EXIT:
        return "Path not exist";
      default:
        assert(false);
        return "Unrecognized io error";
    }
  }
};

// 全局实例
inline const io_error_category& get_io_error_category() {
  static io_error_category instance;
  return instance;
}

// make_error_code 实现
inline std::error_code make_error_code(io_error e) { return {static_cast<int>(e), get_io_error_category()}; }
}  // namespace lepton

namespace std {

template <>
struct is_error_code_enum<lepton::io_error> : true_type {};
}  // namespace std

#endif  // _LEPTON_IO_ERROR_H_
