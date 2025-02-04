#ifndef _LEPTON_ERROR_H_
#define _LEPTON_ERROR_H_
#include <string>

#include "leaf.hpp"

namespace lepton {
namespace leaf {
using namespace boost::leaf;
template <typename T>
using result = boost::leaf::result<T>;
}  // namespace leaf

enum class error_code {
  // 未知错误
  UNKNOWN_ERROR,

  // TODO: 系统错误
  KEY_NOT_FOUND,

  // 编码类错误
  NULL_POINTER,
  OUT_OF_BOUNDS,

  // 业务逻辑错误
  CONFIG_INVALID,
};

struct lepton_error {
  error_code err_code;
  std::string_view message;

  lepton_error(error_code code, const char* msg)
      : err_code(code), message(msg) {}
  lepton_error(error_code code, const std::string& msg)
      : err_code(code), message(msg) {}
};

inline auto new_error(error_code code, const char* msg) {
  return leaf::new_error(lepton_error{code, msg});
}

inline auto new_error(error_code code, const std::string msg) {
  return leaf::new_error(lepton_error{code, msg});
}
}  // namespace lepton

#endif  // _LEPTON_ERROR_H_