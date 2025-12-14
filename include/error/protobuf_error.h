#ifndef _LEPTON_PROTOBUF_ERROR_H_
#define _LEPTON_PROTOBUF_ERROR_H_

#include <cassert>
#include <string>
#include <system_error>

#include "base_error_category.h"
namespace lepton {

enum class protobuf_error {
  PROOBUF_PARSE_FAILED = 1,
  SERIALIZE_TO_ARRAY_FAILED,
};

// protobuf_error_category
class protobuf_error_category : public base_error_category {
 public:
  const char* name() const noexcept override { return "protobuf_error"; }

  std::string message(int ev) const override {
    switch (static_cast<protobuf_error>(ev)) {
      case protobuf_error::PROOBUF_PARSE_FAILED:
        return "Protobuf parse error";
      case protobuf_error::SERIALIZE_TO_ARRAY_FAILED:
        return "Protobuf serialize to array error";
      default:
        assert(false);
        return "Unrecognized protobuf error";
    }
  }
};

// 全局实例
inline const protobuf_error_category& get_protobuf_error_category() {
  static protobuf_error_category instance;
  return instance;
}

// make_error_code 实现
inline std::error_code make_error_code(protobuf_error e) {
  return {static_cast<int>(e), get_protobuf_error_category()};
}
}  // namespace lepton

namespace std {

template <>
struct is_error_code_enum<lepton::protobuf_error> : true_type {};
}  // namespace std

#endif  // _LEPTON_PROTOBUF_ERROR_H_
