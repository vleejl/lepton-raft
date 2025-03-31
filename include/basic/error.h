#ifndef _LEPTON_ERROR_H_
#define _LEPTON_ERROR_H_
#include <source_location>
#include <string>
#include <system_error>

#include "leaf.hpp"
#include "log.h"

namespace lepton {
namespace leaf {
using namespace boost::leaf;
template <typename T>
using result = boost::leaf::result<T>;
}  // namespace leaf

enum class system_error {
  UNKNOWN_ERROR = 1,
};

enum class encoding_error {
  NULL_POINTER = 1,
  OUT_OF_BOUNDS,
};

enum class storage_error {
  // ErrCompacted is returned by Storage.Entries/Compact when a requested
  // index is unavailable because it predates the last snapshot.
  COMPACTED = 1,
  // ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
  // index is older than the existing snapshot.
  SNAP_OUT_OF_DATE,
  // ErrUnavailable is returned by Storage interface when the requested log
  // entries
  // are unavailable.
  UNAVAILABLE,
  // ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when
  // the required
  // snapshot is temporarily unavailable.
  SNAPSHOT_TEMPORARILY_UNAVAILABLE,
};

enum class logic_error {
  CONFIG_INVALID = 1,
  CONFIG_MISMATCH,
  KEY_NOT_FOUND,
  INVALID_PARAM,
  PROPOSAL_DROPPED,
};

// 通用的错误分类基类
class base_error_category : public std::error_category {
 public:
  virtual const char* name() const noexcept override = 0;
  virtual std::string message(int ev) const override = 0;
};

// system_error_category
class system_error_category : public base_error_category {
 public:
  const char* name() const noexcept override { return "system_error"; }

  std::string message(int ev) const override {
    switch (static_cast<system_error>(ev)) {
      case system_error::UNKNOWN_ERROR:
        return "Unknown error";
      default:
        return "Unrecognized system error";
    }
  }
};

// encoding_error_category
class encoding_error_category : public base_error_category {
 public:
  const char* name() const noexcept override { return "encoding_error"; }

  std::string message(int ev) const override {
    switch (static_cast<encoding_error>(ev)) {
      case encoding_error::NULL_POINTER:
        return "Null pointer error";
      case encoding_error::OUT_OF_BOUNDS:
        return "Out of bounds error";
      default:
        return "Unrecognized encoding error";
    }
  }
};

// storage_error_category
class storage_error_category : public base_error_category {
 public:
  const char* name() const noexcept override { return "storage_error"; }

  std::string message(int ev) const override {
    switch (static_cast<storage_error>(ev)) {
      case storage_error::COMPACTED:
        return "requested index is unavailable due to compaction";
      case storage_error::SNAP_OUT_OF_DATE:
        return "requested index is older than the existing snapshot";
      case storage_error::UNAVAILABLE:
        return "requested entry at index is unavailable";
      case storage_error::SNAPSHOT_TEMPORARILY_UNAVAILABLE:
        return "snapshot is temporarily unavailable";
      default:
        return "Unrecognized storage error";
    }
  }
};

// logic_error_category
class logic_error_category : public base_error_category {
 public:
  const char* name() const noexcept override { return "logic_error"; }

  std::string message(int ev) const override {
    switch (static_cast<logic_error>(ev)) {
      case logic_error::CONFIG_INVALID:
        return "Invalid configuration";
      case logic_error::KEY_NOT_FOUND:
        return "Key not found";
      case logic_error::INVALID_PARAM:
        return "Invalid param";
      default:
        return "Unrecognized logic error";
    }
  }
};

// 全局实例
const system_error_category& get_system_error_category();

const encoding_error_category& get_encoding_error_category();

const storage_error_category& get_storage_error_category();

const logic_error_category& get_logic_error_category();

// make_error_code 实现
inline std::error_code make_error_code(system_error e) { return {static_cast<int>(e), get_system_error_category()}; }

inline std::error_code make_error_code(encoding_error e) {
  return {static_cast<int>(e), get_encoding_error_category()};
}

inline std::error_code make_error_code(storage_error e) { return {static_cast<int>(e), get_storage_error_category()}; }

inline std::error_code make_error_code(logic_error e) { return {static_cast<int>(e), get_logic_error_category()}; }

template <typename error_code_type>
concept err_types =
    std::is_same_v<error_code_type, lepton::system_error> || std::is_same_v<error_code_type, lepton::encoding_error> ||
    std::is_same_v<error_code_type, lepton::storage_error> || std::is_same_v<error_code_type, lepton::logic_error>;
struct lepton_error {
  std::error_code err_code;
  std::string_view message;
  std::source_location location;

  template <err_types err_type>
  lepton_error(err_type code, std::source_location&& location) : err_code(make_error_code(code)), location(location) {}

  template <err_types err_type>
  lepton_error(err_type code, const char* msg, std::source_location&& location)
      : err_code(make_error_code(code)), message(msg), location(location) {}

  template <err_types err_type>
  lepton_error(err_type code, std::string_view msg, std::source_location&& location)
      : err_code(make_error_code(code)), message(msg), location(location) {}

  template <err_types err_type>
  lepton_error(err_type code, const std::string& msg, std::source_location&& location)
      : err_code(make_error_code(code)), message(msg), location(location) {}

  template <err_types err_type>
  auto operator<=>(err_type error_code) const {
    return err_code <=> make_error_code(error_code);
  }

  auto operator<=>(const std::error_code& rhs_err_code) const { return err_code <=> rhs_err_code; }
};

template <err_types err_type>
bool operator==(const lepton_error& error, const err_type& code) {
  return error.err_code == code;
}

template <err_types err_type>
bool operator==(const err_type& code, const lepton_error& error) {
  return code == error.err_code;
}

inline bool operator==(const lepton_error& error, const std::error_code& code) { return error.err_code == code; }

inline bool operator==(const std::error_code& code, const lepton_error& error) { return code == error.err_code; }

template <err_types err_type>
bool operator==(const std::error_code& err_code, const err_type& code) {
  return err_code == make_error_code(code);
}

template <err_types err_type>
bool operator==(const err_type& code, const std::error_code& err_code) {
  return err_code == make_error_code(code);
}

template <err_types err_type>
bool operator!=(const std::error_code& err_code, const err_type& code) {
  return err_code == make_error_code(code);
}

template <err_types err_type>
bool operator!=(const err_type& code, const std::error_code& err_code) {
  return err_code == make_error_code(code);
}

template <err_types error_code_type, typename error_msg_type>
auto new_error(error_code_type code, error_msg_type msg,
               std::source_location location = std::source_location::current()) {
  return leaf::new_error(lepton_error{code, msg, std::move(location)});
}

template <err_types error_code_type>
auto new_error(error_code_type code, std::source_location location = std::source_location::current()) {
  return leaf::new_error(lepton_error{code, std::move(location)});
}

inline auto new_error(const lepton::lepton_error& err) { return leaf::new_error(err); }

inline void panic(std::string_view message, std::source_location location = std::source_location::current()) {
  LEPTON_CRITICAL("panic error {}, file_name:{} line:{} column:{} function:{}", message, location.file_name(),
                  location.line(), location.column(), location.function_name());
  assert(false);  // TODO(bdarnell)
}
}  // namespace lepton

// namespace std {
namespace std {
template <>
struct is_error_code_enum<lepton::system_error> : true_type {};

template <>
struct is_error_code_enum<lepton::encoding_error> : true_type {};

template <>
struct is_error_code_enum<lepton::storage_error> : true_type {};

template <>
struct is_error_code_enum<lepton::logic_error> : true_type {};
}  // namespace std

#endif  // _LEPTON_ERROR_H_