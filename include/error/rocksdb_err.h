#pragma once
#ifndef _LEPTON_ROCKSDB_ERR_H_
#define _LEPTON_ROCKSDB_ERR_H_
#include <rocksdb/status.h>

#include <string>
#include <system_error>

#include "error/base_error_category.h"
namespace lepton {

using rocksdb::Status;

enum class rocksdb_err {
  ok = 0,
  not_found,
  corruption,
  not_supported,
  invalid_argument,
  io_error,
  merge_in_progress,
  incomplete,
  shutdown_in_progress,
  timed_out,
  aborted,
  busy,
  expired,
  try_again,
  compaction_too_large,
  column_family_dropped,
  unknown
};

// 自定义 error_category
class rocksdb_err_category : public base_error_category {
 public:
  const char* name() const noexcept override { return "rocksdb"; }

  std::string message(int ev) const override {
    switch (static_cast<rocksdb_err>(ev)) {
      case rocksdb_err::ok:
        return "OK";
      case rocksdb_err::not_found:
        return "Not found";
      case rocksdb_err::corruption:
        return "Corruption";
      case rocksdb_err::not_supported:
        return "Not supported";
      case rocksdb_err::invalid_argument:
        return "Invalid argument";
      case rocksdb_err::io_error:
        return "I/O error";
      case rocksdb_err::merge_in_progress:
        return "Merge in progress";
      case rocksdb_err::incomplete:
        return "Incomplete";
      case rocksdb_err::shutdown_in_progress:
        return "Shutdown in progress";
      case rocksdb_err::timed_out:
        return "Timed out";
      case rocksdb_err::aborted:
        return "Aborted";
      case rocksdb_err::busy:
        return "Busy";
      case rocksdb_err::expired:
        return "Expired";
      case rocksdb_err::try_again:
        return "Try again";
      case rocksdb_err::compaction_too_large:
        return "Compaction too large";
      case rocksdb_err::column_family_dropped:
        return "Column family dropped";
      default:
        return "Unknown RocksDB error";
    }
  }
};

inline const std::error_category& get_rocksdb_err_category() {
  static rocksdb_err_category instance;
  return instance;
}

// 将 rocksdb::Status 映射为 errc
inline rocksdb_err map_code(Status::Code c) noexcept {
  using C = Status::Code;
  switch (c) {
    case C::kOk:
      return rocksdb_err::ok;
    case C::kNotFound:
      return rocksdb_err::not_found;
    case C::kCorruption:
      return rocksdb_err::corruption;
    case C::kNotSupported:
      return rocksdb_err::not_supported;
    case C::kInvalidArgument:
      return rocksdb_err::invalid_argument;
    case C::kIOError:
      return rocksdb_err::io_error;
    case C::kMergeInProgress:
      return rocksdb_err::merge_in_progress;
    case C::kIncomplete:
      return rocksdb_err::incomplete;
    case C::kShutdownInProgress:
      return rocksdb_err::shutdown_in_progress;
    case C::kTimedOut:
      return rocksdb_err::timed_out;
    case C::kAborted:
      return rocksdb_err::aborted;
    case C::kBusy:
      return rocksdb_err::busy;
    case C::kExpired:
      return rocksdb_err::expired;
    case C::kTryAgain:
      return rocksdb_err::try_again;
    case C::kCompactionTooLarge:
      return rocksdb_err::compaction_too_large;
    case C::kColumnFamilyDropped:
      return rocksdb_err::column_family_dropped;
    default:
      return rocksdb_err::unknown;
  }
}

// 转换函数
inline std::error_code make_error_code(const rocksdb::Status& s) noexcept {
  if (s.ok()) return {0, get_rocksdb_err_category()};
  return {static_cast<int>(map_code(s.code())), get_rocksdb_err_category()};
}

// 可选：同时返回详细字符串（ToString）
inline std::error_code make_error_code(const rocksdb::Status& s, std::string* detailed_msg) noexcept {
  if (s.ok()) {
    if (detailed_msg) detailed_msg->clear();
    return {0, get_rocksdb_err_category()};
  }
  if (detailed_msg) *detailed_msg = s.ToString();
  return {static_cast<int>(map_code(s.code())), get_rocksdb_err_category()};
}

}  // namespace lepton

// 让枚举支持隐式转换为 std::error_code
namespace std {
template <>
struct is_error_code_enum<lepton::rocksdb_err> : true_type {};
}  // namespace std

namespace rocksdb {
inline std::error_code make_error_code(const Status& s) noexcept { return lepton::make_error_code(s); }
}  // namespace rocksdb

#endif  // _LEPTON_ROCKSDB_ERR_H_
