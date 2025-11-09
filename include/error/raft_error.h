#ifndef _LEPTON_RAFT_ERROR_H_
#define _LEPTON_RAFT_ERROR_H_
#include <cassert>
#include <string>
#include <system_error>

#include "base_error_category.h"
namespace lepton {

enum class raft_error {
  CONFIG_INVALID = 1,

  CONFIG_MISMATCH,

  PROPOSAL_DROPPED,
  // ErrStepLocalMsg is returned when try to step a local raft message

  STEP_LOCAL_MSG,

  // ErrStepPeerNotFound is returned when try to step a response message
  // but there is no peer found in raft.trk for that node.
  STEP_PEER_NOT_FOUND,

  // ErrStopped is returned by methods on Nodes that have been stopped.
  STOPPED,

  // ErrUnknownError is returned when an unknown error occurs.
  UNKNOWN_ERROR,
};

class raft_error_category : public base_error_category {
 public:
  const char* name() const noexcept override { return "raft_error"; }

  std::string message(int ev) const override {
    switch (static_cast<raft_error>(ev)) {
      case raft_error::CONFIG_INVALID:
        return "raft: invalid configuration";
      case raft_error::CONFIG_MISMATCH:
        return "raft: configuration mismatch";
      case raft_error::PROPOSAL_DROPPED:
        return "raft proposal dropped";
      case raft_error::STEP_LOCAL_MSG:
        return "raft: cannot step raft local message";
      case raft_error::STEP_PEER_NOT_FOUND:
        return "raft: cannot step as peer not found";
      case raft_error::STOPPED:
        return "raft: stopped";
      case raft_error::UNKNOWN_ERROR:
        return "raft: unknown error";
      default:
        assert(false);
        return "Unrecognized storage error";
    }
  }
};

inline const raft_error_category& get_raft_error_category() {
  static raft_error_category instance;
  return instance;
}

inline std::error_code make_error_code(raft_error e) { return {static_cast<int>(e), get_raft_error_category()}; }

}  // namespace lepton

namespace std {

template <>
struct is_error_code_enum<lepton::raft_error> : true_type {};
}  // namespace std

#endif  // _LEPTON_RAFT_ERROR_H_
