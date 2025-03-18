#include "conf_state.h"

#include "error.h"

namespace lepton {

namespace pb {

leaf::result<void> conf_state_equivalent(const raftpb::conf_state &lhs,
                                         const raftpb::conf_state &rhs) {
  if (lhs.voters_size() != rhs.voters_size()) {
    return new_error(logic_error::CONFIG_MISMATCH, "voters size mismatch");
  }
  for (int i = 0; i < lhs.voters_size(); ++i) {
    if (lhs.voters(i) != rhs.voters(i)) {
      return new_error(logic_error::CONFIG_MISMATCH, "voters mismatch");
    }
  }
  if (lhs.voters_outgoing_size() != rhs.voters_outgoing_size()) {
    return new_error(logic_error::CONFIG_MISMATCH,
                     "voters outgoing size mismatch");
  }
  for (int i = 0; i < lhs.voters_outgoing_size(); ++i) {
    if (lhs.voters_outgoing(i) != rhs.voters_outgoing(i)) {
      return new_error(logic_error::CONFIG_MISMATCH,
                       "voters outgoing mismatch");
    }
  }
  if (lhs.learners_size() != rhs.learners_size()) {
    return new_error(logic_error::CONFIG_MISMATCH, "learners size mismatch");
  }
  for (int i = 0; i < lhs.learners_size(); ++i) {
    if (lhs.learners(i) != rhs.learners(i)) {
      return new_error(logic_error::CONFIG_MISMATCH, "learners mismatch");
    }
  }
  if (lhs.learners_next_size() != rhs.learners_next_size()) {
    return new_error(logic_error::CONFIG_MISMATCH,
                     "learners next size mismatch");
  }
  for (int i = 0; i < lhs.learners_next_size(); ++i) {
    if (lhs.learners_next(i) != rhs.learners_next(i)) {
      return new_error(logic_error::CONFIG_MISMATCH, "learners next mismatch");
    }
  }
  if (lhs.auto_leave() != rhs.auto_leave()) {
    return new_error(logic_error::CONFIG_MISMATCH, "auto leave mismatch");
  }
  return {};
}

}  // namespace pb

}  // namespace lepton
