#include "conf_state.h"

#include <cstdint>

#include "lepton_error.h"

namespace lepton {

namespace pb {

leaf::result<void> conf_state_equivalent(const raftpb::conf_state &lhs, const raftpb::conf_state &rhs) {
  // Voters comparison (set-based)
  {
    std::set<std::uint64_t> voter_lhs{lhs.voters().begin(), lhs.voters().end()};
    std::set<std::uint64_t> voter_rhs{rhs.voters().begin(), rhs.voters().end()};
    if (voter_lhs != voter_rhs) {
      return new_error(raft_error::CONFIG_MISMATCH, "voters mismatch");
    }
  }

  // Voters outgoing comparison (set-based)
  {
    std::set<std::uint64_t> out_lhs{lhs.voters_outgoing().begin(), lhs.voters_outgoing().end()};
    std::set<std::uint64_t> out_rhs{rhs.voters_outgoing().begin(), rhs.voters_outgoing().end()};
    if (out_lhs != out_rhs) {
      return new_error(raft_error::CONFIG_MISMATCH, "voters outgoing mismatch");
    }
  }

  // Learners comparison (set-based)
  {
    std::set<std::uint64_t> learner_lhs{lhs.learners().begin(), lhs.learners().end()};
    std::set<std::uint64_t> learner_rhs{rhs.learners().begin(), rhs.learners().end()};
    if (learner_lhs != learner_rhs) {
      return new_error(raft_error::CONFIG_MISMATCH, "learners mismatch");
    }
  }

  // Learners next comparison (set-based)
  {
    std::set<std::uint64_t> next_lhs{lhs.learners_next().begin(), lhs.learners_next().end()};
    std::set<std::uint64_t> next_rhs{rhs.learners_next().begin(), rhs.learners_next().end()};
    if (next_lhs != next_rhs) {
      return new_error(raft_error::CONFIG_MISMATCH, "learners next mismatch");
    }
  }

  // Auto leave comparison
  if (lhs.auto_leave() != rhs.auto_leave()) {
    return new_error(raft_error::CONFIG_MISMATCH, "auto leave mismatch");
  }

  return {};
}

}  // namespace pb

}  // namespace lepton
