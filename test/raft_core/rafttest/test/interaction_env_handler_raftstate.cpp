#include <string>

#include "basic/enum_name.h"
#include "fmt/format.h"
#include "interaction_env.h"
#include "raft_core/tracker/state.h"

namespace interaction {
// isVoter checks whether node id is in the voter list within st.
static bool is_voter(const lepton::core::status &st, std::uint64_t id) {
  return st.config.voters.id_set().contains(id);
}

static auto etcd_rafa_state_name(lepton::core::state_type type) {
  switch (type) {
    case lepton::core::state_type::FOLLOWER:
      return "StateFollower";
    case lepton::core::state_type::CANDIDATE:
      return "StateCandidate";
    case lepton::core::state_type::LEADER:
      return "StateLeader";
    case lepton::core::state_type::PRE_CANDIDATE:
      return "StatePreCandidate";
  }
}

lepton::leaf::result<void> interaction_env::handle_raft_state() {
  for (const auto &n : nodes) {
    auto st = n.raw_node.status();
    std::string vote_status = is_voter(st, st.basic_status.id) ? "(Voter)" : "(Non-Voter)";
    output->write_string(fmt::format("{}: {} {} Term:{} Lead:{}\n", st.basic_status.id,
                                     etcd_rafa_state_name(st.basic_status.soft_state.raft_state), vote_status,
                                     st.basic_status.hard_state.term(), st.basic_status.soft_state.leader_id));
  }
  return {};
}
}  // namespace interaction