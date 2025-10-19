#include <string>

#include "enum_name.h"
#include "fmt/format.h"
#include "interaction_env.h"
#include "state.h"

namespace interaction {
// isVoter checks whether node id is in the voter list within st.
static bool is_voter(const lepton::status &st, std::uint64_t id) { return st.config.voters.id_set().contains(id); }

lepton::leaf::result<void> interaction_env::handle_raft_state() {
  for (const auto &n : nodes) {
    auto st = n.raw_node.status();
    std::string vote_status = is_voter(st, st.basic_status.id) ? "(Voter)" : "(Non-Voter)";
    output->write_string(fmt::format("{}: {} {} Term:{} Lead:{}\n", st.basic_status.id,
                                     magic_enum::enum_name(st.basic_status.soft_state.raft_state), vote_status,
                                     st.basic_status.hard_state.term(), st.basic_status.soft_state.leader_id));
  }
  return {};
}
}  // namespace interaction