#include "raft.h"

#include <utility>

#include "error.h"
#include "raft_log.h"
using namespace lepton;

namespace lepton {
leaf::result<raft> new_raft(const config& c) {
  BOOST_LEAF_CHECK(c.validate());
  BOOST_LEAF_AUTO(raftlog, new_raft_log_with_size(
                               c.storage, c.max_uncommitted_entries_size));
  BOOST_LEAF_AUTO(inital_states, c.storage->initial_state());
  auto [hard_state, conf_state] = inital_states;
  raft r{c.id,
         std::move(raftlog),
         c.max_size_per_msg,
         c.max_uncommitted_entries_size,
         c.max_inflight_msgs,
         c.election_tick,
         c.heartbeat_tick,
         c.check_quorum,
         c.pre_vote,
         c.read_only_opt,
         c.disable_proposal_forwarding};

  return r;
}
}  // namespace lepton