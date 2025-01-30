#include "raft.h"

#include "error.h"
#include "raft_log.h"
using namespace lepton;

namespace lepton {
leaf::result<std::unique_ptr<raft>> new_raft(
    std::unique_ptr<config> config_ptr) {
  if (config_ptr == nullptr) {
    return leaf::new_error("config is null ptr, can not new raft object");
  }
  BOOST_LEAF_CHECK(config_ptr->validate());
  BOOST_LEAF_AUTO(raftlog, new_raft_log_with_size(
                               config_ptr->storage,
                               config_ptr->max_uncommitted_entries_size));
  BOOST_LEAF_AUTO(inital_states, config_ptr->storage->initial_state());
  auto [hard_state, conf_state] = inital_states;
  return {};
}
}  // namespace lepton