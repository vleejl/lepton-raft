#include <cstddef>

#include "error/logic_error.h"
#include "interaction_env.h"
#include "raft_core/tracker/progress.h"

namespace interaction {

lepton::leaf::result<void> interaction_env::handle_tick_election(const datadriven::test_data &test_data) {
  std::size_t idx = first_as_node_idx(test_data);
  return tick(idx, nodes[idx].config.election_tick);
}

lepton::leaf::result<void> interaction_env::handle_tick_heartbeat(const datadriven::test_data &test_data) {
  std::size_t idx = first_as_node_idx(test_data);
  return tick(idx, nodes[idx].config.heartbeat_tick);
}

lepton::leaf::result<void> interaction_env::tick(std::size_t node_idx, int count) {
  for (int i = 0; i < count; ++i) {
    nodes[node_idx].raw_node.tick();
  }
  return {};
}

}  // namespace interaction