#include <cstddef>

#include "error/logic_error.h"
#include "interaction_env.h"

namespace interaction {
lepton::leaf::result<void> interaction_env::handle_campaign(const datadriven::test_data &test_data) {
  std::size_t idx = first_as_node_idx(test_data);
  return campaign(idx);
}

lepton::leaf::result<void> interaction_env::campaign(std::size_t node_idx) {
  if (node_idx < 0 || node_idx >= nodes.size()) {
    return new_error(lepton::logic_error::INVALID_PARAM);
  }
  return nodes[node_idx].raw_node.campaign();
}
}  // namespace interaction