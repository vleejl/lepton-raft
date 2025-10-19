#include <cstddef>

#include "data_driven.h"
#include "interaction_env.h"
#include "logic_error.h"

namespace interaction {

lepton::leaf::result<void> interaction_env::handle_forget_leader(const datadriven::test_data &test_data) {
  std::size_t idx = first_as_node_idx(test_data);
  return forget_leader(idx);
}

lepton::leaf::result<void> interaction_env::forget_leader(std::size_t node_idx) {
  if (node_idx < 0 || node_idx >= nodes.size()) {
    return new_error(lepton::logic_error::INVALID_PARAM);
  }
  nodes[node_idx].raw_node.forget_leader();
  return {};
}

}  // namespace interaction