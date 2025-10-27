#include <cstddef>

#include "interaction_env.h"
#include "logic_error.h"
#include "progress.h"

namespace interaction {
lepton::leaf::result<void> interaction_env::handle_status(const datadriven::test_data &test_data) {
  std::size_t idx = first_as_node_idx(test_data);
  return status(idx);
}

lepton::leaf::result<void> interaction_env::status(std::size_t node_idx) {
  if (node_idx < 0 || node_idx >= nodes.size()) {
    return new_error(lepton::logic_error::INVALID_PARAM);
  }
  // TODO(tbg): actually print the full status.
  auto st = nodes[node_idx].raw_node.status();
  lepton::tracker::progress_map m;
  for (const auto &[id, pr] : st.progress.view()) {
    m.add_progress(id, pr.clone());
  }
  output->write_string(m.string());
  return {};
}

}  // namespace interaction