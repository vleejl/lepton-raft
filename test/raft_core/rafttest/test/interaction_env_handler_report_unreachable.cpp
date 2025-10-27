
#include "data_driven.h"
#include "interaction_env.h"
#include "logic_error.h"

namespace interaction {

lepton::leaf::result<void> interaction_env::handle_report_unreachable(const datadriven::test_data &test_data) {
  auto idxs = node_idxs(test_data);
  if (idxs.size() != 2) {
    return lepton::new_error(lepton::logic_error::INVALID_PARAM,
                             "must specify exactly two node indexes: node on which to report, and reported node");
  }
  nodes[idxs[0]].raw_node.report_unreachable(nodes[idxs[1]].config.id);
  return {};
}
}  // namespace interaction