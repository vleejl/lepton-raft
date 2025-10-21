#include <cassert>
#include <cstddef>

#include "data_driven.h"
#include "interaction_env.h"
#include "logic_error.h"

namespace interaction {

lepton::leaf::result<void> interaction_env::handle_propose(const datadriven::test_data &test_data) {
  std::size_t idx = first_as_node_idx(test_data);
  if (test_data.cmd_args.size() != 2 || test_data.cmd_args[1].vals_.size() > 0) {
    LEPTON_CRITICAL("expected exactly one key with no vals");
    return new_error(lepton::logic_error::INVALID_PARAM);
  }
  return propose(idx, test_data.cmd_args[1].key_);
}

lepton::leaf::result<void> interaction_env::propose(std::size_t node_idx, const std::string &data) {
  if (node_idx < 0 || node_idx >= nodes.size()) {
    return new_error(lepton::logic_error::INVALID_PARAM);
  }
  nodes[node_idx].raw_node.propose(std::string{data});
  return {};
}

}  // namespace interaction