#include <cstddef>

#include "interaction_env.h"
#include "test_utility_data.h"

namespace interaction {
lepton::leaf::result<void> interaction_env::handle_compact(const datadriven::test_data &test_data) {
  std::size_t idx = first_as_node_idx(test_data);
  auto result = safe_stoull(test_data.cmd_args[1].key_);
  assert(result.has_value());
  auto new_first_index = *result;
  return compact(idx, new_first_index);
}

lepton::leaf::result<void> interaction_env::compact(std::size_t node_idx, std::uint64_t new_first_index) {
  LEPTON_LEAF_CHECK(nodes[node_idx].storage->compact(new_first_index));
  return raft_log(node_idx);
}
}  // namespace interaction