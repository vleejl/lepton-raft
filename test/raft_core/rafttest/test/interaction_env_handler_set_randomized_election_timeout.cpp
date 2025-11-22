#include <cassert>
#include <cstdint>
#include <string>

#include "interaction_env.h"

namespace interaction {
lepton::leaf::result<void> interaction_env::handle_set_randomized_election_timeout(
    const datadriven::test_data &test_data) {
  std::size_t idx = first_as_node_idx(test_data);
  std::uint64_t timeout = 0;
  datadriven::scan_first_args(test_data, "timeout", timeout);
  assert(timeout > 0);
  EXPECT_NE(timeout, 9);
  options.set_randomized_election_timeout(nodes[idx].raw_node, timeout);
  return {};
}

}  // namespace interaction