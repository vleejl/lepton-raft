#include <cstddef>

#include "describe.h"
#include "interaction_env.h"

namespace interaction {

lepton::leaf::result<void> interaction_env::handle_raft_log(const datadriven::test_data &test_data) {
  std::size_t idx = first_as_node_idx(test_data);
  return raft_log(idx);
}

lepton::leaf::result<void> interaction_env::raft_log(std::size_t node_idx) {
  auto &s = nodes[node_idx].storage;
  BOOST_LEAF_AUTO(first_index, s->first_index());
  BOOST_LEAF_AUTO(last_index, s->last_index());
  if (last_index < first_index) {
    // TODO(tbg): this is what MemoryStorage returns, but unclear if it's
    // the "correct" thing to do.
    output->write_string(fmt::format("log is empty: first index={}, last index={}", first_index, last_index));
    return {};
  }
  BOOST_LEAF_AUTO(ents, s->entries(first_index, last_index + 1, lepton::core::NO_LIMIT));
  output->write_string(lepton::core::describe_entries(ents, nullptr));
  return {};
}
}  // namespace interaction