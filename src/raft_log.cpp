#include "raft_log.h"

#include <memory>

#include "error.h"

namespace lepton {
raft_log::raft_log(pro::proxy_view<storage_builer> storage,
                   std::uint64_t offset, std::uint64_t committed,
                   std::uint64_t applied, std::uint64_t max_next_ents_size)
    : storage_(storage),
      unstable_(offset),
      committed_(committed),
      applied_(applied),
      max_next_ents_size_(max_next_ents_size) {}

leaf::result<std::unique_ptr<raft_log>> new_raft_log_with_size(
    pro::proxy_view<storage_builer> storage, std::uint64_t max_next_ents_size) {
  if (!storage.has_value()) {
    return leaf::new_error("storage must not be nil");
  }

  BOOST_LEAF_AUTO(first_index, storage->first_index());
  BOOST_LEAF_AUTO(last_index, storage->last_index());
  return std::make_unique<raft_log>(storage, last_index + 1, first_index - 1,
                                    first_index - 1, max_next_ents_size);
}
}  // namespace lepton
