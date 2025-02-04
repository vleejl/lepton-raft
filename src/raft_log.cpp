#include "raft_log.h"

#include <fmt/core.h>
#include <fmt/format.h>

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

std::string raft_log::string() {
  return fmt::format(
      "committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d",
      committed_, applied_, unstable_.offset(),
      unstable_.entries_view().size());
}

leaf::result<raft_log> new_raft_log_with_size(
    pro::proxy_view<storage_builer> storage, std::uint64_t max_next_ents_size) {
  if (!storage.has_value()) {
    return new_error(error_code::NULL_POINTER, "storage must not be nil");
  }

  BOOST_LEAF_AUTO(first_index, storage->first_index());
  BOOST_LEAF_AUTO(last_index, storage->last_index());
  return raft_log{storage, last_index + 1, first_index - 1, first_index - 1,
                  max_next_ents_size};
}
}  // namespace lepton
