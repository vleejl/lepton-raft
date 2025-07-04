#include "raw_node.h"

namespace lepton {

leaf::result<raw_node> new_raw_node(config&& c) {
  const auto async_storage_writes = c.async_storage_writes;
  BOOST_LEAF_AUTO(r, new_raft(std::move(c)));
  return raw_node{std::move(r), async_storage_writes};
}

}  // namespace lepton
