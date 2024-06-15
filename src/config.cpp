#include "config.h"

#include "leaf.hpp"
using namespace lepton;

leaf::result<void> config::validate() {
  if (this->id == NONE) {
    return leaf::new_error("cannot use none as id");
  }
  if (this->heartbeat_tick <= 0) {
    return leaf::new_error("heartbeat tick must be greater than 0");
  }

  if (this->election_tick <= this->heartbeat_tick) {
    return leaf::new_error("election tick must be greater than heartbeat tick");
  }

  if (!this->storage.has_value()) {
    return leaf::new_error("storage cannot be null");
  }

  if (this->max_uncommitted_entries_size == 0) {
    this->max_uncommitted_entries_size = NO_LIMIT;
  }

  if (this->max_committed_size_per_ready == 0) {
    this->max_committed_size_per_ready = this->max_size_per_msg;
  }

  if (this->max_inflight_msgs <= 0) {
    return leaf::new_error("max inflight messages must be greater than 0");
  }

  if (this->read_only_opt == read_only_option::READ_ONLY_LEASE_BASED &&
      !this->check_quorum) {
    return leaf::new_error(
        "CheckQuorum must be enabled when ReadOnlyOption is "
        "ReadOnlyLeaseBased");
  }
  return {};
}