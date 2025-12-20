#include "raft_core/config.h"

#include "error/lepton_error.h"
#include "error/logic_error.h"

namespace lepton::core {

leaf::result<void> config::validate() const {
  if (this->id == NONE) {
    return new_error(raft_error::CONFIG_INVALID, "cannot use none as id");
  }
  if (this->heartbeat_tick <= 0) {
    return new_error(raft_error::CONFIG_INVALID, "heartbeat tick must be greater than 0");
  }

  if (this->election_tick <= this->heartbeat_tick) {
    return new_error(raft_error::CONFIG_INVALID, "election tick must be greater than heartbeat tick");
  }

  if (!this->storage.has_value()) {
    return new_error(raft_error::CONFIG_INVALID, "storage cannot be null");
  }

  if (this->max_inflight_msgs <= 0) {
    return new_error(raft_error::CONFIG_INVALID, "max inflight messages must be greater than 0");
  }

  if (this->max_inflight_bytes != NO_LIMIT) {
    if (this->max_inflight_bytes < this->max_inflight_msgs) {
      return new_error(raft_error::CONFIG_INVALID, "max inflight bytes must be >= max message size");
    }
  }

  if (this->read_only_opt == read_only_option::READ_ONLY_LEASE_BASED && !this->check_quorum) {
    return new_error(raft_error::CONFIG_INVALID,
                     "CheckQuorum must be enabled when ReadOnlyOption is "
                     "ReadOnlyLeaseBased");
  }
  if (this->logger == nullptr) {
    return new_error(logic_error::NULL_POINTER, "logger cannot be null");
  }
  return {};
}

}  // namespace lepton::core
