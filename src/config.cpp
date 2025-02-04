#include "config.h"

#include "error.h"
using namespace lepton;

leaf::result<void> config::validate() const {
  if (this->id == NONE) {
    return new_error(error_code::CONFIG_INVALID, "cannot use none as id");
  }
  if (this->heartbeat_tick <= 0) {
    return new_error(error_code::CONFIG_INVALID,
                     "heartbeat tick must be greater than 0");
  }

  if (this->election_tick <= this->heartbeat_tick) {
    return new_error(error_code::CONFIG_INVALID,
                     "election tick must be greater than heartbeat tick");
  }

  if (!this->storage.has_value()) {
    return new_error(error_code::CONFIG_INVALID, "storage cannot be null");
  }

  if (this->max_inflight_msgs <= 0) {
    return new_error(error_code::CONFIG_INVALID,
                     "max inflight messages must be greater than 0");
  }

  if (this->read_only_opt == read_only_option::READ_ONLY_LEASE_BASED &&
      !this->check_quorum) {
    return new_error(error_code::CONFIG_INVALID,
                     "CheckQuorum must be enabled when ReadOnlyOption is "
                     "ReadOnlyLeaseBased");
  }
  return {};
}