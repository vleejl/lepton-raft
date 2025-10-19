#include <cstddef>
#include <cstdint>

#include "data_driven.h"
#include "interaction_env.h"
#include "log.h"
#include "logic_error.h"

namespace interaction {

lepton::leaf::result<void> interaction_env::handle_transfer_leadership(const datadriven::test_data &test_data) {
  std::uint64_t from = 0;
  std::uint64_t to = 0;
  datadriven::scan_first_args(test_data, "from", from);
  datadriven::scan_first_args(test_data, "to", to);
  if (from == 0 || from > nodes.size()) {
    LEPTON_CRITICAL("expected valid \"from\" argument");
    return new_error(lepton::logic_error::INVALID_PARAM);
  }
  if (to == 0 || to > nodes.size()) {
    LEPTON_CRITICAL("expected valid \"to\" argument");
    return new_error(lepton::logic_error::INVALID_PARAM);
  }
  return transfer_leadership(static_cast<std::size_t>(from), static_cast<std::size_t>(to));
}

lepton::leaf::result<void> interaction_env::transfer_leadership(std::size_t from, std::size_t to) {
  auto from_idx = from - 1;
  nodes[from_idx].raw_node.transfer_leadership(to);
  return {};
}

}  // namespace interaction