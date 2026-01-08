#include <absl/strings/match.h>

#include <cstddef>

#include "error/error.h"
#include "error/logic_error.h"
#include "fmt/format.h"
#include "interaction_env.h"
#include "interaction_env_logger.h"

namespace interaction {
lepton::leaf::result<void> interaction_env::handle_log_level(const datadriven::test_data &test_data) {
  assert(!test_data.cmd_args.empty());
  return log_level(test_data.cmd_args[0].key_);
}

lepton::leaf::result<void> interaction_env::log_level(const std::string &name) {
  for (std::size_t i = 0; i < redirect_logger::lvl_names.size(); ++i) {
    if (absl::EqualsIgnoreCase(redirect_logger::lvl_names[i], name)) {
      output->set_level(static_cast<redirect_logger::level>(i));
      return {};
    }
  }
  return lepton::new_error(lepton::logic_error::INVALID_PARAM, fmt::format("log levels must be either of {}", name));
}

}  // namespace interaction