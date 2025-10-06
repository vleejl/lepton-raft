#include <cassert>
#include <vector>

#include "interaction_env.h"
#include "test_utility_data.h"

namespace interaction {
std::string interaction_env::handle(const datadriven::test_data &test_data) {
  auto &cmd = test_data.cmd;
  // This is a helper case to attach a debugger to when a problem needs
  // to be investigated in a longer test file. In such a case, add the
  // following stanza immediately before the interesting behavior starts:
  //
  // _breakpoint:
  // ----
  // ok
  //
  // and set a breakpoint on the `case` above.
  if (cmd == "_breakpoint") {
    // used for debug
  } else if (cmd == "add-nodes") {
    // Example:
    //
    // add-nodes <number-of-nodes-to-add> voters=(1 2 3) learners=(4 5) index=2 content=foo async-storage-writes=true
  }
}

int first_as_int(const datadriven::test_data &test_data) {
  auto result = safe_stoull(test_data.cmd_args.front().key_);
  assert(result.has_value());
  return static_cast<int>(*result);
}

int first_as_node_idx(const datadriven::test_data &test_data) {
  auto n = first_as_int(test_data);
  return n - 1;
}

std::vector<int> node_idxs(const datadriven::test_data &test_data) {
  std::vector<int> ints;
  for (auto &item : test_data.cmd_args) {
    if (!item.vals_.empty()) {
      continue;
    }
    auto result = safe_stoull(item.key_);
    assert(result.has_value());
    ints.push_back(static_cast<int>(*result) - 1);
  }
  return ints;
}
}  // namespace interaction
