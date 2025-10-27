#include <cassert>
#include <cstddef>

#include "data_driven.h"
#include "interaction_env.h"
#include "leaf.h"
#include "logic_error.h"

namespace interaction {

lepton::leaf::result<void> interaction_env::handle_propose_conf_change(const datadriven::test_data &test_data) {
  std::size_t idx = first_as_node_idx(test_data);
  bool v1 = false;
  auto transition = raftpb::conf_change_transition::CONF_CHANGE_TRANSITION_AUTO;
  assert(test_data.cmd_args.size() >= 1);
  for (std::size_t i = 1; i < test_data.cmd_args.size(); ++i) {
    const auto &arg = test_data.cmd_args[i];
    for (std::size_t j = 0; j < arg.vals_.size(); ++j) {
      const auto &val = arg.vals_[j];
      if (arg.key_ == "v1") {
        LEPTON_LEAF_CHECK(handle_bool(val, v1));
      } else if (arg.key_ == "transition") {
        if (val == "auto") {
          transition = raftpb::conf_change_transition::CONF_CHANGE_TRANSITION_AUTO;
        } else if (val == "implicit") {
          transition = raftpb::conf_change_transition::CONF_CHANGE_TRANSITION_JOINT_IMPLICIT;
        } else if (val == "explicit") {
          transition = raftpb::conf_change_transition::CONF_CHANGE_TRANSITION_JOINT_EXPLICIT;
        } else {
          return lepton::new_error(lepton::logic_error::INVALID_PARAM,
                                   fmt::format("invalid transition value: {}", val));
        }
      } else {
        return lepton::new_error(lepton::logic_error::INVALID_PARAM, fmt::format("unknown command {}", arg.key_));
      }
    }
  }

  BOOST_LEAF_AUTO(ccs, lepton::pb::conf_changes_from_string(test_data.input));

  lepton::pb::conf_change_var cc;
  if (v1) {
    if (ccs.size() > 1 || transition != raftpb::conf_change_transition::CONF_CHANGE_TRANSITION_AUTO) {
      return lepton::new_error(lepton::logic_error::INVALID_PARAM,
                               "v1 conf change cannot have multiple changes or non-auto transition");
    }
    raftpb::conf_change c;
    c.set_type(ccs.Get(0).type());
    c.set_node_id(ccs.Get(0).node_id());
    cc = c;
  } else {
    raftpb::conf_change_v2 c;
    c.mutable_changes()->Swap(&ccs);
    c.set_transition(transition);
    cc = c;
  }
  return propose_conf_change(idx, cc);
}

// ProposeConfChange proposes a configuration change on the node with the given index.
lepton::leaf::result<void> interaction_env::propose_conf_change(std::size_t node_idx,
                                                                const lepton::pb::conf_change_var &cc) {
  if (node_idx < 0 || node_idx >= nodes.size()) {
    return new_error(lepton::logic_error::INVALID_PARAM);
  }
  nodes[node_idx].raw_node.propose_conf_change(cc);
  return {};
}

}  // namespace interaction