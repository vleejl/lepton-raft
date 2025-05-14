#include "restore.h"

#include <absl/types/span.h>

#include <functional>
#include <vector>

#include "confchange.h"
#include "leaf.hpp"
#include "types.h"
namespace lepton {

namespace confchange {

raftpb::conf_change_single create_conf_change_single(raftpb::conf_change_type type, uint64_t node_id) {
  raftpb::conf_change_single conf;
  conf.set_node_id(node_id);
  conf.set_type(type);
  return conf;
}

// toConfChangeSingle translates a conf state into 1) a slice of operations creating first the config that will become
// the outgoing one, and then the incoming one, and b) another slice that, when applied to the config resulted from 1),
// represents the ConfState.
std::tuple<pb::repeated_conf_change, pb::repeated_conf_change> to_conf_change_single(const raftpb::conf_state &cs) {
  // Example to follow along this code:
  // voters=(1 2 3) learners=(5) outgoing=(1 2 4 6) learners_next=(4)
  //
  // This means that before entering the joint config, the configuration
  // had voters (1 2 4 6) and perhaps some learners that are already gone.
  // The new set of voters is (1 2 3), i.e. (1 2) were kept around, and (4 6)
  // are no longer voters; however 4 is poised to become a learner upon leaving
  // the joint state.
  // We can't tell whether 5 was a learner before entering the joint config,
  // but it doesn't matter (we'll pretend that it wasn't).
  //
  // The code below will construct
  // outgoing = add 1; add 2; add 4; add 6
  // incoming = remove 1; remove 2; remove 4; remove 6
  //            add 1;    add 2;    add 3;
  //            add-learner 5;
  //            add-learner 4;
  //
  // So, when starting with an empty config, after applying 'outgoing' we have
  //
  //   quorum=(1 2 4 6)
  //
  // From which we enter a joint state via 'incoming'
  //
  //   quorum=(1 2 3)&&(1 2 4 6) learners=(5) learners_next=(4)
  //
  // as desired.
  pb::repeated_conf_change out;
  out.Reserve(cs.voters_outgoing_size());
  for (const auto &id : cs.voters_outgoing()) {
    // If there are outgoing voters, first add them one by one so that the
    // (non-joint) config has them all.
    out.Add(create_conf_change_single(raftpb::conf_change_type::CONF_CHANGE_ADD_NODE, id));
  }

  // We're done constructing the outgoing slice, now on to the incoming one
  // (which will apply on top of the config created by the outgoing slice).

  pb::repeated_conf_change in;
  auto in_size = cs.voters_outgoing_size() + cs.voters_size() + cs.learners_size() + cs.learners_next_size();
  in.Reserve(in_size);
  // First, we'll remove all of the outgoing voters.
  for (const auto &id : cs.voters_outgoing()) {
    in.Add(create_conf_change_single(raftpb::conf_change_type::CONF_CHANGE_REMOVE_NODE, id));
  }
  // Then we'll add the incoming voters and learners.
  for (const auto &id : cs.voters()) {
    in.Add(create_conf_change_single(raftpb::conf_change_type::CONF_CHANGE_ADD_NODE, id));
  }
  for (const auto &id : cs.learners()) {
    in.Add(create_conf_change_single(raftpb::conf_change_type::CONF_CHANGE_ADD_LEARNER_NODE, id));
  }
  // Same for LearnersNext; these are nodes we want to be learners but which
  // are currently voters in the outgoing config.
  for (const auto &id : cs.learners_next()) {
    in.Add(create_conf_change_single(raftpb::conf_change_type::CONF_CHANGE_ADD_LEARNER_NODE, id));
  }
  return {out, in};
}

changer::result restor(const raftpb::conf_state &cs, changer &&chg) {
  auto [outgoing, incoming] = to_conf_change_single(cs);
  std::vector<std::function<changer::result(changer &)>> ops;
  if (outgoing.empty()) {
    // No outgoing config, so just apply the incoming changes one by one.
    auto incoming_span = absl::MakeSpan(incoming);
    for (auto &cc : incoming_span) {
      ops.push_back([&cc](const changer &ch) -> changer::result {
        auto span = absl::MakeSpan(&cc, 1);
        return ch.simple(span);
      });
    }
  } else {
    // The ConfState describes a joint configuration.
    //
    // First, apply all of the changes of the outgoing config one by one, so
    // that it temporarily becomes the incoming active config. For example,
    // if the config is (1 2 3)&(2 3 4), this will establish (2 3 4)&().
    auto outgoing_span = absl::MakeSpan(outgoing);
    for (auto &cc : outgoing_span) {
      ops.push_back([&cc](const changer &ch) -> changer::result {
        auto span = absl::MakeSpan(&cc, 1);
        return ch.simple(span);
      });
    }
    // Now enter the joint state, which rotates the above additions into the
    // outgoing config, and adds the incoming config in. Continuing the
    // example above, we'd get (1 2 3)&(2 3 4), i.e. the incoming operations
    // would be removing 2,3,4 and then adding in 1,2,3 while transitioning
    // into a joint state.
    ops.push_back([&incoming](const changer &ch) -> changer::result {
      auto span = absl::MakeSpan(incoming);
      return ch.simple(span);
    });
  }

  for (const auto &op : ops) {
    BOOST_LEAF_AUTO(v, op(chg));
    auto &[cfg, prs] = v;
    chg.update_tracker_config(std::move(cfg));
    chg.update_tracker_progress(std::move(prs));
  }
  return {std::move(chg.move_config()), std::move(chg.move_progress())};
}

}  // namespace confchange

}  // namespace lepton
