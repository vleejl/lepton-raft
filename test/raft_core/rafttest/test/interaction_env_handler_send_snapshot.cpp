#include <cassert>
#include <cstddef>

#include "data_driven.h"
#include "describe.h"
#include "interaction_env.h"
#include "leaf.hpp"
#include "logic_error.h"
#include "raft.pb.h"

namespace interaction {

lepton::leaf::result<void> interaction_env::handle_send_snapshot(const datadriven::test_data &test_data) {
  auto idxs = node_idxs(test_data);
  assert(idxs.size() == 2);
  return send_snapshot(idxs[0], idxs[1]);
}

// SendSnapshot sends a snapshot.
lepton::leaf::result<void> interaction_env::send_snapshot(std::size_t from_idx, std::size_t to_idx) {
  if (from_idx < 0 || from_idx >= nodes.size()) {
    return new_error(lepton::logic_error::INVALID_PARAM);
  }
  if (to_idx < 0 || to_idx >= nodes.size()) {
    return new_error(lepton::logic_error::INVALID_PARAM);
  }

  BOOST_LEAF_AUTO(snap, nodes[from_idx].storage->snapshot());
  auto from = from_idx + 1;
  auto to = to_idx + 1;
  raftpb::message msg;
  msg.set_type(::raftpb::message_type::MSG_SNAP);
  msg.set_from(from);
  msg.set_to(to);
  msg.set_term(nodes[from_idx].raw_node.basic_status().hard_state.term());
  msg.mutable_snapshot()->Swap(&snap);
  output->write_string(lepton::describe_message(msg, nullptr));
  messages.Add(std::move(msg));
  return {};
}

}  // namespace interaction