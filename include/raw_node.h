#ifndef _LEPTON_RAW_NODE_H_
#define _LEPTON_RAW_NODE_H_
#include <error.h>

#include "conf_change.h"
#include "leaf.hpp"
#include "lepton_error.h"
#include "protobuf.h"
#include "raft.h"
#include "raft.pb.h"
#include "raft_error.h"
#include "state.h"
#include "types.h"
#include "utility_macros.h"
namespace lepton {

class raw_node;

// NewRawNode instantiates a RawNode from the given configuration.
//
// See Bootstrap() for bootstrapping an initial state; this replaces the former
// 'peers' argument to this method (with identical behavior). However, It is
// recommended that instead of calling Bootstrap, applications bootstrap their
// state manually by setting up a Storage that has a first index > 1 and which
// stores the desired ConfState as its InitialState.
leaf::result<raw_node> new_raw_node(config &&c);

// RawNode is a thread-unsafe Node.
// The methods of this struct correspond to the methods of Node and are described
// more fully there.
class raw_node {
  NOT_COPYABLE(raw_node);

 public:
  raw_node(lepton::raft &&r, bool async_storage_writes)
      : raft_(std::move(r)),
        async_storage_writes_(async_storage_writes),
        prev_soft_state_(raft_.soft_state()),
        prev_hard_state_(raft_.hard_state()),
        steps_on_advance_() {}
  raw_node(raw_node &&) = default;

  // Tick advances the internal logical clock by a single tick.
  void tick() { raft_.tick(); }

  // Campaign causes this RawNode to transition to candidate state.
  leaf::result<void> campaign() {
    raftpb::message m;
    m.set_type(raftpb::message_type::MSG_HUP);
    return raft_.step(std::move(m));
  }

  // Propose proposes data be appended to the raft log.
  leaf::result<void> propose(std::string &&data) {
    raftpb::message m;
    m.set_type(raftpb::message_type::MSG_PROP);
    *m.add_entries()->mutable_data() = std::move(data);
    return raft_.step(std::move(m));
  }

  // ProposeConfChange proposes a config change. See (Node).ProposeConfChange for
  // details.
  leaf::result<void> propose_conf_change(const pb::conf_change_var &cc) {
    BOOST_LEAF_AUTO(m, pb::conf_change_to_message(cc));
    return raft_.step(std::move(m));
  }

  // ApplyConfChange applies a config change to the local node. The app must call
  // this when it applies a configuration change, except when it decides to reject
  // the configuration change, in which case no call must take place.
  raftpb::conf_state apply_conf_change(raftpb::conf_change_v2 &&cc) { return raft_.apply_conf_change(std::move(cc)); }

  // Step advances the state machine using the given message.
  leaf::result<void> step(raftpb::message &&m) {
    // Ignore unexpected local messages receiving over network.
    const auto msg_type = m.type();
    const auto msg_from = m.from();
    if (pb::is_local_msg(msg_type) && !pb::is_local_msg_target(msg_from)) {
      return new_error(raft_error::STEP_LOCAL_MSG);
    }
    if (pb::is_response_msg(msg_type) && !pb::is_local_msg_target(msg_from)) {
      return new_error(raft_error::STEP_PEER_NOT_FOUND);
    }
    return raft_.step(std::move(m));
  }

 private:
  lepton::raft raft_;
  bool async_storage_writes_;

  // Mutable fields.
  soft_state prev_soft_state_;
  raftpb::hard_state prev_hard_state_;
  lepton::pb::repeated_message steps_on_advance_;
};

}  // namespace lepton

#endif  // _LEPTON_RAW_NODE_H_
