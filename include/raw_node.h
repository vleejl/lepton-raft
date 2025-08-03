#ifndef _LEPTON_RAW_NODE_H_
#define _LEPTON_RAW_NODE_H_
#include <error.h>

#include <cassert>
#include <vector>

#include "conf_change.h"
#include "leaf.hpp"
#include "lepton_error.h"
#include "node_interface.h"
#include "protobuf.h"
#include "raft.h"
#include "raft.pb.h"
#include "raft_error.h"
#include "state.h"
#include "types.h"
#include "utility_macros.h"
namespace lepton {

enum class progress_type {
  // ProgressTypePeer accompanies a Progress for a regular peer replica.
  PROGRESS_TYPE_PEER,
  // ProgressTypeLearner accompanies a Progress for a learner replica.
  PROGRESS_TYPE_LEARNER
};

class node;

// RawNode is a thread-unsafe Node.
// The methods of this struct correspond to the methods of Node and are described
// more fully there.
class raw_node {
  NOT_COPYABLE(raw_node)
  friend class node;

 public:
  raw_node(lepton::raft &&r, bool async_storage_writes)
      : raft_(std::move(r)),
        async_storage_writes_(async_storage_writes),
        prev_soft_state_(raft_.soft_state()),
        prev_hard_state_(raft_.hard_state()),
        steps_on_advance_() {}
  raw_node(raw_node &&) = default;

  auto async_storage_writes() const { return async_storage_writes_; }

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
    if (pb::is_local_msg(m.type()) && !pb::is_local_msg_target(m.from())) {
      return new_error(raft_error::STEP_LOCAL_MSG);
    }

    if (pb::is_response_msg(m.type()) && !pb::is_local_msg_target(m.from()) && !raft_.has_trk_progress(m.from())) {
      return new_error(raft_error::STEP_PEER_NOT_FOUND);
    }

    return raft_.step(std::move(m));
  }

  // Ready returns the outstanding work that the application needs to handle. This
  // includes appending and applying entries or a snapshot, updating the HardState,
  // and sending messages. The returned Ready() *must* be handled and subsequently
  // passed back via Advance().
  lepton::ready ready() {
    auto rd = ready_without_accept();
    accept_ready(rd);
    return rd;
  }

  // readyWithoutAccept returns a Ready. This is a read-only operation, i.e. there
  // is no obligation that the Ready must be handled.
  lepton::ready ready_without_accept() const;

  // acceptReady is called when the consumer of the RawNode has decided to go
  // ahead and handle a Ready. Nothing must alter the state of the RawNode between
  // this call and the prior call to Ready().
  void accept_ready(const lepton::ready &rd);

  // applyUnstableEntries returns whether entries are allowed to be applied once
  // they are known to be committed but before they have been written locally to
  // stable storage.
  bool apply_unstable_entries() const { return !async_storage_writes_; }

  // HasReady called when RawNode user need to check if any Ready pending.
  bool has_ready() const;

  // Advance notifies the RawNode that the application has applied and saved progress in the
  // last Ready results.
  //
  // NOTE: Advance must not be called when using AsyncStorageWrites. Response messages from
  // the local append and apply threads take its place.
  void advance(/*ready*/);

  // Status returns the current status of the given group. This allocates, see
  // BasicStatus and WithProgress for allocation-friendlier choices.
  lepton::status status() const { return raft_.get_status(); }

  // BasicStatus returns a BasicStatus. Notably this does not contain the
  // Progress map; see WithProgress for an allocation-free way to inspect it.
  lepton::basic_status basic_status() const { return raft_.get_basic_status(); }

  // WithProgress is a helper to introspect the Progress for this node and its
  // peers.
  template <typename F>
  requires std::invocable<F, uint64_t, progress_type, tracker::progress &>
  void with_progress(F &&f) {
    raft_.trk_.visit([&](std::uint64_t id, tracker::progress &pr) {
      auto type = pr.is_learner() ? progress_type::PROGRESS_TYPE_LEARNER : progress_type::PROGRESS_TYPE_PEER;
      auto p = pr.clone();
      p.mutable_inflights().reset();
      f(id, type, pr);
    });
  }

  // ReportUnreachable reports the given node is not reachable for the last send.
  void report_unreachable(std::uint64_t id) {
    raftpb::message msg;
    msg.set_from(id);
    raft_.step(std::move(msg));
  }

  // ReportSnapshot reports the status of the sent snapshot.
  void report_snapshot(std::uint64_t id, snapshot_status status) {
    auto rej = status == snapshot_status::SNAPSHOT_FAILURE;
    raftpb::message msg;
    msg.set_type(raftpb::message_type::MSG_SNAP_STATUS);
    msg.set_from(id);
    msg.set_reject(rej);
    raft_.step(std::move(msg));
  }

  // TransferLeader tries to transfer leadership to the given transferee.
  void transfer_leadership(std::uint64_t transferee) {
    raftpb::message msg;
    msg.set_type(raftpb::message_type::MSG_TRANSFER_LEADER);
    msg.set_from(transferee);
    raft_.step(std::move(msg));
  }

  // ForgetLeader forgets a follower's current leader, changing it to None.
  // See (Node).ForgetLeader for details.
  void forget_leader() {
    raftpb::message msg;
    msg.set_type(raftpb::message_type::MSG_FORGET_LEADER);
    raft_.step(std::move(msg));
  }

  // ReadIndex requests a read state. The read state will be set in ready.
  // Read State has a read index. Once the application advances further than the read
  // index, any linearizable read requests issued before the read request can be
  // processed safely. The read state will have the same rctx attached.
  void read_index(std::string &&rctx) {
    raftpb::message msg;
    msg.set_type(raftpb::message_type::MSG_READ_INDEX);
    *msg.mutable_entries()->Add()->mutable_data() = std::move(rctx);
    raft_.step(std::move(msg));
  }

  // Bootstrap initializes the RawNode for first use by appending configuration
  // changes for the supplied peers. This method returns an error if the Storage
  // is nonempty.
  //
  // It is recommended that instead of calling this method, applications bootstrap
  // their state manually by setting up a Storage that has a first index > 1 and
  // which stores the desired ConfState as its InitialState.
  leaf::result<void> bootstrap(std::vector<peer> &&peers);

// 为了方便单元测试 修改私有成员函数作用域
#ifdef LEPTON_TEST
 public:
#else
 private:
#endif
  lepton::raft raft_;
  bool async_storage_writes_;

  // Mutable fields.
  soft_state prev_soft_state_;
  raftpb::hard_state prev_hard_state_;
  lepton::pb::repeated_message steps_on_advance_;
};

// NewRawNode instantiates a RawNode from the given configuration.
//
// See Bootstrap() for bootstrapping an initial state; this replaces the former
// 'peers' argument to this method (with identical behavior). However, It is
// recommended that instead of calling Bootstrap, applications bootstrap their
// state manually by setting up a Storage that has a first index > 1 and which
// stores the desired ConfState as its InitialState.
leaf::result<raw_node> new_raw_node(config &&c);

}  // namespace lepton

#endif  // _LEPTON_RAW_NODE_H_
