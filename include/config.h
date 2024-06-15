#ifndef _LEPTON_CONFIG_H_
#define _LEPTON_CONFIG_H_
#include <cstdint>

namespace lepton {

// None is a placeholder node ID used when there is no leader.
constexpr std::uint64_t NONE = 0;

enum class read_only_option : int { READ_ONLY_SAFE, READ_ONLY_LEASE_BASED };

// Config contains the parameters to start a raft.
struct config {
  // ID is the identity of the local raft. ID cannot be 0.
  std::uint64_t id;

  // ElectionTick is the number of Node.Tick invocations that must pass between
  // elections. That is, if a follower does not receive any message from the
  // leader of current term before ElectionTick has elapsed, it will become
  // candidate and start an election. ElectionTick must be greater than
  // HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
  // unnecessary leader switching.
  int election_tick;
  // HeartbeatTick is the number of Node.Tick invocations that must pass between
  // heartbeats. That is, a leader sends heartbeat messages to maintain its
  // leadership every HeartbeatTick ticks.
  int heatbeat_tick;

  // Applied is the last applied index. It should only be set when restarting
  // raft. raft will not return entries to the application smaller or equal to
  // Applied. If Applied is unset when restarting, raft might return previous
  // applied entries. This is a very application dependent configuration.
  std::uint64_t applied_index;

  // MaxSizePerMsg limits the max byte size of each append message. Smaller
  // value lowers the raft recovery cost(initial probing and message lost
  // during normal operation). On the other side, it might affect the
  // throughput during normal replication. Note: math.MaxUint64 for unlimited,
  // 0 for at most one entry per message.
  std::uint64_t max_size_per_msg;

  // MaxCommittedSizePerReady limits the size of the committed entries which
  // can be applied.
  std::uint64_t max_committed_size_per_ready;
  // MaxUncommittedEntriesSize limits the aggregate byte size of the
  // uncommitted entries that may be appended to a leader's log. Once this
  // limit is exceeded, proposals will begin to return ErrProposalDropped
  // errors. Note: 0 for no limit.
  std::uint64_t max_uncommitted_entries_size;
  // MaxInflightMsgs limits the max number of in-flight append messages during
  // optimistic replication phase. The application transportation layer usually
  // has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
  // overflowing that sending buffer. TODO (xiangli): feedback to application to
  // limit the proposal rate?
  int max_inflight_Msgs;

  // CheckQuorum specifies if the leader should check quorum activity. Leader
  // steps down when quorum is not active for an electionTimeout.
  bool check_quorum;

  // ReadOnlyOption specifies how the read only request is processed.
  //
  // ReadOnlySafe guarantees the linearizability of the read only request by
  // communicating with the quorum. It is the default and suggested option.
  //
  // ReadOnlyLeaseBased ensures linearizability of the read only request by
  // relying on the leader lease. It can be affected by clock drift.
  // If the clock drift is unbounded, leader might keep the lease longer than it
  // should (clock can move backward/pause without any bound). ReadIndex is not
  // safe in that case. CheckQuorum MUST be enabled if ReadOnlyOption is
  // ReadOnlyLeaseBased.
  read_only_option read_only_opt;

  // DisableProposalForwarding set to true means that followers will drop
  // proposals, rather than forwarding them to the leader. One use case for
  // this feature would be in a situation where the Raft leader is used to
  // compute the data of a proposal, for example, adding a timestamp from a
  // hybrid logical clock to data in a monotonically increasing way. Forwarding
  // should be disabled to prevent a follower with an inaccurate hybrid
  // logical clock from assigning the timestamp and then forwarding the data
  // to the leader.
  bool disable_proposal_forwarding;
};
} // namespace lepton
#endif // _LEPTON_CONFIG_H_