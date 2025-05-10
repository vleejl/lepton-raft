#ifndef _LEPTON_RAFT_H_
#define _LEPTON_RAFT_H_

#include <cstdint>
#include <tuple>
#include <vector>

#include "config.h"
#include "error.h"
#include "progress.h"
#include "protobuf.h"
#include "quorum.h"
#include "raft_log.h"
#include "read_only.h"
#include "status.h"
#include "tracker.h"
#include "types.h"
#include "utility_macros.h"
namespace lepton {

enum class campaign_type {
  CAMPAIGN_PRE_ELECTION,
  CAMPAIGN_ELECTION,
  CAMPAIGN_TRANSFER,  // 强制领导者转移
};

class raft;

using tick_func = std::function<void()>;
using step_func = std::function<leaf::result<void>(raft&, raftpb::message&&)>;
leaf::result<raft> new_raft(const config&);
void release_pending_read_index_message(raft& r);
void send_msg_read_index_response(raft& r, raftpb::message&& m);
leaf::result<void> step_leader(raft& r, raftpb::message&& m);
// stepCandidate is shared by StateCandidate and StatePreCandidate; the
// difference is whether they respond to MsgVoteResp or MsgPreVoteResp.
leaf::result<void> step_candidate(raft& r, raftpb::message&& m);
leaf::result<void> step_follower(raft& r, raftpb::message&& m);

class raft {
 private:
  NOT_COPYABLE(raft)
  friend leaf::result<raft> new_raft(const config&);
  friend void release_pending_read_index_message(raft& r);
  friend void send_msg_read_index_response(raft& r, raftpb::message&& m);
  friend leaf::result<void> step_leader(raft& r, raftpb::message&& m);
  friend leaf::result<void> step_candidate(raft& r, raftpb::message&& m);
  friend leaf::result<void> step_follower(raft& r, raftpb::message&& m);

  void load_state(const raftpb::hard_state& state);

  bool past_election_timeout();

  // send schedules persisting state to a stable storage and AFTER that
  // sending the message (as part of next Ready message processing).
  void send(raftpb::message&& message);

  void hup(campaign_type t);

  bool has_unapplied_conf_change() const;

  void handle_append_entries(raftpb::message&& message);

  void handle_heartbeat(raftpb::message&& message);

  void handle_snapshot(raftpb::message&& message);

  // restore recovers the state machine from a snapshot. It restores the log and the
  // configuration of state machine. If this method returns false, the snapshot was
  // ignored, either because it was obsolete or because of an error.
  bool restore(raftpb::snapshot&& snapshot);

  // maybeSendAppend sends an append RPC with new entries to the given peer,
  // if necessary. Returns true if a message was sent. The sendIfEmpty
  // argument controls whether messages with no entries will be sent
  // ("empty" messages are useful to convey updated Commit indexes, but
  // are undesirable when we're sending multiple messages in a batch).
  //
  // TODO(pav-kv): make invocation of maybeSendAppend stateless. The Progress
  // struct contains all the state necessary for deciding whether to send a
  // message.
  bool maybe_send_append(std::uint64_t id, bool send_if_empty);

  // maybeSendSnapshot fetches a snapshot from Storage, and sends it to the given
  // node. Returns true iff the snapshot message has been emitted successfully.
  bool maybe_send_snapshot(std::uint64_t to, tracker::progress& pr);

  // sendAppend sends an append RPC with new entries (if any) and the
  // current commit index to the given peer
  void send_append(std::uint64_t id);

  // sendHeartbeat sends a heartbeat RPC to the given peer.
  void send_heartbeat(std::uint64_t id, std::string&& ctx);

  // bcastAppend sends RPC, with entries to all peers that are not up-to-date
  // according to the progress recorded in r.prs.
  void bcast_append();

  // bcastHeartbeat sends RPC, without entries to all the peers.
  void bcast_heartbeat();

  void bcast_heartbeat_with_ctx(std::string&& ctx);

  void applied_to(std::uint64_t index, pb::entry_encoding_size size);

  void applied_snap(const raftpb::snapshot& snapshot);

  // maybeCommit attempts to advance the commit index. Returns true if
  // the commit index changed (in which case the caller should call
  // r.bcastAppend).
  bool maybe_commit();

  bool append_entry(pb::repeated_entry&& entries);

  void tick_election();

  // tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
  void tick_heartbeat();

  void reset(std::uint64_t term);

  void send_timmeout_now(std::uint64_t id);

  void abort_leader_transfer();

  // switchToConfig reconfigures this node to use the provided configuration. It
  // updates the in-memory state and, when necessary, carries out additional
  // actions such as reacting to the removal of nodes or changed quorum
  // requirements.
  //
  // The inputs usually result from restoring a ConfState or applying a
  // ConfChange.
  raftpb::conf_state switch_to_config(tracker::config&& cfg, tracker::progress_map&& pgs_map);

  // promotable indicates whether state machine can be promoted to leader,
  // which is true when its own id is in progress list.
  bool promotable();

  // campaign transitions the raft instance to candidate state.
  // This must only be called after verifying that this is a legitimate transition.
  void campaign(campaign_type t);

  std::tuple<std::uint64_t, std::uint64_t, quorum::vote_result> poll(std::uint64_t id, raftpb::message_type vt,
                                                                     bool vote);

  void reset_randomized_election_timeout();

  // committedEntryInCurrentTerm return true if the peer has committed an entry in its term.
  bool committed_entry_in_current_term() const;

  // responseToReadIndexReq constructs a response for `req`. If `req` comes from the peer
  // itself, a blank value will be returned.
  raftpb::message response_to_read_index_req(raftpb::message&& req, std::uint64_t read_index);

  // increaseUncommittedSize computes the size of the proposed entries and
  // determines whether they would push leader over its maxUncommittedSize limit.
  // If the new entries would exceed the limit, the method returns false. If not,
  // the increase in uncommitted entry size is recorded and the method returns
  // true.
  //
  // Empty payloads are never refused. This is used both for appending an empty
  // entry at a new leader's term, as well as leaving a joint configuration.
  bool increase_uncommitted_size(const pb::repeated_entry& entries);

  // reduceUncommittedSize accounts for the newly committed entries by decreasing
  // the uncommitted entry size limit.
  void reduce_uncommitted_size(pb::entry_encoding_size size);

 public:
  //  字段初始化顺序和etcd-raft 一致
  raft(std::uint64_t id, raft_log&& raft_log_handle, std::uint64_t max_size_per_msg,
       std::uint64_t max_uncommitted_entries_size, std::size_t max_inflight_msgs, std::uint64_t max_inflight_bytes,
       int election_tick, int heartbeat_tick, bool check_quorum, bool pre_vote, read_only_option read_only_opt,
       bool disable_proposal_forwarding, bool disable_conf_change_validation)
      : id_(id),
        raft_log_handle_(std::move(raft_log_handle)),
        max_msg_size_(max_size_per_msg),
        max_uncommitted_size_(max_uncommitted_entries_size),
        trk_(tracker::progress_tracker{max_inflight_msgs, max_inflight_bytes}),
        is_learner_(false),
        lead_(NONE),
        disable_conf_change_validation_(disable_conf_change_validation),
        read_only_(read_only_opt),
        check_quorum_(check_quorum),
        pre_vote_(pre_vote),
        heartbeat_timeout_(heartbeat_tick),
        election_timeout_(election_tick),
        disable_proposal_forwarding_(disable_proposal_forwarding) {}
  raft(raft&&) = default;

  void become_candidate();

  void become_pre_candidate();

  void become_leader();

  void become_follower(std::uint64_t term, std::uint64_t lead);

  leaf::result<void> step(raftpb::message&& m);

 private:
  // 对应config 配置里的 id，表示唯一一个raft 节点
  const std::uint64_t id_ = NONE;

  // 当前节点所处的任期号。Raft 协议通过任期号来避免旧日志覆盖新日志。当 term
  // 发生变化时，Raft 会重新选举领导者。
  std::uint64_t term_ = 0;

  // 当前节点上次投票的目标节点 ID。在选举过程中，每个节点会投票给其他节点，vote
  // 保存的是当前节点投票的目标节点 ID。每个节点每个任期只能投一次票。
  std::uint64_t vote_id_ = 0;

  // 存储节点的只读请求的状态。通常在 Raft
  // 协议中，如果客户端请求一个不修改日志的操作（例如读取），这个字段会保存这些请求的状态。用于管理客户端的读请求。
  std::vector<read_state> read_states_;

  // raftLog 保存了 Raft 协议的所有日志条目，它是 Raft
  // 协议的核心部分之一。日志条目是 Raft 节点进行日志复制和提交的基础。
  raft_log raft_log_handle_;

  // 节点可以发送的最大消息大小。用于限制 Raft
  // 节点发送的消息的大小，避免单个消息过大导致性能下降或网络问题。
  const std::uint64_t max_msg_size_ = 0;
  // 表示尚未提交的日志条目的最大大小。这个字段有助于控制日志条目的大小，防止在日志还没有被提交的情况下占用过多的内存。
  const std::uint64_t max_uncommitted_size_ = 0;

  // 跟踪所有 Raft 节点的进度。ProgressTracker
  // 是一个用于跟踪节点在复制日志过程中的进度（如已复制的日志条目）。它通常会跟踪每个节点的日志索引、已提交的日志索引等。
  tracker::progress_tracker trk_;

  // 当前节点 raft 状态
  state_type state_type_;

  // isLearner is true if the local raft node is a learner.
  bool is_learner_ = false;

  // 存储当前节点待发送的消息列表。Raft
  // 协议中的节点之间会交换消息（例如投票请求、心跳等）。该字段用于存储待发送的消息。
  pb::repeated_message msgs_;

  // the leader id
  std::uint64_t lead_ = 0;

  // leadTransferee is id of the leader transfer target when its value is not
  // zero. Follow the procedure defined in raft thesis 3.10.
  // 领导者转移目标节点的 ID。在 Raft
  // 协议中，当需要进行领导者转移时，leadTransferee 会保存目标节点的
  // ID，表明转移目标。
  std::uint64_t leader_transferee_ = NONE;

  // Only one conf change may be pending (in the log, but not yet
  // applied) at a time. This is enforced via pendingConfIndex, which
  // is set to a value >= the log index of the latest pending
  // configuration change (if any). Config changes are only allowed to
  // be proposed if the leader's applied index is greater than this
  // value.
  // 记录正在等待应用的配置变更的日志索引。Raft
  // 协议中，配置变更（例如添加或删除节点）是通过日志条目进行的。这个字段确保一次只能有一个配置变更被提交。
  std::uint64_t pending_conf_index_ = 0;

  // disableConfChangeValidation is Config.DisableConfChangeValidation,
  // see there for details.
  bool disable_conf_change_validation_ = false;

  // an estimate of the size of the uncommitted tail of the Raft log. Used to
  // prevent unbounded log growth. Only maintained by the leader. Reset on
  // term changes.
  // 估计的尚未提交的日志条目的大小。这个字段由领导者节点维护，并且随着新的日志条目被添加而增加。它用于控制日志的大小，防止日志条目在未提交前无限增长。
  std::uint64_t uncommitted_size_ = 0;

  // readOnly
  // 管理客户端的只读请求，确保在节点转变角色时能处理这些请求。它用于存储只读操作的相关信息和状态。
  read_only read_only_;

  // number of ticks since it reached last electionTimeout when it is leader
  // or candidate.
  // number of ticks since it reached last electionTimeout or received a
  // valid message from current leader when it is a follower.
  // 自上次选举超时以来的时间（以 tick 个数为单位）。
  // 该字段用于控制选举超时机制。如果一个节点在选举超时之前没有接收到选票，它就会启动新的选举。
  int election_elapsed_ = 0;

  // number of ticks since it reached last heartbeatTimeout.
  // only leader keeps heartbeatElapsed.
  // 自上次心跳发送以来的时间（以 tick 个数为单位）。
  // 领导者节点会定期发送心跳消息给其他节点，以防止其他节点启动选举。
  int heartbeat_elapsed_ = 0;

  // 在 Raft 协议中，checkQuorum
  // 用于控制是否需要检查选举是否达成法定人数的支持。如果 checkQuorum 为
  // true，则只有在满足法定人数时选举才算有效。
  const bool check_quorum_ = false;
  // 是否启用预选举。在 Raft
  // 协议的扩展中，为了优化选举过程，增加了预选举机制。在预选举中，节点会在正式投票前先进行一次投票尝试，preVote
  // 表示是否启用这个机制。
  const bool pre_vote_ = false;

  // 心跳超时的值。领导者节点定期发送心跳，防止其他节点启动选举。heartbeatTimeout
  // 控制心跳的超时设置。
  const int heartbeat_timeout_ = 0;
  // 选举超时的值。每个 Raft
  // 节点都会设置一个选举超时值，当超过此时间后，节点如果没有收到来自领导者的消息，会开始启动新的选举过程。
  const int election_timeout_ = 0;

  // randomizedElectionTimeout is a random number between
  // [electiontimeout, 2 * electiontimeout - 1]. It gets reset
  // when raft changes its state to follower or candidate.
  // 随机化的选举超时值。在实际使用中，Raft
  // 节点的选举超时值是一个范围内的随机数，避免所有节点同时启动选举。randomizedElectionTimeout
  // 保存的是随机化后的超时值。
  int randomized_election_timeout_ = 0;

  // 是否禁用提案转发。如果设置为
  // true，节点将不会将提案（例如日志条目）转发给其他节点。该字段用于某些特殊场景下的控制。
  const bool disable_proposal_forwarding_ = false;

  // 一个函数指针，表示定时器事件的处理函数。tick
  // 可能被用来控制心跳、选举超时等周期性事件。
  tick_func tick_func_;
  // 用于处理接收到的消息。step 函数是 Raft
  // 协议中的消息处理函数，根据不同的消息类型（如投票请求、日志条目等）执行相应的操作。
  step_func step_func_;

  // pendingReadIndexMessages is used to store messages of type MsgReadIndex
  // that can't be answered as new leader didn't committed any log in
  // current term. Those will be handled as fast as first log is committed in
  // current term.
  // 待处理的读取索引消息。当有客户端请求读取日志时，pendingReadIndexMessages
  // 保存这些消息，直到领导者节点确认日志条目已提交后再进行响应。
  pb::repeated_message pending_read_index_messages_;
};

}  // namespace lepton

#endif  // _LEPTON_RAFT_H_
