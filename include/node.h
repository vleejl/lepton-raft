#ifndef _LEPTON_NODE_
#define _LEPTON_NODE_
#include <absl/types/span.h>
#include <proxy.h>
#include <raft.pb.h>

#include <cstddef>
#include <memory>
#include <vector>

#include "channel.h"
#include "error.h"
#include "read_only.h"
#include "state.h"
#include "utility_macros.h"
// Node represents a node in a raft cluster.
namespace lepton {

enum class snapshot_status : int {
  SNAPSHOT_FINISH = 1,
  SNAPSHOT_FAILURE = 2,
};
// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
struct ready {
  MOVABLE_BUT_NOT_COPYABLE(ready)
  // The current volatile state of a Node.
  // SoftState will be nil if there is no update.
  // It is not required to consume or store SoftState.
  // 非持久化状态，可能为空
  // 代表节点的易变状态（volatile state），例如 Leader 是谁、当前 Raft 节点的
  // State（如 Follower、Candidate、Leader）。
  // 由于 SoftState 不是持久化数据，因此不需要存储，只用于节点间的交互。
  // 如果没有更新，它会是 nil。
  std::optional<soft_state> current_soft_state;

  // The current state of a Node to be saved to stable storage BEFORE
  // Messages are sent.
  // HardState will be equal to empty state if there is no update.
  // 代表持久化的状态，包括
  // Term（当前任期）、Vote（投票给谁）、Commit（已提交的日志索引）。
  // 需要在发送消息之前保存到稳定存储（如磁盘）。
  raftpb::hard_state hard_state;

  // ReadStates can be used for node to serve linearizable read requests locally
  // when its applied index is greater than the index in ReadState.
  // Note that the readState will be returned when raft receives msgReadIndex.
  // The returned is only valid for the request that requested to read.
  // 线性一致性读相关状态
  // 存储了当前节点可以用于**本地线性一致性读（linearizable read）**的状态。
  // 当 Raft 节点收到 msgReadIndex 消息后，会返回
  // ReadState，应用层可以用它来服务读请求。
  // 只有发起该读请求的客户端才能使用这个 ReadState。
  std::vector<read_state> read_states;

  // Entries specifies entries to be saved to stable storage BEFORE
  // Messages are sent.
  // 需要在发送消息之前存储到稳定存储（如磁盘）的日志条目（未提交）。
  // 这些日志是新的，还没有被提交，但应该被存储。
  std::vector<raftpb::entry> entries;

  // Snapshot specifies the snapshot to be saved to stable storage.
  // 如果存在快照数据，需要存储到稳定存储。
  // 这个字段用于快照恢复，如果没有新的快照，该字段为空。
  raftpb::snapshot snapshot;

  // CommittedEntries specifies entries to be committed to a
  // store/state-machine. These have previously been committed to stable
  // store.
  // 已经提交的日志条目，需要应用到状态机（store/state-machine）。
  // 这些日志之前已经被存储到稳定存储，只是还没有执行。
  std::vector<raftpb::entry> committed_entries;

  // Messages specifies outbound messages to be sent AFTER Entries are
  // committed to stable storage.
  // If it contains a MsgSnap message, the application MUST report back to raft
  // when the snapshot has been received or has failed by calling
  // ReportSnapshot.
  // 需要发送到其他 Raft 节点的消息，但必须在日志条目（Entries）持久化后发送。
  // 如果其中包含 MsgSnap（快照消息），应用层必须在快照被接收或失败后调用
  // ReportSnapshot 进行反馈。
  std::vector<raftpb::message> messages;

  // MustSync indicates whether the HardState and Entries must be synchronously
  // written to disk or if an asynchronous write is permissible.
  // 是否需要**同步写入（fsync）**到磁盘。
  // 如果 true，表示 HardState 和 Entries 必须同步写入（保证数据安全）。
  // 如果 false，可以异步写入，提高性能，但可能有数据丢失风险。
  bool must_sync;
};

using ready_channel = channel<void(asio::error_code, ready)>;

// Tick increments the internal logical clock for the Node by a single tick.
// Election timeouts and heartbeat timeouts are in units of ticks. etcd-raft
// 中维护了一个单调递增的逻辑时钟，避免受到系统时钟出现的回调等问题
PRO_DEF_MEM_DISPATCH(node_tick, tick);

// Campaign causes the Node to transition to candidate state and start
// campaigning to become leader.
PRO_DEF_MEM_DISPATCH(node_campaign, campaign);

// Propose proposes that data be appended to the log. Note that proposals can be
// lost without notice, therefore it is user's job to ensure proposal retries.
PRO_DEF_MEM_DISPATCH(node_propose, propose);

// ProposeConfChange proposes a configuration change. Like any proposal, the
// configuration change may be dropped with or without an error being
// returned. In particular, configuration changes are dropped unless the
// leader has certainty that there is no prior unapplied configuration
// change in its log.
//
// The method accepts either a pb.ConfChange (deprecated) or pb.ConfChangeV2
// message. The latter allows arbitrary configuration changes via joint
// consensus, notably including replacing a voter. Passing a ConfChangeV2
// message is only allowed if all Nodes participating in the cluster run a
// version of this library aware of the V2 API. See pb.ConfChangeV2 for
// usage details and semantics.
PRO_DEF_MEM_DISPATCH(node_propose_conf_change, propose_conf_change);

// Step advances the state machine using the given message. ctx.Err() will be
// returned, if any.
// 根据 message 类型，轮转当前节点的状态机
PRO_DEF_MEM_DISPATCH(node_step, step);

// Ready returns a channel that returns the current point-in-time state.
// Users of the Node must call Advance after retrieving the state returned by
// Ready.
//
// NOTE: No committed entries from the next Ready may be applied until all
// committed entries and snapshots from the previous one have finished.
// TODO: C++ 中没有直接类似 golang 的 channel 机制替代。需要考虑替代方案
PRO_DEF_MEM_DISPATCH(node_ready, ready);

// Advance notifies the Node that the application has saved progress up to the
// last Ready. It prepares the node to return the next available Ready.
//
// The application should generally call Advance after it applies the entries in
// last Ready.
//
// However, as an optimization, the application may call Advance while it is
// applying the commands. For example. when the last Ready contains a snapshot,
// the application might take a long time to apply the snapshot data. To
// continue receiving Ready without blocking raft progress, it can call Advance
// before finishing applying the last ready.
// 函数在 Raft 协议中用于通知 Raft 节点，应用程序已经完成了对 Ready()
// 中包含的状态（例如日志条目、快照、配置变更等）的处理，并准备好接收下一个
// Ready 状态。通常和 Ready 函数配套使用
PRO_DEF_MEM_DISPATCH(node_advance, advance);

// ApplyConfChange applies a config change (previously passed to
// ProposeConfChange) to the node. This must be called whenever a config
// change is observed in Ready.CommittedEntries, except when the app decides
// to reject the configuration change (i.e. treats it as a noop instead), in
// which case it must not be called.
//
// Returns an opaque non-nil ConfState protobuf which must be recorded in
// snapshots.
PRO_DEF_MEM_DISPATCH(node_apply_conf_change, apply_conf_change);

// TransferLeadership attempts to transfer leadership to the given transferee.
PRO_DEF_MEM_DISPATCH(node_transfter_leadership, transfter_leadership);

// ReadIndex request a read state. The read state will be set in the ready.
// Read state has a read index. Once the application advances further than the
// read index, any linearizable read requests issued before the read request can
// be processed safely. The read state will have the same rctx attached. Note
// that request can be lost without notice, therefore it is user's job to ensure
// read index retries.
// 用于实现线性化读（linearizable read）
PRO_DEF_MEM_DISPATCH(node_read_index, read_index);

// Status returns the current status of the raft state machine
PRO_DEF_MEM_DISPATCH(node_status, status);

// ReportUnreachable reports the given node is not reachable for the last send.
PRO_DEF_MEM_DISPATCH(node_report_unreachable, report_unreachable);

// ReportSnapshot reports the status of the sent snapshot. The id is the raft ID
// of the follower who is meant to receive the snapshot, and the status is
// SnapshotFinish or SnapshotFailure. Calling ReportSnapshot with SnapshotFinish
// is a no-op. But, any failure in applying a snapshot (for e.g., while
// streaming it from leader to follower), should be reported to the leader with
// SnapshotFailure. When leader sends a snapshot to a follower, it pauses any
// raft log probes until the follower can apply the snapshot and advance its
// state. If the follower can't do that, for e.g., due to a crash, it could end
// up in a limbo, never getting any updates from the leader. Therefore, it is
// crucial that the application ensures that any failure in snapshot sending is
// caught and reported back to the leader; so it can resume raft log probing in
// the follower.
PRO_DEF_MEM_DISPATCH(node_report_snapshot, report_snapshot);

// Stop performs any necessary termination of the Node.
PRO_DEF_MEM_DISPATCH(node_stop, stop);

// clang-format off
struct node_builer : pro::facade_builder 
  ::add_convention<node_tick, void()> 
  ::add_convention<node_campaign, leaf::result<void>()>
  ::add_convention<node_propose, leaf::result<void>(absl::Span<std::byte> data)>
  ::add_convention<node_propose_conf_change, leaf::result<void>(const raftpb::conf_change_v2 &conf_change)>
  ::add_convention<node_step, leaf::result<void>(raftpb::message msg)>
  ::add_convention<node_ready, ready_channel()>
  ::add_convention<node_advance, void()>
  ::add_convention<node_apply_conf_change, std::unique_ptr<raftpb::conf_state>()>
  ::add_convention<node_transfter_leadership, void(std::uint64_t leader_id, std::uint64_t transferee)>
  ::add_convention<node_read_index, leaf::result<void>(absl::Span<std::byte> rctx)>
  ::add_convention<node_status, raft_status()>
  ::add_convention<node_report_unreachable, void(std::uint64_t id)>
  ::add_convention<node_report_snapshot, void(std::uint64_t id, snapshot_status status)>
  ::add_convention<node_advance, void()>
  ::build{};
// clang-format on

}  // namespace lepton

#endif  // _LEPTON_NODE_