#ifndef _LEPTON_NODE_INTERFACE_H_
#define _LEPTON_NODE_INTERFACE_H_
#include <absl/types/span.h>
#include <proxy.h>
#include <raft.pb.h>

#include "conf_change.h"
#include "lepton_error.h"
#include "ready.h"
namespace lepton::core {

enum class snapshot_status : int {
  SNAPSHOT_FINISH = 1,
  SNAPSHOT_FAILURE = 2,
};

struct peer {
  std::uint64_t ID;
  std::string context;
};

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

/// Asynchronously request and wait for the next Raft Ready state.
///
/// This is the C++/Asio equivalent of etcd-raft's `Ready() <-chan Ready`.
/// It sends an internal request for a Ready callback channel, then suspends
/// until the Raft node produces the next point-in-time state.
///
/// **Usage contract**:
/// - The caller must fully process the returned `ready_handle`
///   (apply committed entries, persist snapshots, etc.).
/// - Once processing completes, the caller must invoke `advance()`
///   to notify the node, unless async storage writes are enabled.
/// - No entries from the next Ready will be delivered until all
///   items from the previous Ready are fully handled.
///
/// \param exec The executor on which the internal callback channel will run.
/// \return An awaitable that yields `ready_handle` on success,
///         or an error if the node is stopped or the channel is closed.
PRO_DEF_MEM_DISPATCH(node_wait_ready, wait_ready);

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
PRO_DEF_MEM_DISPATCH(node_transfer_leadership, transfer_leadership);

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

// Node represents a node in a raft cluster.
// clang-format off
struct node_builder : pro::facade_builder 
  ::add_convention<node_tick, void()> 
  ::add_convention<node_campaign, asio::awaitable<expected<void>>()>
  ::add_convention<node_propose, asio::awaitable<expected<void>>(std::string &&data)>
  ::add_convention<node_propose_conf_change, asio::awaitable<expected<void>>(const pb::conf_change_var &cc)>
  ::add_convention<node_step, asio::awaitable<expected<void>>(raftpb::message &&msg)>
  ::add_convention<node_wait_ready, asio::awaitable<expected<ready_handle>>(asio::any_io_executor executor)>
  ::add_convention<node_advance, asio::awaitable<void>()>
  ::add_convention<node_apply_conf_change, asio::awaitable<expected<raftpb::conf_state>>(raftpb::conf_change_v2 &&cc)>
  ::add_convention<node_transfer_leadership, asio::awaitable<void>(std::uint64_t leader_id, std::uint64_t transferee)>
  ::add_convention<node_read_index, asio::awaitable<expected<void>>(std::string &&rctx)>
  ::add_convention<node_status, asio::awaitable<expected<lepton::core::status>>()>
  ::add_convention<node_report_unreachable, asio::awaitable<void>(std::uint64_t id)>
  ::add_convention<node_report_snapshot, asio::awaitable<void>(std::uint64_t id, snapshot_status status)>
  ::add_convention<node_stop, asio::awaitable<void>()>
  ::add_skill<pro::skills::as_view>
  ::build{};
// clang-format on

}  // namespace lepton::core

#endif  // _LEPTON_NODE_INTERFACE_H_
