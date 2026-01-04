#pragma once
#ifndef _LEPTON_READY_H_
#define _LEPTON_READY_H_
#include <absl/types/span.h>
#include <proxy.h>
#include <raft.pb.h>

#include <memory>
#include <vector>

#include "coroutine/channel_endpoint.h"
#include "raft_core/pb/types.h"
#include "raft_core/read_only.h"
#include "raft_core/state.h"

namespace lepton::core {
// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
struct ready {
 private:
  ready(const ready &) = default;
  ready &operator=(const ready &) = delete;

 public:
  ready() = default;
  ready(ready &&) = default;
  ready &operator=(ready &&) = default;
  ready clone() const { return ready{*this}; }

  // The current volatile state of a Node.
  // SoftState will be nil if there is no update.
  // It is not required to consume or store SoftState.
  // 非持久化状态，可能为空
  // 代表节点的易变状态（volatile state），例如 Leader 是谁、当前 Raft 节点的
  // State（如 Follower、Candidate、Leader）。
  // 由于 SoftState 不是持久化数据，因此不需要存储，只用于节点间的交互。
  // 如果没有更新，它会是 nil。
  std::optional<core::soft_state> soft_state;

  // The current state of a Node to be saved to stable storage BEFORE
  // Messages are sent.
  // HardState will be equal to empty state if there is no update.
  // 代表持久化的状态，包括
  // Term（当前任期）、Vote（投票给谁）、Commit（已提交的日志索引）。
  // 需要在发送消息之前保存到稳定存储（如磁盘）。
  raftpb::HardState hard_state;

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
  pb::repeated_entry entries;

  // Snapshot specifies the snapshot to be saved to stable storage.
  // 如果存在快照数据，需要存储到稳定存储。
  // 这个字段用于快照恢复，如果没有新的快照，该字段为空。
  raftpb::Snapshot snapshot;

  // CommittedEntries specifies entries to be committed to a
  // store/state-machine. These have previously been committed to stable
  // store.
  // 已经提交的日志条目，需要应用到状态机（store/state-machine）。
  // 这些日志之前已经被存储到稳定存储，只是还没有执行。
  pb::repeated_entry committed_entries;

  // Messages specifies outbound messages.
  //
  // If async storage writes are not enabled, these messages must be sent
  // AFTER Entries are appended to stable storage.
  //
  // If async storage writes are enabled, these messages can be sent
  // immediately as the messages that have the completion of the async writes
  // as a precondition are attached to the individual MsgStorage{Append,Apply}
  // messages instead.
  //
  // If it contains a MsgSnap message, the application MUST report back to raft
  // when the snapshot has been received or has failed by calling ReportSnapshot.
  // 需要发送到其他 Raft 节点的消息，但必须在日志条目（Entries）持久化后发送。
  // 如果其中包含 MsgSnap（快照消息），应用层必须在快照被接收或失败后调用
  // ReportSnapshot 进行反馈。
  pb::repeated_message messages;

  // MustSync indicates whether the HardState and Entries must be synchronously
  // written to disk or if an asynchronous write is permissible.
  // 是否需要**同步写入（fsync）**到磁盘。
  // 如果 true，表示 HardState 和 Entries 必须同步写入（保证数据安全）。
  // 如果 false，可以异步写入，提高性能，但可能有数据丢失风险。
  bool must_sync = false;
};

using ready_handle = std::shared_ptr<ready>;
using ready_channel = coro::channel_endpoint<ready_handle>;
using ready_channel_handle = std::shared_ptr<ready_channel>;
}  // namespace lepton::core

#endif  // _LEPTON_READY_H_
