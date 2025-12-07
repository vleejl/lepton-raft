#ifndef _LEPTON_CONFIG_H_
#define _LEPTON_CONFIG_H_
#include <proxy.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "lepton_error.h"
#include "logger.h"
#include "storage.h"
#include "types.h"
namespace lepton::core {

// None is a placeholder node ID used when there is no leader.
constexpr std::uint64_t NONE = 0;
constexpr std::uint64_t NO_LIMIT = UINT64_MAX;
enum class read_only_option : int {
  // ReadOnlySafe guarantees the linearizability of the read only request by
  // communicating with the quorum. It is the default and suggested option.
  // 通过与 quorum（法定多数）通信确认当前 leader 身份仍然有效，从而保证线性一致性。
  READ_ONLY_SAFE,

  // ReadOnlyLeaseBased ensures linearizability of the read only request by
  // relying on the leader lease. It can be affected by clock drift.
  // If the clock drift is unbounded, leader might keep the lease longer than it
  // should (clock can move backward/pause without any bound). ReadIndex is not
  // safe in that case.
  // 借助 leader lease（领导者租约） 确保一致性；
  // 依赖于本地的系统时钟；
  // 可能受时钟漂移影响，存在潜在一致性风险。
  READ_ONLY_LEASE_BASED
};

// Config contains the parameters to start a raft.
struct config {
#ifdef LEPTON_TEST
  config() = default;
#endif
  config(std::uint64_t id, int election_tick, int heartbeat_tick, pro::proxy<storage_builer> &&storage,
         std::uint64_t applied_index, bool async_storage_writes, std::uint64_t max_size_per_msg,
         std::uint64_t max_committed_size_per_ready, std::uint64_t max_uncommitted_entries_size,
         std::size_t max_inflight_msgs, std::uint64_t max_inflight_bytes, bool check_quorum, bool pre_vote,
         read_only_option read_only_opt, bool disable_proposal_forwarding, bool disable_conf_change_validation,
         bool step_down_on_removal, std::shared_ptr<lepton::logger_interface> &&logger)
      : id(id),
        election_tick(election_tick),
        heartbeat_tick(heartbeat_tick),
        storage(std::move(storage)),
        applied_index(applied_index),
        async_storage_writes(async_storage_writes),
        max_size_per_msg(max_size_per_msg),
        max_committed_size_per_ready(max_committed_size_per_ready),
        max_uncommitted_entries_size(max_uncommitted_entries_size),
        max_inflight_msgs(max_inflight_msgs),
        max_inflight_bytes(max_inflight_bytes),
        check_quorum(check_quorum),
        pre_vote(pre_vote),
        read_only_opt(read_only_opt),
        disable_proposal_forwarding(disable_proposal_forwarding),
        disable_conf_change_validation(disable_conf_change_validation),
        step_down_on_removal(step_down_on_removal),
        logger(std::move(logger)) {
    if (this->max_uncommitted_entries_size == 0) {
      this->max_uncommitted_entries_size = NO_LIMIT;
    }

    if (this->max_committed_size_per_ready == 0) {
      this->max_committed_size_per_ready = this->max_size_per_msg;
    }

    if (this->max_inflight_bytes == 0) {
      this->max_inflight_bytes = NO_LIMIT;
    }
  }

  config(std::uint64_t id, int election_tick, int heartbeat_tick, pro::proxy<storage_builer> &&storage,
         std::uint64_t max_size_per_msg, std::size_t max_inflight_msgs, bool disable_conf_change_validation,
         bool step_down_on_removal, std::shared_ptr<lepton::logger_interface> &&logger)
      : config(id, election_tick, heartbeat_tick, std::move(storage), 0, false, max_size_per_msg, 0, 0,
               max_inflight_msgs, 0, false, false, read_only_option::READ_ONLY_SAFE, false,
               disable_conf_change_validation, step_down_on_removal, std::move(logger)) {}

  config clone() const {
    std::shared_ptr<lepton::logger_interface> copy_logger = logger;
    return config(id,              // ID
                  election_tick,   // 选举超时
                  heartbeat_tick,  // 心跳间隔
                  pro::proxy<storage_builer>(),
                  applied_index,                   // 已应用索引
                  async_storage_writes,            // 异步存储写入
                  max_size_per_msg,                // 单消息最大大小
                  max_committed_size_per_ready,    // 单次就绪提交大小限制
                  max_uncommitted_entries_size,    // 未提交条目大小限制
                  max_inflight_msgs,               // 在途消息数量限制
                  max_inflight_bytes,              // 在途消息字节限制
                  check_quorum,                    // 法定人数检查
                  pre_vote,                        // 预投票
                  read_only_opt,                   // 只读选项
                  disable_proposal_forwarding,     // 禁止提案转发
                  disable_conf_change_validation,  // 禁止配置变更验证
                  step_down_on_removal,            // 移除时退位
                  std::move(copy_logger)           // 日志器
    );
  }

  // ID is the identity of the local raft. ID cannot be 0.
  // Raft 节点的唯一标识符。每个 Raft
  // 节点（例如领导者、跟随者、候选者）都会有一个唯一的 id，通常是节点的
  // ID，用来区分不同的节点。
  std::uint64_t id = 0;

  // ElectionTick is the number of Node.Tick invocations that must pass between
  // elections. That is, if a follower does not receive any message from the
  // leader of current term before ElectionTick has elapsed, it will become
  // candidate and start an election. ElectionTick must be greater than
  // HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
  // unnecessary leader switching.
  int election_tick = 0;
  // HeartbeatTick is the number of Node.Tick invocations that must pass between
  // heartbeats. That is, a leader sends heartbeat messages to maintain its
  // leadership every HeartbeatTick ticks.
  int heartbeat_tick = 0;

  // Storage is the storage for raft. raft generates entries and states to be
  // stored in storage. raft reads the persisted entries and states out of
  // Storage when it needs. raft reads out the previous state and configuration
  // out of storage when restarting.
  pro::proxy<storage_builer> storage;

  // Applied is the last applied index. It should only be set when
  // restarting raft. raft will not return entries to the application
  // smaller or equal to Applied. If Applied is unset when restarting, raft
  // might return previous applied entries. This is a very application
  // dependent configuration.
  std::uint64_t applied_index = 0;

  // AsyncStorageWrites configures the raft node to write to its local storage
  // (raft log and state machine) using a request/response message passing
  // interface instead of the default Ready/Advance function call interface.
  // Local storage messages can be pipelined and processed asynchronously
  // (with respect to Ready iteration), facilitating reduced interference
  // between Raft proposals and increased batching of log appends and state
  // machine application. As a result, use of asynchronous storage writes can
  // reduce end-to-end commit latency and increase maximum throughput.
  //
  // When true, the Ready.Message slice will include MsgStorageAppend and
  // MsgStorageApply messages. The messages will target a LocalAppendThread
  // and a LocalApplyThread, respectively. Messages to the same target must be
  // reliably processed in order. In other words, they can't be dropped (like
  // messages over the network) and those targeted at the same thread can't be
  // reordered. Messages to different targets can be processed in any order.
  //
  // MsgStorageAppend carries Raft log entries to append, election votes /
  // term changes / updated commit indexes to persist, and snapshots to apply.
  // All writes performed in service of a MsgStorageAppend must be durable
  // before response messages are delivered. However, if the MsgStorageAppend
  // carries no response messages, durability is not required. The message
  // assumes the role of the Entries, HardState, and Snapshot fields in Ready.
  //
  // MsgStorageApply carries committed entries to apply. Writes performed in
  // service of a MsgStorageApply need not be durable before response messages
  // are delivered. The message assumes the role of the CommittedEntries field
  // in Ready.
  //
  // Local messages each carry one or more response messages which should be
  // delivered after the corresponding storage write has been completed. These
  // responses may target the same node or may target other nodes. The storage
  // threads are not responsible for understanding the response messages, only
  // for delivering them to the correct target after performing the storage
  // write.
  bool async_storage_writes = false;

  // MaxSizePerMsg limits the max byte size of each append message. Smaller
  // value lowers the raft recovery cost(initial probing and message lost
  // during normal operation). On the other side, it might affect the
  // throughput during normal replication. Note: math.MaxUint64 for unlimited,
  // 0 for at most one entry per message.
  pb::entry_encoding_size max_size_per_msg = 0;

  // MaxCommittedSizePerReady limits the size of the committed entries which
  // can be applied.
  std::uint64_t max_committed_size_per_ready = 0;
  // MaxUncommittedEntriesSize limits the aggregate byte size of the
  // uncommitted entries that may be appended to a leader's log. Once this
  // limit is exceeded, proposals will begin to return ErrProposalDropped
  // errors. Note: 0 for no limit.
  pb::entry_payload_size max_uncommitted_entries_size = 0;
  // MaxInflightMsgs limits the max number of in-flight append messages during
  // optimistic replication phase. The application transportation layer usually
  // has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
  // overflowing that sending buffer. TODO (xiangli): feedback to application to
  // limit the proposal rate?
  std::size_t max_inflight_msgs = 0;
  // MaxInflightBytes limits the number of in-flight bytes in append messages.
  // Complements MaxInflightMsgs. Ignored if zero.
  //
  // This effectively bounds the bandwidth-delay product. Note that especially
  // in high-latency deployments setting this too low can lead to a dramatic
  // reduction in throughput. For example, with a peer that has a round-trip
  // latency of 100ms to the leader and this setting is set to 1 MB, there is a
  // throughput limit of 10 MB/s for this group. With RTT of 400ms, this drops
  // to 2.5 MB/s. See Little's law to understand the maths behind.
  std::uint64_t max_inflight_bytes = 0;

  // CheckQuorum specifies if the leader should check quorum activity. Leader
  // steps down when quorum is not active for an electionTimeout.
  bool check_quorum = false;

  // PreVote enables the Pre-Vote algorithm described in raft thesis section
  // 9.6. This prevents disruption when a node that has been partitioned away
  // rejoins the cluster.
  bool pre_vote = false;

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
  read_only_option read_only_opt = read_only_option::READ_ONLY_SAFE;

  // DisableProposalForwarding set to true means that followers will drop
  // proposals, rather than forwarding them to the leader. One use case for
  // this feature would be in a situation where the Raft leader is used to
  // compute the data of a proposal, for example, adding a timestamp from a
  // hybrid logical clock to data in a monotonically increasing way. Forwarding
  // should be disabled to prevent a follower with an inaccurate hybrid
  // logical clock from assigning the timestamp and then forwarding the data
  // to the leader.
  bool disable_proposal_forwarding = false;

  // DisableConfChangeValidation turns off propose-time verification of
  // configuration changes against the currently active configuration of the
  // raft instance. These checks are generally sensible (cannot leave a joint
  // config unless in a joint config, et cetera) but they have false positives
  // because the active configuration may not be the most recent
  // configuration. This is because configurations are activated during log
  // application, and even the leader can trail log application by an
  // unbounded number of entries.
  // Symmetrically, the mechanism has false negatives - because the check may
  // not run against the "actual" config that will be the predecessor of the
  // newly proposed one, the check may pass but the new config may be invalid
  // when it is being applied. In other words, the checks are best-effort.
  //
  // Users should *not* use this option unless they have a reliable mechanism
  // (above raft) that serializes and verifies configuration changes. If an
  // invalid configuration change enters the log and gets applied, a panic
  // will result.
  //
  // This option may be removed once false positives are no longer possible.
  // See: https://github.com/etcd-io/raft/issues/80
  bool disable_conf_change_validation = false;

  // StepDownOnRemoval makes the leader step down when it is removed from the
  // group or demoted to a learner.
  //
  // This behavior will become unconditional in the future. See:
  // https://github.com/etcd-io/raft/issues/83
  bool step_down_on_removal = false;

  std::shared_ptr<lepton::logger_interface> logger;

  leaf::result<void> validate() const;
};
}  // namespace lepton::core
#endif  // _LEPTON_CONFIG_H_