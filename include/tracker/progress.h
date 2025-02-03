#ifndef _LEPTON_PROGRESS_H_
#define _LEPTON_PROGRESS_H_
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <sstream>
#include <unordered_map>

#include "inflights.h"
#include "quorum.h"
#include "state.h"
#include "utility_macros.h"
namespace lepton {
namespace tracker {
// Progress represents a follower’s progress in the view of the leader. Leader
// maintains progresses of all followers, and sends entries to the follower
// based on its progress.
//
// NB(tbg): Progress is basically a state machine whose transitions are mostly
// strewn around `*raft.raft`. Additionally, some fields are only used when in a
// certain State. All of this isn't ideal.
class progress {
  NOT_COPYABLE(progress)
  progress(std::uint64_t match, std::uint64_t next, state_type state,
           std::uint64_t pending_snapshot, bool recent_active, bool probe_sent,
           inflights&& inflights, bool is_learner)
      : match_(match),
        next_(next),
        state_(state),
        pending_snapshot_(pending_snapshot),
        recent_active_(recent_active),
        probe_sent_(probe_sent),
        inflights_(std::move(inflights)),
        is_learner_(is_learner) {}

 public:
  progress clone() const {
    return progress{match_,
                    next_,
                    state_,
                    pending_snapshot_,
                    recent_active_,
                    probe_sent_,
                    inflights_.clone(),
                    is_learner_};
  }

  auto is_learner() const { return is_learner_; }

  auto recent_active() const { return recent_active_; }
  // ResetState moves the Progress into the specified State, resetting
  // ProbeSent,
  // PendingSnapshot, and Inflights.
  void reset_state(state_type state) {
    probe_sent_ = false;
    pending_snapshot_ = 0;
    state_ = state;
    inflights_.reset();
  }

  void probe_acked() { probe_sent_ = false; }

  void become_probe() {
    // If the original state is StateSnapshot, progress knows that
    // the pending snapshot has been sent to this peer successfully, then
    // probes from pendingSnapshot + 1.
    if (state_ == state_type::state_snapshot) {
      auto pending_snapshot = pending_snapshot_;
      reset_state(state_type::state_probe);
      // 可以理解为日志和snapshot都共同放在同一个消息队列，所以这里可以直接使用max来确认next
      next_ = std::max(match_ + 1, pending_snapshot + 1);
    } else {
      reset_state(state_type::state_probe);
      next_ = match_ + 1;
    }
  }

  // BecomeReplicate transitions into StateReplicate, resetting Next to Match+1.
  void become_replicate() {
    reset_state(state_type::state_replicate);
    next_ = match_ + 1;
  }

  // BecomeSnapshot moves the Progress to StateSnapshot with the specified
  // pending snapshot index.
  void become_snapshot(std::uint64_t pending_snapshot) {
    reset_state(state_type::state_snapshot);
    pending_snapshot_ = pending_snapshot;
  }

  // MaybeUpdate is called when an MsgAppResp arrives from the follower, with
  // the index acked by it. The method returns false if the given n index comes
  // from an outdated message.
  // Otherwise it updates the progress and returns true.
  bool maybe_update(std::uint64_t n) {
    auto updated = false;
    if (match_ < n) {
      match_ = n;
      updated = true;
      probe_acked();
    }
    next_ = std::max(next_, n + 1);
    return updated;
  }

  // OptimisticUpdate signals that appends all the way up to and including index
  // n are in-flight. As a result, Next is increased to n+1.
  void optimistic_update(std::uint64_t n) { next_ = n + 1; }

  // MaybeDecrTo adjusts the Progress to the receipt of a MsgApp rejection. The
  // arguments are the index of the append message rejected by the follower, and
  // the hint that we want to decrease to.
  //
  // Rejections can happen spuriously as messages are sent out of order or
  // duplicated. In such cases, the rejection pertains to an index that the
  // Progress already knows were previously acknowledged, and false is returned
  // without changing the Progress.
  //
  // If the rejection is genuine, Next is lowered sensibly, and the Progress is
  // cleared for sending log entries.
  bool maybe_decr_to(std::uint64_t rejected, std::uint64_t match_hint) {
    if (state_ == state_type::state_replicate) {
      // The rejection must be stale if the progress has matched and "rejected"
      // is smaller than "match".
      // 如果领导者收到拒绝的日志索引 rejected 小于或等于它当前匹配的日志索引
      // Match，那么这说明拒绝的信息是“过时”的，因为领导者已经确认 Match
      // 之前的日志条目已经成功复制。此时，拒绝应该被认为是无效的，函数返回
      // false，并且 Progress 状态不变。
      if (rejected <= match_) {
        return false;
      }
      // Directly decrease next to match + 1.
      //
      // TODO(tbg): why not use matchHint if it's larger?
      // 之所以不适用 mathc_hint ，可能的原因如下：
      // 1. 保守性设计：避免过度回退
      // 2. mathc_hint 并非总是可靠
      next_ = match_ + 1;
      return true;
    }

    // The rejection must be stale if "rejected" does not match next - 1. This
    // is because non-replicating followers are probed one entry at a time.
    // 如果当前状态不是 StateReplicate（即 StateProbe 或 StateSnapshot）
    // leader 在这些状态下每次发送的日志条目都只涉及一个条目，而不是一组条目。

    // 因此，如果 follower 拒绝的日志条目不是 Next - 1
    // （即正在发送的条目的前一个条目），
    // 则这个拒绝也被认为是“过时”的或无效的，函数返回
    assert(next_ > 0);
    if (next_ - 1 != rejected) {
      return false;
    }

    // 如果拒绝的日志条目确实是 Next - 1，说明拒绝是真实的

    // 这里为何可以使用 matchHint？
    // matchHint 的使用是为了解决在日志拒绝后如何调整 Next
    // 索引的决策问题，尽管它不完全可靠，但它为调整提供了一个合理的范围。
    // 使用 matchHint 并不是依赖它的准确性，而是用它来为 Next
    // 的调整提供一些弹性，确保调整的合理性。 在许多情况下，matchHint
    // 作为一个建议值并不会产生致命问题，尤其是在它与其他更准确的值（如 rejected
    // 和 Match）结合使用时。 因此，在 MaybeDecrTo 函数中，matchHint
    // 的使用是为了更精确地调整
    // Next，避免其变得过小或过大，从而保证日志同步的正确性。
    next_ = std::max(std::min(rejected, match_hint + 1),
                     static_cast<std::uint64_t>(1));
    // 不再等待进一步的探测响应（即 follower 的进度可能已经发生变化）。
    probe_sent_ = false;
    return true;
  }

  bool is_paused() const {
    switch (state_) {
      case state_type::state_probe:
        return probe_sent_;
      case state_type::state_replicate:
        return inflights_.full();
      case state_type::state_snapshot:
        return true;
      default:
        // SPDLOG_CRITICAL("[unreacheable] can not recognize state type");
        return false;
    }
  }

  auto string() const {
    std::ostringstream ss;
    ss << magic_enum::enum_name(state_) << " match=" << match_
       << " next=" << next_;
    if (is_learner_) {
      ss << " learner";
    }
    if (is_paused()) {
      ss << " paused";
    }
    if (pending_snapshot_ > 0) {
      ss << " pending_snapshot=" << pending_snapshot_;
    }
    if (!recent_active_) {
      ss << " inactive";
    }
    if (auto count = inflights_.count(); count > 0) {
      ss << " inflight=" << count;
      if (inflights_.full()) {
        ss << "[full]";
      }
    }
    return ss.str();
  }

 private:
  // Match 表示 follower 已经复制的最新日志条目的索引。即，leader 认为 follower
  // 已经跟进到的日志索引。
  std::uint64_t match_;
  // Next 表示 leader 将要发送给 follower 的下一个日志条目的索引。每次 leader
  // 向 follower 发送数据时，会把 Next 索引的日志条目发过去。Next
  // 会在不同状态之间发生变化。
  /*Next 字段表示领导者（leader）将要发送给 follower
    的下一个日志条目的索引，它在 Raft
    协议中扮演了一个非常关键的角色。下面详细解释为什么需要这个字段以及它在不同状态下的作用：

    1. 确保日志条目顺序传递
    Raft 协议要求日志条目的顺序是严格一致的，所有的 follower
    必须按正确的顺序接收日志条目。Next 指向了当前应该发送给 follower
    的下一个日志条目的索引。每次发送日志时，领导者根据 Next 确定发送哪些条目。

    如果 follower 的进度落后，Next 会指向领导者日志中 follower
    缺失的第一个条目。这样，领导者可以通过 Next 确保从正确的地方开始向 follower
    发送数据，避免漏掉任何条目。
    2. 处理不同的 follower 进度
    每个 follower
    可能会有不同的进度，也就是说，它们可能已经复制了一部分日志，但并不完全相同。通过使用
    Next，领导者可以有针对性地将日志条目发送给每个 follower。

    状态变动时调整 Next:
    StateProbe: 当 follower 的进度不确定时，Next
    会指向当前已知的最后一个条目的索引，领导者会发送一个探测消息来确认 follower
    的进度。 StateReplicate: 当 follower 已经紧跟领导者的日志时，Next
    会向前推进，发送更多的日志条目。 StateSnapshot: 当 follower
    的日志丢失，需要快照时，Next
    可能会指向一个需要通过快照恢复的点，直到快照恢复完毕。
    3. 确保可靠的数据传输
    Next
    的存在帮助领导者在发送日志时保持一致性和可靠性。领导者不会重复发送已经成功复制的日志条目，也不会发送不需要的条目。它可以精确地知道从哪个日志条目开始发送，减少了无谓的传输。

    避免重复发送日志: Next
    确保了每次发送的日志条目是唯一且有序的，避免了重复的日志条目传输，提高了效率。
    4. 协调日志复制
    Next 还可以帮助领导者与 follower 之间更好地协调日志复制。如果 follower
    丢失了日志条目，Next
    允许领导者从正确的位置重新开始复制数据，而不是重新发送所有日志。它通过适时的调整来确保日志复制的效率。

    总结
    简而言之，Next 用于标记领导者准备发送给 follower
    的下一个日志条目的索引，这样可以确保日志条目的顺序性、可靠性和一致性。在
    Raft 协议中，不同 follower 可能处于不同的进度状态，Next 是协调各个 follower
    和 leader 之间日志复制的关键字段。
   */
  std::uint64_t next_;

  // State defines how the leader should interact with the follower.
  //
  // When in StateProbe, leader sends at most one replication message
  // per heartbeat interval. It also probes actual progress of the follower.
  //
  // When in StateReplicate, leader optimistically increases next
  // to the latest entry sent after sending replication message. This is
  // an optimized state for fast replicating log entries to the follower.
  //
  // When in StateSnapshot, leader should have sent out snapshot
  // before and stops sending any replication message.
  // State 表示 follower 的当前状态，它影响 leader 如何与 follower
  // 交互。可能的状态有：
  // * StateProbe: 该状态表示 leader 正在探测 follower 的进度，leader
  // 每次心跳间隔最多发送一条 replication 消息。
  // * StateReplicate: 该状态表示 follower 已经准备好接收日志条目，leader
  // 会乐观地增加 Next 索引，并继续快速地向 follower 发送日志条目。
  // * StateSnapshot: 该状态表示 follower
  // 丢失了部分日志条目，需要通过快照恢复，leader
  // 将会停止发送日志条目，直到快照发送完毕。
  state_type state_;

  // PendingSnapshot is used in StateSnapshot.
  // If there is a pending snapshot, the pendingSnapshot will be set to the
  // index of the snapshot. If pendingSnapshot is set, the replication process
  // of this Progress will be paused. raft will not resend snapshot until the
  // pending one is reported to be failed.
  // 该字段在 StateSnapshot
  // 状态下使用，表示是否有待处理的快照。如果有待处理的快照，PendingSnapshot
  // 将会存储快照的索引，且在此期间不会发送任何日志条目。
  std::uint64_t pending_snapshot_;

  // RecentActive is true if the progress is recently active. Receiving any
  // messages from the corresponding follower indicates the progress is active.
  // RecentActive can be reset to false after an election timeout.
  //
  // TODO(tbg): the leader should always have this set to true.
  // RecentActive 用于标记该 follower 是否最近是活跃的。只要 leader 接收到来自
  // follower 的消息，RecentActive 就会被设置为 true，否则可能会因超时重置为
  // false。这是一个用于追踪 follower 活跃性的标记。
  bool recent_active_;

  // ProbeSent is used while this follower is in StateProbe. When ProbeSent is
  // true, raft should pause sending replication message to this peer until
  // ProbeSent is reset. See ProbeAcked() and IsPaused().
  // 该字段表示 follower 是否处于 StateProbe 状态。如果为 true，表示 leader
  // 在当前心跳间隔已经发送了一个探测消息。此时，leader
  // 暂时不会继续发送日志条目，直到 ProbeSent 被重置。
  bool probe_sent_;

  // Inflights is a sliding window for the inflight messages.
  // Each inflight message contains one or more log entries.
  // The max number of entries per message is defined in raft config as
  // MaxSizePerMsg. Thus inflight effectively limits both the number of inflight
  // messages and the bandwidth each Progress can use. When inflights is Full,
  // no more message should be sent. When a leader sends out a message, the
  // index of the last entry should be added to inflights. The index MUST be
  // added into inflights in order. When a leader receives a reply, the previous
  // inflights should be freed by calling inflights.FreeLE with the index of the
  // last received entry.
  // Inflights 是一个滑动窗口，用于追踪当前正在发送的日志消息。在 leader 向
  // follower 发送消息时，它会将每个消息的最后一个日志条目的索引添加到 Inflights
  // 中。 当一个消息发送完并且收到确认时，leader 会调用 Inflights.FreeLE()
  // 来释放不再需要的 inflight 消息，确保只有有限的消息在等待确认。
  inflights inflights_;

  bool is_learner_;

  friend class progress_map;
};

class progress_map {
  NOT_COPYABLE(progress_map)
 public:
  using type = std::unordered_map<std::uint64_t, progress>;
  friend class progress_tracker;

  progress_map() = default;
  explicit progress_map(type&& map) : map_(std::move(map)) {}
  progress_map(progress_map&&) = default;

  const type& view() const { return map_; }

  progress_map clone() const;

  std::string string(const progress_map& m);

  leaf::result<quorum::log_index> acked_index(std::uint64_t id);

 private:
  type map_;
};

// AckedIndex implements IndexLookuper.
using match_ack_indexer = progress_map;
}  // namespace tracker
}  // namespace lepton

#endif  // _LEPTON_PROGRESS_H_
