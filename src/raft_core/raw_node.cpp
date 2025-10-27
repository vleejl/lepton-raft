#include "raw_node.h"

#include <cstddef>
#include <vector>

#include "describe.h"
#include "leaf.h"
#include "lepton_error.h"
#include "log.h"
#include "logger.h"
#include "protobuf.h"
#include "raft.pb.h"
#include "raft_error.h"
#include "spdlog/spdlog.h"
#include "state_trace.h"
#include "types.h"
namespace lepton {

leaf::result<raw_node> new_raw_node(config &&c) {
  const auto async_storage_writes = c.async_storage_writes;
  BOOST_LEAF_AUTO(r, new_raft(std::move(c)));
  return raw_node{std::move(r), async_storage_writes};
}

// MustSync returns true if the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
bool must_sync(const raftpb::hard_state &st, const raftpb::hard_state &prev_st, int ents_num) {
  // Persistent state on all servers:
  // (Updated on stable storage before responding to RPCs)
  // currentTerm
  // votedFor
  // log entries[]
  if (ents_num > 0) {
    return true;
  }
  if (st.vote() != prev_st.vote()) {
    return true;
  }
  if (st.term() != prev_st.term()) {
    return true;
  }
  return false;
}

static bool need_storage_append_msg(raft &r, const ready &rd) {
  // Return true if log entries, hard state, or a snapshot need to be written
  // to stable storage. Also return true if any messages are contingent on all
  // prior MsgStorageAppend being processed.
  SPDLOG_TRACE("needStorageAppendMsg rd:\n{}", describe_ready(rd, nullptr));
  if (rd.entries.size() > 0) {
    return true;
  }
  if (!pb::is_empty_hard_state(rd.hard_state)) {
    return true;
  }
  if (!pb::is_empty_snap(rd.snapshot)) {
    return true;
  }
  if (!r.msgs_after_append().empty()) {
    return true;
  }
  return false;
}

static bool need_storage_append_resp_msg(const raft &r, const ready &rd) {
  // Return true if raft needs to hear about stabilized entries or an applied
  // snapshot. See the comment in newStorageAppendRespMsg, which explains why
  // we check hasNextOrInProgressUnstableEnts instead of len(rd.Entries) > 0.
  if (r.raft_log_handle().has_next_or_in_progress_unstable_ents()) {
    return true;
  }
  if (!pb::is_empty_snap(rd.snapshot)) {
    return true;
  }
  return false;
}

// newStorageAppendRespMsg creates the message that should be returned to node
// after the unstable log entries, hard state, and snapshot in the current Ready
// (along with those in all prior Ready structs) have been saved to stable
// storage.
static raftpb::message new_storage_append_resp_msg(const raft &r, const ready &rd) {
  raftpb::message m;
  m.set_type(raftpb::message_type::MSG_STORAGE_APPEND_RESP);
  m.set_to(r.id());
  m.set_from(pb::LOCAL_APPEND_THREAD);
  // Dropped after term change, see below.
  m.set_term(r.term());
  if (r.raft_log_handle().has_next_or_in_progress_unstable_ents()) {
    /*
    ​​ABA 问题场景​​：
      假设 Leader A 发送日志条目（term=1, index=5）给节点 B。
      B 开始异步持久化，但未完成时 A 崩溃，新 Leader C 发送新条目（term=2, index=5），覆盖原条目。
      C 崩溃后 A 恢复，重新发送旧条目（term=1, index=5）。
      如果 B 不检查任期，可能错误截断日志，导致持久化数据被覆盖。
    ​​解决方案​​：
      ​​携带当前任期​​：m.set_term(r.term())。
      ​​响应忽略旧任期​​：节点处理响应时，若当前任期已变更，则丢弃该响应（不截断日志）。
    ​​保证最终收敛​​：
      每次任期变更都会发送新的 MsgStorageAppend。
      只要任期不再频繁变更，最终会有响应不被忽略，从而截断日志。
    */
    // If the raft log has unstable entries, attach the last index and term of the
    // append to the response message. This (index, term) tuple will be handed back
    // and consulted when the stability of those log entries is signaled to the
    // unstable. If the (index, term) match the unstable log by the time the
    // response is received (unstable.stableTo), the unstable log can be truncated.
    //
    // However, with just this logic, there would be an ABA problem[^1] that could
    // lead to the unstable log and the stable log getting out of sync temporarily
    // and leading to an inconsistent view. Consider the following example with 5
    // nodes, A B C D E:
    //
    //  1. A is the leader.
    //  2. A proposes some log entries but only B receives these entries.
    //  3. B gets the Ready and the entries are appended asynchronously.
    //  4. A crashes and C becomes leader after getting a vote from D and E.
    //  5. C proposes some log entries and B receives these entries, overwriting the
    //     previous unstable log entries that are in the process of being appended.
    //     The entries have a larger term than the previous entries but the same
    //     indexes. It begins appending these new entries asynchronously.
    //  6. C crashes and A restarts and becomes leader again after getting the vote
    //     from D and E.
    //  7. B receives the entries from A which are the same as the ones from step 2,
    //     overwriting the previous unstable log entries that are in the process of
    //     being appended from step 5. The entries have the original terms and
    //     indexes from step 2. Recall that log entries retain their original term
    //     numbers when a leader replicates entries from previous terms. It begins
    //     appending these new entries asynchronously.
    //  8. The asynchronous log appends from the first Ready complete and stableTo
    //     is called.
    //  9. However, the log entries from the second Ready are still in the
    //     asynchronous append pipeline and will overwrite (in stable storage) the
    //     entries from the first Ready at some future point. We can't truncate the
    //     unstable log yet or a future read from Storage might see the entries from
    //     step 5 before they have been replaced by the entries from step 7.
    //     Instead, we must wait until we are sure that the entries are stable and
    //     that no in-progress appends might overwrite them before removing entries
    //     from the unstable log.
    //
    // To prevent these kinds of problems, we also attach the current term to the
    // MsgStorageAppendResp (above). If the term has changed by the time the
    // MsgStorageAppendResp if returned, the response is ignored and the unstable
    // log is not truncated. The unstable log is only truncated when the term has
    // remained unchanged from the time that the MsgStorageAppend was sent to the
    // time that the MsgStorageAppendResp is received, indicating that no-one else
    // is in the process of truncating the stable log.
    //
    // However, this replaces a correctness problem with a liveness problem. If we
    // only attempted to truncate the unstable log when appending new entries but
    // also occasionally dropped these responses, then quiescence of new log entries
    // could lead to the unstable log never being truncated.
    //
    // To combat this, we attempt to truncate the log on all MsgStorageAppendResp
    // messages where the unstable log is not empty, not just those associated with
    // entry appends. This includes MsgStorageAppendResp messages associated with an
    // updated HardState, which occur after a term change.
    //
    // In other words, we set Index and LogTerm in a block that looks like:
    //
    //  if r.raftLog.hasNextOrInProgressUnstableEnts() { ... }
    //
    // not like:
    //
    //  if len(rd.Entries) > 0 { ... }
    //
    // To do so, we attach r.raftLog.lastIndex() and r.raftLog.lastTerm(), not the
    // (index, term) of the last entry in rd.Entries. If rd.Entries is not empty,
    // these will be the same. However, if rd.Entries is empty, we still want to
    // attest that this (index, term) is correct at the current term, in case the
    // MsgStorageAppend that contained the last entry in the unstable slice carried
    // an earlier term and was dropped.
    //
    // A MsgStorageAppend with a new term is emitted on each term change. This is
    // the same condition that causes MsgStorageAppendResp messages with earlier
    // terms to be ignored. As a result, we are guaranteed that, assuming a bounded
    // number of term changes, there will eventually be a MsgStorageAppendResp
    // message that is not ignored. This means that entries in the unstable log
    // which have been appended to stable storage will eventually be truncated and
    // dropped from memory.
    //
    // [^1]: https://en.wikipedia.org/wiki/ABA_problem
    auto last = r.raft_log_handle().last_entry_id();
    m.set_index(last.index);
    m.set_log_term(last.term);
  }
  if (!pb::is_empty_snap(rd.snapshot)) {
    m.mutable_snapshot()->CopyFrom(rd.snapshot);
  }
  return m;
}

// newStorageAppendMsg creates the message that should be sent to the local
// append thread to instruct it to append log entries, write an updated hard
// state, and apply a snapshot. The message also carries a set of responses
// that should be delivered after the rest of the message is processed. Used
// with AsyncStorageWrites.
static raftpb::message new_storage_append_msg(raft &r, const ready &rd) {
  raftpb::message m;
  m.set_type(raftpb::message_type::MSG_STORAGE_APPEND);
  m.set_to(pb::LOCAL_APPEND_THREAD);
  m.set_from(r.id());
  *m.mutable_entries() = rd.entries;
  if (!pb::is_empty_hard_state(rd.hard_state)) {
    // If the Ready includes a HardState update, assign each of its fields
    // to the corresponding fields in the Message. This allows clients to
    // reconstruct the HardState and save it to stable storage.
    //
    // If the Ready does not include a HardState update, make sure to not
    // assign a value to any of the fields so that a HardState reconstructed
    // from them will be empty (return true from raft.IsEmptyHardState).
    m.set_term(rd.hard_state.term());
    m.set_vote(rd.hard_state.vote());
    m.set_commit(rd.hard_state.commit());
  }
  if (!pb::is_empty_snap(rd.snapshot)) {
    *m.mutable_snapshot() = rd.snapshot;
  }
  // Attach all messages in msgsAfterAppend as responses to be delivered after
  // the message is processed, along with a self-directed MsgStorageAppendResp
  // to acknowledge the entry stability.
  //
  // NB: it is important for performance that MsgStorageAppendResp message be
  // handled after self-directed MsgAppResp messages on the leader (which will
  // be contained in msgsAfterAppend). This ordering allows the MsgAppResp
  // handling to use a fast-path in r.raftLog.term() before the newly appended
  // entries are removed from the unstable log.
  m.mutable_responses()->CopyFrom(r.msgs_after_append());
  SPDLOG_TRACE("has msg after append {}", m.DebugString());
  if (need_storage_append_resp_msg(r, rd)) {
    m.mutable_responses()->Add(new_storage_append_resp_msg(r, rd));
  }
  SPDLOG_TRACE(m.DebugString());
  return m;
}

static bool need_storage_apply_msg(const ready &rd) { return !rd.committed_entries.empty(); }

static bool need_storage_apply_resp_msg(const ready &rd) { return need_storage_apply_msg(rd); }

// newStorageApplyRespMsg creates the message that should be returned to node
// after the committed entries in the current Ready (along with those in all
// prior Ready structs) have been applied to the local state machine.
static raftpb::message new_storage_apply_resp_msg(const raft &r, const pb::repeated_entry &entries) {
  raftpb::message m;
  m.set_type(raftpb::message_type::MSG_STORAGE_APPLY_RESP);
  m.set_to(r.id());
  m.set_from(pb::LOCAL_APPLY_THREAD);
  m.set_term(0);
  m.mutable_entries()->CopyFrom(entries);
  return m;
}

// newStorageApplyMsg creates the message that should be sent to the local
// apply thread to instruct it to apply committed log entries. The message
// also carries a response that should be delivered after the rest of the
// message is processed. Used with AsyncStorageWrites.
static raftpb::message new_storage_apply_msg(const raft &r, const ready &rd) {
  raftpb::message m;
  m.set_type(raftpb::message_type::MSG_STORAGE_APPLY);
  m.set_to(pb::LOCAL_APPLY_THREAD);
  m.set_from(r.id());
  m.set_term(0);  // committed entries don't apply under a specific term
  m.mutable_entries()->CopyFrom(rd.committed_entries);
  m.mutable_responses()->Add(new_storage_apply_resp_msg(r, rd.committed_entries));
  SPDLOG_TRACE(m.DebugString());
  return m;
}

std::string get_highres_nanoseconds() {
  auto now = std::chrono::high_resolution_clock::now();
  auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
  return std::to_string(ns.count());
}

lepton::ready raw_node::ready_without_accept() {
  lepton::ready rd;
  rd.entries = pb::convert_span_entry(raft_.raft_log_handle_.next_unstable_ents());
  rd.committed_entries = raft_.raft_log_handle_.next_committed_ents(this->apply_unstable_entries());
  rd.messages.CopyFrom(raft_.msgs_);
  if (auto soft_state = raft_.soft_state(); soft_state != prev_soft_state_) {
    rd.soft_state = std::move(soft_state);
  }
  auto hard_state = raft_.hard_state();
  rd.must_sync = must_sync(hard_state, prev_hard_state_, rd.entries.size());
  if (hard_state != prev_hard_state_) {
    rd.hard_state = std::move(hard_state);
  }
  if (raft_.raft_log_handle_.has_next_unstable_snapshot()) {
    auto snap = raft_.raft_log_handle_.next_unstable_snapshot();
    assert(snap);
    rd.snapshot = snap.value();
  }
  if (!raft_.read_states_.empty()) {
    rd.read_states = raft_.read_states_;
  }
  if (async_storage_writes_) {
    // If async storage writes are enabled, enqueue messages to
    // local storage threads, where applicable.
    if (need_storage_append_msg(raft_, rd)) {
      rd.messages.Add(new_storage_append_msg(raft_, rd));
    }
    if (need_storage_apply_msg(rd)) {
      rd.messages.Add(new_storage_apply_msg(raft_, rd));
    }
  } else {
    // If async storage writes are disabled, immediately enqueue
    // msgsAfterAppend to be sent out. The Ready struct contract
    // mandates that Messages cannot be sent until after Entries
    // are written to stable storage.
    // raft durability 持久化保证：不能发送任何消息（尤其是包含 Index 的 AppendEntries）直到对应的日志 entries
    // 已经被写入稳定存储。
    // 所有要发送的 msgsAfterAppend（即 应该在日志成功写入之后再发送的消息）现在可以立刻发送；
    // 这些消息就会放入 rd.Messages 里，外层驱动代码会立刻把它们发送出去；
    for (const auto &msg : raft_.msgs_after_append_) {
      if (msg.to() != raft_.id()) {
        rd.messages.Add()->CopyFrom(msg);
      }
    }
  }
  SPDLOG_TRACE("[ready_without_accept]generate ready, content:\n{}", describe_ready(rd, nullptr));
  return rd;
}

void raw_node::accept_ready(const lepton::ready &rd) {
  SPDLOG_TRACE("accept ready, content:\n{}", describe_ready(rd, nullptr));
  if (rd.soft_state) {
    prev_soft_state_ = *rd.soft_state;
  }
  if (!pb::is_empty_hard_state(rd.hard_state)) {
    prev_hard_state_ = rd.hard_state;
  }
  if (!rd.read_states.empty()) {
    raft_.reset_read_states();
  }
  if (!async_storage_writes_) {
    if (!steps_on_advance_.empty()) {
      LEPTON_CRITICAL("two accepted Ready structs without call to Advance");
    }
    for (const auto &msg : raft_.msgs_after_append()) {
      if (msg.to() == raft_.id()) {
        steps_on_advance_.Add()->CopyFrom(msg);
      }
    }
    if (need_storage_append_resp_msg(raft_, rd)) {
      steps_on_advance_.Add(new_storage_append_resp_msg(raft_, rd));
    }
    if (need_storage_apply_resp_msg(rd)) {
      steps_on_advance_.Add(new_storage_apply_resp_msg(raft_, rd.committed_entries));
    }
  }
  raft_.reset_msgs();
  raft_.raft_log_handle_.accept_unstable();
  if (!rd.committed_entries.empty()) {
    const auto &ents = rd.committed_entries;
    const auto index = ents[ents.size() - 1].index();
    raft_.raft_log_handle_.accept_applying(index, pb::ent_size(ents), apply_unstable_entries());
  }
  trace_ready(raft_);
}

bool raw_node::has_ready() const {
  // TODO(nvanbenschoten): order these cases in terms of cost and frequency.
  if (auto soft_state = raft_.soft_state(); soft_state != prev_soft_state_) {
    SPDLOG_TRACE("soft state has changed, soft_state: {}, prev_soft_state_: {}", describe_soft_state(soft_state),
                 describe_soft_state(prev_soft_state_));
    return true;
  }
  if (auto hard_state = raft_.hard_state(); !pb::is_empty_hard_state(hard_state) && hard_state != prev_hard_state_) {
    SPDLOG_TRACE("hard state has changed, hafrd_state:{}, prev_hard_state_:{}", hard_state.DebugString(),
                 prev_hard_state_.DebugString());
    return true;
  }
  if (raft_.raft_log_handle_.has_next_unstable_snapshot()) {
    SPDLOG_TRACE("has next unstable snapshot");
    return true;
  }
  if (!raft_.msgs_.empty() || !raft_.msgs_after_append().empty()) {
    SPDLOG_TRACE("msg not empty, msgs size:{}, msgs_after_append size:{}", raft_.msgs_.size(),
                 raft_.msgs_after_append().size());
    return true;
  }
  if (raft_.raft_log_handle().has_next_unstable_ents() ||
      raft_.raft_log_handle().has_next_committed_ents(this->apply_unstable_entries())) {
    SPDLOG_TRACE("has next unstable ents:{}, has next committed ents:{}",
                 raft_.raft_log_handle().has_next_unstable_ents(),
                 raft_.raft_log_handle().has_next_committed_ents(this->apply_unstable_entries()));
    return true;
  }
  if (!raft_.read_states_.empty()) {
    SPDLOG_TRACE("read stattes not empty");
    return true;
  }
  return false;
}

void raw_node::advance(/*ready*/) {
  // The actions performed by this function are encoded into stepsOnAdvance in
  // acceptReady. In earlier versions of this library, they were computed from
  // the provided Ready struct. Retain the unused parameter for compatibility.
  if (async_storage_writes_) {
    LEPTON_CRITICAL("Advance must not be called when using AsyncStorageWrites");
  }
  for (auto &msg : steps_on_advance_) {
    discard(raft_.step(std::move(msg)));
  }
  steps_on_advance_.Clear();
}

leaf::result<void> raw_node::bootstrap(std::vector<peer> &&peers) {
  if (peers.empty()) {
    return new_error(raft_error::CONFIG_INVALID, "must provide at least one peer to Bootstrap");
  }

  BOOST_LEAF_AUTO(last_index, raft_.raft_log_handle_.storage_last_index());
  if (last_index != 0) {
    return new_error(raft_error::CONFIG_INVALID, "can't bootstrap a nonempty Storage");
  }

  // We've faked out initial entries above, but nothing has been
  // persisted. Start with an empty HardState (thus the first Ready will
  // emit a HardState update for the app to persist).
  prev_hard_state_ = raftpb::hard_state{};

  // TODO(tbg): remove StartNode and give the application the right tools to
  // bootstrap the initial membership in a cleaner way.
  raft_.become_follower(1, NONE);
  pb::repeated_entry ents;
  ents.Reserve(static_cast<int>(peers.size()));
  for (std::size_t i = 0; i < peers.size(); ++i) {
    auto &iter = peers[i];
    raftpb::conf_change cc;
    cc.set_type(raftpb::CONF_CHANGE_ADD_NODE);
    cc.set_node_id(iter.ID);
    if (!iter.context.empty()) {
      cc.set_context(std::move(iter.context));
    }

    auto entry = ents.Add();
    entry->set_type(raftpb::entry_type::ENTRY_CONF_CHANGE);
    entry->set_term(1);
    entry->set_index(i + 1);
    entry->set_data(cc.SerializeAsString());
  }
  const std::uint64_t ents_size = static_cast<std::uint64_t>(ents.size());
  raft_.raft_log_handle_.append(std::move(ents));

  // Now apply them, mainly so that the application can call Campaign
  // immediately after StartNode in tests. Note that these nodes will
  // be added to raft twice: here and when the application's Ready
  // loop calls ApplyConfChange. The calls to addNode must come after
  // all calls to raftLog.append so progress.next is set after these
  // bootstrapping entries (it is an error if we try to append these
  // entries since they have already been committed).
  // We do not set raftLog.applied so the application will be able
  // to observe all conf changes via Ready.CommittedEntries.
  //
  // TODO(bdarnell): These entries are still unstable; do we need to preserve
  // the invariant that committed < unstable?
  raft_.raft_log_handle_.set_commit(ents_size);
  for (const auto &iter : peers) {
    raftpb::conf_change cc;
    cc.set_type(raftpb::CONF_CHANGE_ADD_NODE);
    cc.set_node_id(iter.ID);
    raft_.apply_conf_change(pb::conf_change_as_v2(std::move(cc)));
  }
  return {};
}

}  // namespace lepton
