#ifndef _LEPTON_RAFT_LOG_H_
#define _LEPTON_RAFT_LOG_H_
#include <absl/types/span.h>
#include <proxy.h>
#include <raft.pb.h>

#include "config.h"
#include "lepton_error.h"
#include "raft_log_unstable.h"
#include "storage.h"
#include "types.h"
#include "utility_macros.h"

namespace lepton {
class raft_log {
  NOT_COPYABLE(raft_log)
 public:
  raft_log(raft_log&& rhs) = default;
  raft_log(pro::proxy<storage_builer>&& sstorage, std::uint64_t first_index, std::uint64_t last_index,
           std::uint64_t max_applying_ents_size);

  std::string string();

  // maybeAppend returns (0, false) if the entries cannot be appended.
  // Otherwise, it returns (last index of new entries, true).
  leaf::result<std::uint64_t> maybe_append(pb::log_slice&& log_slice, std::uint64_t committed);

  std::uint64_t append(pb::repeated_entry&& entries);

  // findConflict finds the index of the conflict.
  // It returns the first pair of conflicting entries between the existing
  // entries and the given entries, if there are any.
  // If there is no conflicting entries, and the existing entries contains
  // all the given entries, zero will be returned.
  // If there is no conflicting entries, but the given entries contains new
  // entries, the index of the first new entry will be returned.
  // An entry is considered to be conflicting if it has the same index but
  // a different term.
  // [major point]: The index of the given entries MUST be continuously
  // increasing.
  std::uint64_t find_conflict(pb::span_entry entries);

  // findConflictByTerm takes an (index, term) pair (indicating a conflicting
  // log
  // entry on a leader/follower during an append) and finds the largest index in
  // log l with a term <= `term` and an index <= `index`. If no such index
  // exists in the log, the log's first index is returned.
  //
  // The index provided MUST be equal to or less than l.lastIndex(). Invalid
  // inputs log a warning and the input index is returned.
  std::tuple<std::uint64_t, std::uint64_t> find_conflict_by_term(std::uint64_t index, std::uint64_t term);

  // nextUnstableEnts returns all entries that are available to be written to the
  // local stable log and are not already in-progress.
  pb::span_entry next_unstable_ents() const { return unstable_.next_entries(); }

  // hasNextUnstableEnts returns if there are any entries that are available to be
  // written to the local stable log and are not already in-progress.
  bool has_next_unstable_ents() const { return !next_unstable_ents().empty(); }

  // hasNextOrInProgressUnstableEnts returns if there are any entries that are
  // available to be written to the local stable log or in the process of being
  // written to the local stable log.
  bool has_next_or_in_progress_unstable_ents() const { return !unstable_.entries_view().empty(); }

  // nextCommittedEnts returns all the available entries for execution.
  // Entries can be committed even when the local raft instance has not durably
  // appended them to the local raft log yet. If allowUnstable is true, committed
  // entries from the unstable log may be returned; otherwise, only entries known
  // to reside locally on stable storage will be returned.
  pb::repeated_entry next_committed_ents(bool allow_unstable) const;

  // hasNextCommittedEnts returns if there is any available entries for execution.
  // This is a fast check without heavy raftLog.slice() in nextCommittedEnts().
  bool has_next_committed_ents(bool allow_unstable) const;

  // maxAppliableIndex returns the maximum committed index that can be applied.
  // If allowUnstable is true, committed entries from the unstable log can be
  // applied; otherwise, only entries known to reside locally on stable storage
  // can be applied.
  std::uint64_t max_appliable_index(bool allow_unstable) const;

  // nextUnstableSnapshot returns the snapshot, if present, that is available to
  // be applied to the local storage and is not already in-progress.
  std::optional<std::reference_wrapper<const raftpb::snapshot>> next_unstable_snapshot() const {
    return unstable_.next_snapshot();
  }

  // hasNextUnstableSnapshot returns if there is a snapshot that is available to
  // be applied to the local storage and is not already in-progress.
  bool has_next_unstable_snapshot() const { return unstable_.next_snapshot().has_value(); }

  // hasNextOrInProgressSnapshot returns if there is pending snapshot waiting for
  // applying or in the process of being applied.
  bool has_next_or_in_progress_snapshot() const { return unstable_.has_pending_snapshot(); }

  leaf::result<raftpb::snapshot> snapshot() const;

  std::uint64_t first_index() const;

  std::uint64_t last_index() const;

#ifdef LEPTON_TEST
  raft_log& operator=(raft_log&&) = default;

  void set_commit(std::uint64_t tocommit) { committed_ = tocommit; }

  void set_applying_ents_paused(bool applying_ents_paused) { applying_ents_paused_ = applying_ents_paused; }
#endif

  void commit_to(std::uint64_t tocommit);

  auto committed() const { return committed_; }

  void applied_to(std::uint64_t i, pb::entry_encoding_size size);

  void accept_applying(std::uint64_t i, pb::entry_encoding_size size, bool allow_unstable);

  void stable_to(const pb::entry_id& id) { unstable_.stable_to(id); }

  void stable_snap_to(std::uint64_t i) { unstable_.stable_snap_to(i); }

  // acceptUnstable indicates that the application has started persisting the
  // unstable entries in storage, and that the current unstable entries are thus
  // to be marked as being in-progress, to avoid returning them with future calls
  // to Ready().
  void accept_unstable() { unstable_.accept_in_progress(); }

  // lastEntryID returns the ID of the last entry in the log.
  pb::entry_id last_entry_id() const;

  auto applying() const { return applying_; }

  auto applied() const { return applied_; }

  auto max_applying_ents_size() const { return max_applying_ents_size_; }

  auto applying_ents_size() const { return applying_ents_size_; }

  leaf::result<std::uint64_t> term(std::uint64_t i) const;

  leaf::result<pb::repeated_entry> entries(std::uint64_t i, std::uint64_t max_size);

  // allEntries returns all entries in the log.
  pb::repeated_entry all_entries();

  // isUpToDate determines if the given (lastIndex,term) log is more up-to-date
  // by comparing the index and term of the last entries in the existing logs.
  // If the logs have last entries with different terms, then the log with the
  // later term is more up-to-date. If the logs end with the same term, then
  // whichever log has the larger lastIndex is more up-to-date. If the logs are
  // the same, the given log is up-to-date.
  bool is_up_to_date(const pb::entry_id& their);

  bool match_term(const pb::entry_id& id);

  bool maybe_commit(const pb::entry_id& at);

  void restore(raftpb::snapshot&& snapshot);

  // scan visits all log entries in the [lo, hi) range, returning them via the
  // given callback. The callback can be invoked multiple times, with consecutive
  // sub-ranges of the requested range. Returns up to pageSize bytes worth of
  // entries at a time. May return more if a single entry size exceeds the limit.
  //
  // The entries in [lo, hi) must exist, otherwise scan() eventually returns an
  // error (possibly after passing some entries through the callback).
  //
  // If the callback returns an error, scan terminates and returns this error
  // immediately. This can be used to stop the scan early ("break" the loop).
  leaf::result<void> scan(std::uint64_t lo, std::uint64_t hi, pb::entry_encoding_size page_size,
                          std::function<leaf::result<void>(const pb::repeated_entry& entries)> callback) const;

  leaf::result<pb::repeated_entry> slice(std::uint64_t lo, std::uint64_t hi, pb::entry_encoding_size max_size) const;

  // l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
  leaf::result<void> must_check_out_of_bounds(std::uint64_t lo, std::uint64_t hi) const;

  std::uint64_t zero_term_on_err_compacted(std::uint64_t i) const;

  const unstable& unstable_view() const { return unstable_; }

  const auto& applying_ents_paused() const { return applying_ents_paused_; }

 private:
  // storage contains all stable entries since the last snapshot.
  pro::proxy<storage_builer> storage_;

  // unstable contains all unstable entries and snapshot.
  // they will be saved into storage.
  unstable unstable_;

  // committed is the highest log position that is known to be in
  // stable storage on a quorum of nodes.
  // committed 表示已知在一组节点中，最远已提交的日志条目的索引。committed
  // 是指当前集群中已经被大多数节点确认并且处于稳定状态的日志索引。
  // 用途：这是 Raft 日志的一个关键指标，指示了日志的提交进度。在 Raft
  // 中，日志必须得到大多数节点的确认才能提交（即成为 commited 日志）。
  std::uint64_t committed_ = 0;

  // applying is the highest log position that the application has
  // been instructed to apply to its state machine. Some of these
  // entries may be in the process of applying and have not yet
  // reached applied.
  // Use: The field is incremented when accepting a Ready struct.
  // Invariant: applied <= applying && applying <= committed
  std::uint64_t applying_ = 0;

  // applied is the highest log position that the application has
  // successfully applied to its state machine.
  // Use: The field is incremented when advancing after the committed
  // entries in a Ready struct have been applied (either synchronously
  // or asynchronously).
  // Invariant: applied <= committed
  // 是指已经被应用到状态机的日志条目的最高索引。该字段表示状态机已经处理并执行的日志条目。应用到状态机后，日志条目会被执行并影响系统的状态。
  // 用途：它和 committed 紧密相关，并且是 Raft 协议中的一个重要概念。
  // applied 永远小于等于 committed，确保只有被提交的日志才能被应用。
  std::uint64_t applied_ = 0;

  // maxApplyingEntsSize limits the outstanding byte size of the messages
  // returned from calls to nextCommittedEnts that have not been acknowledged
  // by a call to appliedTo.
  // 获取日志条目时，返回的日志条目的总字节数的最大值。这是为了避免一次性返回太多日志，导致内存使用过高或性能问题。
  // 用途：限制每次从日志中取出的条目大小。这有助于控制系统的内存和带宽，防止一次请求中返回过多日志条目。
  pb::entry_encoding_size max_applying_ents_size_ = 0;

  // applyingEntsSize is the current outstanding byte size of the messages
  // returned from calls to nextCommittedEnts that have not been acknowledged
  // by a call to appliedTo.
  pb::entry_encoding_size applying_ents_size_ = 0;

  // applyingEntsPaused is true when entry application has been paused until
  // enough progress is acknowledged.
  // 该字段表示当前是否暂停了日志条目的应用。它用于控制日志条目的应用进度。
  // 用途：在某些情况下，可能需要暂停日志条目的应用，以便进行其他操作或等待条件满足。
  bool applying_ents_paused_ = false;
};

leaf::result<raft_log> new_raft_log_with_size(pro::proxy<storage_builer>&& storage,
                                              pb::entry_encoding_size max_applying_ents_size);

inline leaf::result<raft_log> new_raft_log(pro::proxy<storage_builer>&& storage) {
  return new_raft_log_with_size(std::move(storage), NO_LIMIT);
}

}  // namespace lepton

#endif  // _LEPTON_RAFT_LOG_H_
