#ifndef _LEPTON_RAFT_LOG_UNSTABLE_H_
#define _LEPTON_RAFT_LOG_UNSTABLE_H_
#include <absl/types/span.h>
#include <raft.pb.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <utility>

#include "lepton_error.h"
#include "log.h"
#include "protobuf.h"
#include "types.h"
#include "utility_macros.h"
namespace lepton {
// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
// 用于存储那些还没有持久化（写入磁盘存储）的 Raft 日志条目。
class unstable {
  NOT_COPYABLE(unstable)
  // u.offset <= lo <= hi <= u.offset+len(u.entries)
  void must_check_out_of_bounds(std::uint64_t lo, std::uint64_t hi) const {
    if (lo > hi) {
      LEPTON_CRITICAL("invalid unstable.slice {} > {}", lo, hi);
      assert(false);
    }
    auto upper = offset_ + static_cast<std::uint64_t>(entries_.size());
    if ((lo < offset_) || hi > upper) {
      LEPTON_CRITICAL("unstable.slice[{},{}) out of bound [{},{}]", lo, hi, offset_, upper);
      assert(false);
    }
  }

 public:
  unstable(std::uint64_t offset, std::uint64_t offset_in_progress)
      : offset_(offset), offset_in_progress_(offset_in_progress) {}
  unstable(pb::repeated_entry&& entries, std::uint64_t offset) : entries_(std::move(entries)), offset_(offset) {}
  unstable(pb::repeated_entry&& entries, std::uint64_t offset, std::uint64_t offset_in_progress)
      : entries_(std::move(entries)), offset_(offset), offset_in_progress_(offset_in_progress) {}
  unstable(pb::repeated_entry&& entries, std::optional<raftpb::snapshot>&& s, std::uint64_t offset_in_progress,
           bool snapshot_in_progress)
      : snapshot_(std::move(s)),
        entries_(std::move(entries)),
        snapshot_in_progress_(snapshot_in_progress),
        offset_in_progress_(offset_in_progress) {}
  unstable(std::optional<raftpb::snapshot>&& s, bool snapshot_in_progress)
      : snapshot_(std::move(s)), snapshot_in_progress_(snapshot_in_progress) {}
  unstable(raftpb::snapshot&& snapshot, pb::repeated_entry&& entries, std::uint64_t offset)
      : snapshot_(std::move(snapshot)), entries_(std::move(entries)), offset_(offset) {}
  unstable(unstable&& lhs) = default;
#ifdef LEPTON_TEST
  unstable& operator=(unstable&&) = default;
#endif

  const auto& entries_view() const { return entries_; }

  auto entries_span() const { return absl::MakeSpan(entries_); }

  auto entries_span(std::uint64_t lhs_idx, std::uint64_t rhs_idx) const {
    if (lhs_idx > rhs_idx) {
      LEPTON_CRITICAL("invalid unstable.slice {} > {}", lhs_idx, rhs_idx);
    }

    auto upper = offset_ + static_cast<uint64_t>(entries_.size());
    if (lhs_idx < offset_ || rhs_idx > upper) {
      LEPTON_CRITICAL("unstable.slice[{},{}) out of bound [{},{}]", lhs_idx, rhs_idx, offset_, upper);
    }

    return pb::span_entry(entries_span().data() + lhs_idx - offset_, rhs_idx - lhs_idx);
  }

  std::uint64_t offset() const { return offset_; }

  auto offset_in_progress() const { return offset_in_progress_; }

  auto snapshot_in_progress() const { return snapshot_in_progress_; }

  bool has_snapshot() const { return snapshot_.has_value(); }

  const raftpb::snapshot& snapshot_view() const {
    assert(has_snapshot());
    return *snapshot_;
  }

  bool has_pending_snapshot() const { return has_snapshot() && !pb::is_empty_snap(*snapshot_); }

  // maybeFirstIndex returns the index of the first possible entry in entries
  // if it has a snapshot.
  leaf::result<std::uint64_t> maybe_first_index() const {
    if (has_snapshot()) {
      return snapshot_->metadata().index() + 1;
    }
    return new_error(logic_error::NULL_POINTER, "snapshot is null");
  }

  // maybeLastIndex returns the last index if it has at least one
  // unstable entry or snapshot.
  leaf::result<std::uint64_t> maybe_last_index() const {
    if (auto l = entries_.size(); l != 0) {
      return offset_ + static_cast<std::uint64_t>(l) - 1;
    }
    if (has_snapshot()) {
      return snapshot_->metadata().index();
    }
    return new_error(logic_error::NULL_POINTER, "entries and snapshot both null ptr");
  }

  // maybeTerm returns the term of the entry at index i, if there
  // is any.
  leaf::result<std::uint64_t> maybe_term(std::uint64_t i) const {
    if (i < offset_) {
      if ((has_snapshot()) && (snapshot_->metadata().index() == i)) {
        return snapshot_->metadata().term();
      }
      return new_error(logic_error::NULL_POINTER, "snapshot is null ptr");
    }

    auto result = maybe_last_index();
    if (result.has_error()) {
      return result.error();
    }

    if (i > result.value()) {
      return new_error(logic_error::OUT_OF_BOUNDS, "args is invalid");
    }
    return entries_[static_cast<int>(i - offset_)].term();
  }

  // nextEntries returns the unstable entries that are not already in the process
  // of being written to storage.
  pb::span_entry next_entries() const {
    auto in_progress = static_cast<std::ptrdiff_t>(offset_in_progress_ - offset_);
    if (entries_.size() == in_progress) {
      return {};
    }
    return absl::MakeSpan(entries_.data() + in_progress,
                          static_cast<std::size_t>(entries_.size()) - static_cast<std::size_t>(in_progress));
  }

  // nextSnapshot returns the unstable snapshot, if one exists that is not already
  // in the process of being written to storage.
  std::optional<std::reference_wrapper<const raftpb::snapshot>> next_snapshot() const {
    if (!snapshot_ || snapshot_in_progress_) {
      return std::nullopt;
    }
    return std::cref(*snapshot_);
  }

  // acceptInProgress marks all entries and the snapshot, if any, in the unstable
  // as having begun the process of being written to storage. The entries/snapshot
  // will no longer be returned from nextEntries/nextSnapshot. However, new
  // entries/snapshots added after a call to acceptInProgress will be returned
  // from those methods, until the next call to acceptInProgress.
  void accept_in_progress() {
    if (entries_.size() > 0) {
      // NOTE: +1 because offsetInProgress is exclusive, like offset.
      offset_in_progress_ = entries_[entries_.size() - 1].index() + 1;
    }
    if (snapshot_) {
      snapshot_in_progress_ = true;
    }
  }

  // stableTo marks entries up to the entry with the specified (index, term) as
  // being successfully written to stable storage.
  //
  // The method should only be called when the caller can attest that the entries
  // can not be overwritten by an in-progress log append. See the related comment
  // in newStorageAppendRespMsg.
  void stable_to(const pb::entry_id& id) {
    auto term_result = maybe_term(id.index);
    // 1. log index 可能已经被快照覆盖
    // 2. log index 可能已经无效
    if (term_result.has_error()) {
      // Unstable entry missing. Ignore.
      SPDLOG_INFO("entry at index {} missing from unstable log; ignoring", id.index);
      return;
    }
    if (id.index < offset_) {
      // Index matched unstable snapshot, not unstable entry. Ignore.
      SPDLOG_INFO("entry at index {} matched unstable snapshot; ignoring", id.index);
      return;
    }
    if (term_result.value() != id.term) {
      // Term mismatch between unstable entry and specified entry. Ignore.
      // This is possible if part or all of the unstable log was replaced
      // between that time that a set of entries started to be written to
      // stable storage and when they finished.
      SPDLOG_INFO("entry at (index,term)=({},{}) mismatched with entry at ({},{}) in unstable log; ignoring", id.index,
                  id.term, id.index, term_result.value());
      return;
    }

    // 匹配到 log index 对应的日志
    auto index = static_cast<std::ptrdiff_t>(id.index + 1 - offset_);
    if (entries_.begin() + index >= entries_.end()) {
      entries_.Clear();
    } else {
      // 移动 i 之后的元素并重新赋值给 entries_
      entries_ = pb::repeated_entry(std::make_move_iterator(entries_.begin() + index),
                                    std::make_move_iterator(entries_.end()));
    }
    offset_ = id.index + 1;
    offset_in_progress_ = std::max(offset_in_progress_, offset_);
    shrink_entries_array();
  }

  // shrinkEntriesArray discards the underlying array used by the entries slice
  // if most of it isn't being used. This avoids holding references to a bunch of
  // potentially large entries that aren't needed anymore. Simply clearing the
  // entries wouldn't be safe because clients might still be using them.
  void shrink_entries_array() {
    // no need to do in cpp
  }

  void stable_snap_to(std::uint64_t i) {
    if ((has_snapshot()) && snapshot_->metadata().index() == i) {
      snapshot_.reset();
      snapshot_in_progress_ = false;
    }
  }

  void restore(raftpb::snapshot&& snapshot) {
    offset_ = snapshot.metadata().index() + 1;
    offset_in_progress_ = offset_;
    entries_.Clear();
    snapshot_ = std::move(snapshot);
    snapshot_in_progress_ = false;
  }

  void truncate_and_append(pb::repeated_entry&& entry_list) {
    assert(!entry_list.empty());
    auto from_index = entry_list[0].index();
    if (from_index == (offset_ + static_cast<std::uint64_t>(entries_.size()))) {  // max after value
      entries_.Add(std::make_move_iterator(entry_list.begin()), std::make_move_iterator(entry_list.end()));
    } else if (from_index <= offset_) {  // min after value
      SPDLOG_INFO("replace the unstable entries from index {}", from_index);
      // The log is being truncated to before our current offset
      // portion, so set the offset and replace the entries
      offset_ = from_index;
      offset_in_progress_ = offset_;
      entries_ = std::move(entry_list);
    } else {  // after > u.offset && after < u.offset + uint64(len(u.entries))
      // truncate to after and copy to u.entries
      // then append
      SPDLOG_INFO("truncate the unstable entries before index {}", from_index);
      auto start = static_cast<std::ptrdiff_t>(offset_ - offset_);
      auto end = static_cast<std::ptrdiff_t>(from_index - offset_);
      // 截取 offset 到 after 之间的 entry
      entries_ = pb::repeated_entry(std::make_move_iterator(entries_.begin() + start),
                                    std::make_move_iterator(entries_.begin() + end));
      entries_.Add(std::make_move_iterator(entry_list.begin()), std::make_move_iterator(entry_list.end()));
      // Only in-progress entries before fromIndex are still considered to be
      // in-progress.
      offset_in_progress_ = std::min(offset_in_progress_, from_index);
    }
  }

  // slice returns the entries from the unstable log with indexes in the range
  // [lo, hi). The entire range must be stored in the unstable log or the method
  // will panic. The returned slice can be appended to, but the entries in it must
  // not be changed because they are still shared with unstable.
  //
  // TODO(pavelkalinnikov): this, and similar []pb.Entry slices, may bubble up all
  // the way to the application code through Ready struct. Protect other slices
  // similarly, and document how the client can use them.
  pb::span_entry slice(std::uint64_t lo, std::uint64_t hi) const {
    must_check_out_of_bounds(lo, hi);
    if (lo == hi) {
      return {};
    }
    // NB: use the full slice expression to limit what the caller can do with the
    // returned slice. For example, an append will reallocate and copy this slice
    // instead of corrupting the neighbouring u.entries.
    auto start = static_cast<std::uint64_t>(lo - offset_);
    auto end = static_cast<std::uint64_t>(hi - offset_);
    return absl::MakeSpan(entries_.data() + start, end - start);
  }

 private:
  // the incoming unstable snapshot, if any.
  std::optional<raftpb::snapshot> snapshot_;
  // all entries that have not yet been written to storage.
  pb::repeated_entry entries_;
  // 这个字段记录了未稳定日志条目在 Raft
  // 日志中的位置偏移。也就是说，unstable.offset
  // 是这些日志条目相对于持久化存储中日志的起始位置的偏移量。这个字段的存在是为了确保
  // Raft 节点在日志持久化过程中能够准确地定位到这些日志条目的位置。offset
  // 使得在写入存储时，Raft
  // 能够知道从哪个位置开始写入，避免了重复写入或覆盖已存在的日志。
  // entries[i] has raft log position i+offset.
  std::uint64_t offset_ = 0;

  // if true, snapshot is being written to storage.
  bool snapshot_in_progress_ = false;
  // entries[:offsetInProgress-offset] are being written to storage.
  // Like offset, offsetInProgress is exclusive, meaning that it
  // contains the index following the largest in-progress entry.
  // Invariant: offset <= offsetInProgress
  std::uint64_t offset_in_progress_ = 0;
};

static_assert(std::is_move_constructible_v<raftpb::snapshot>, "raftpb::snapshot is not move constructible");
static_assert(std::is_move_assignable_v<raftpb::snapshot>, "raftpb::snapshot is not move assignable");

}  // namespace lepton

#endif  // _LEPTON_RAFT_LOG_UNSTABLE_H_
