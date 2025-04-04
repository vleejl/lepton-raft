#ifndef _LEPTON_RAFT_LOG_UNSTABLE_H_
#define _LEPTON_RAFT_LOG_UNSTABLE_H_
#include <absl/types/span.h>
#include <raft.pb.h>

#include <cassert>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

#include "error.h"
#include "log.h"
#include "protobuf.h"
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
  void must_check_out_of_bounds(std::uint64_t lo, std::uint64_t hi) {
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
  unstable(std::uint64_t offset) : offset_(offset) {}
  unstable(pb::repeated_entry&& entries, std::uint64_t offset) : entries_(std::move(entries)), offset_(offset) {}
  unstable(raftpb::snapshot&& snapshot, pb::repeated_entry&& entries, std::uint64_t offset)
      : snapshot_(std::move(snapshot)), entries_(std::move(entries)), offset_(offset) {}
  unstable(unstable&& lhs) = default;

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

    return absl::Span<const raftpb::entry* const>(entries_span().data() + lhs_idx - offset_, rhs_idx - lhs_idx);
  }

  std::uint64_t offset() const { return offset_; }

  bool has_snapshot() const { return snapshot_.has_value(); }

  const raftpb::snapshot& snapshot_view() const {
    assert(has_snapshot());
    return *snapshot_;
  }

  bool has_pending_snapshot() const { return has_snapshot() && pb::is_empty_snap(*snapshot_); }

  // maybeFirstIndex returns the index of the first possible entry in entries
  // if it has a snapshot.
  leaf::result<std::uint64_t> maybe_first_index() const {
    if (has_snapshot()) {
      return snapshot_->metadata().index() + 1;
    }
    return new_error(encoding_error::NULL_POINTER, "snapshot is null");
  }

  // maybeLastIndex returns the last index if it has at least one
  // unstable entry or snapshot.
  leaf::result<std::uint64_t> maybe_last_index() {
    if (auto l = entries_.size(); l != 0) {
      return offset_ + static_cast<std::uint64_t>(l) - 1;
    }
    if (has_snapshot()) {
      return snapshot_->metadata().index();
    }
    return new_error(encoding_error::NULL_POINTER, "entries and snapshot both null ptr");
  }

  // maybeTerm returns the term of the entry at index i, if there
  // is any.
  leaf::result<std::uint64_t> maybe_term(std::uint64_t i) {
    if (i < offset_) {
      if ((has_snapshot()) && (snapshot_->metadata().index() == i)) {
        return snapshot_->metadata().term();
      }
      return new_error(encoding_error::NULL_POINTER, "snapshot is null ptr");
    }

    auto result = maybe_last_index();
    if (result.has_error()) {
      return result.error();
    }

    if (i > result.value()) {
      return new_error(encoding_error::OUT_OF_BOUNDS, "args is invalid");
    }
    return entries_[static_cast<int>(i - offset_)].term();
  }

  // i: log index
  // t: term
  void stable_to(std::uint64_t i, std::uint64_t t) {
    auto term_result = maybe_term(i);
    // 1. log index 可能已经被快照覆盖
    // 2. log index 可能已经无效
    if (!term_result) {
      return;
    }
    // 匹配到 log index 对应的日志
    // if i < offset, term is matched with the snapshot
    // only update the unstable entries if term is matched with
    // an unstable entry.
    if ((term_result.value() == t) && (i >= offset_)) {
      auto index = static_cast<std::ptrdiff_t>(i + 1 - offset_);
      if (entries_.begin() + index >= entries_.end()) {
        entries_.Clear();
      } else {
        // 移动 i 之后的元素并重新赋值给 entries_
        entries_ = pb::repeated_entry(std::make_move_iterator(entries_.begin() + index),
                                      std::make_move_iterator(entries_.end()));
      }
      offset_ = i + 1;
    }
  }

  void stable_snap_to(std::uint64_t i) {
    if ((has_snapshot()) && snapshot_->metadata().index() == i) {
      snapshot_.reset();
    }
  }

  void restore(raftpb::snapshot&& snapshot) {
    offset_ = snapshot.metadata().index() + 1;
    entries_.Clear();
    snapshot_ = std::move(snapshot);
  }

  void truncate_and_append(pb::repeated_entry&& entry_list) {
    assert(!entry_list.empty());
    auto after = entry_list[0].index();
    if (after == (offset_ + static_cast<std::uint64_t>(entries_.size()))) {  // max after value
      entries_.Add(std::make_move_iterator(entry_list.begin()), std::make_move_iterator(entry_list.end()));
    } else if (after <= offset_) {  // min after value
      SPDLOG_INFO("replace the unstable entries from index {}", after);
      // The log is being truncated to before our current offset
      // portion, so set the offset and replace the entries
      offset_ = after;
      entries_ = std::move(entry_list);
    } else {  // after > u.offset && after < u.offset + uint64(len(u.entries))
      // truncate to after and copy to u.entries
      // then append
      SPDLOG_INFO("truncate the unstable entries before index {}", after);
      auto start = static_cast<std::ptrdiff_t>(offset_ - offset_);
      auto end = static_cast<std::ptrdiff_t>(after - offset_);
      // 截取 offset 到 after 之间的 entry
      entries_ = pb::repeated_entry(std::make_move_iterator(entries_.begin() + start),
                                    std::make_move_iterator(entries_.begin() + end));
      entries_.Add(std::make_move_iterator(entry_list.begin()), std::make_move_iterator(entry_list.end()));
    }
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
  std::uint64_t offset_;
};

static_assert(std::is_move_constructible_v<raftpb::snapshot>, "raftpb::snapshot is not move constructible");
static_assert(std::is_move_assignable_v<raftpb::snapshot>, "raftpb::snapshot is not move assignable");

}  // namespace lepton

#endif  // _LEPTON_RAFT_LOG_UNSTABLE_H_
