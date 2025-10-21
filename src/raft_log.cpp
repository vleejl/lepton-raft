#include "raft_log.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cassert>
#include <utility>

#include "absl/types/span.h"
#include "config.h"
#include "leaf.h"
#include "lepton_error.h"
#include "log.h"
#include "logger.h"
#include "protobuf.h"
#include "raft.pb.h"
#include "types.h"

namespace lepton {
raft_log::raft_log(pro::proxy<storage_builer>&& storage, std::uint64_t first_index, std::uint64_t last_index,
                   std::uint64_t max_applying_ents_size, std::shared_ptr<lepton::logger_interface> logger)
    : storage_(std::move(storage)),
      unstable_(last_index + 1, last_index + 1, logger),
      committed_(first_index - 1),
      applying_(first_index - 1),
      applied_(first_index - 1),
      max_applying_ents_size_(max_applying_ents_size),
      logger_(std::move(logger)) {}

std::string raft_log::string() {
  return fmt::format("committed={}, applied={}, applying={}, unstable.offset={}, len(unstable.Entries)={}", committed_,
                     applied_, applying_, unstable_.offset(), unstable_.entries_view().size());
}

leaf::result<std::uint64_t> raft_log::maybe_append(pb::log_slice&& log_slice, std::uint64_t committed) {
  if (match_term(log_slice.prev)) {
    auto lastnewi = log_slice.prev.index + static_cast<std::uint64_t>(log_slice.entries.size());
    auto ci = find_conflict(absl::MakeSpan(log_slice.entries));
    if (ci == 0) {  // 没有冲突
      // do nothing
    } else if (ci <= committed_) {
      // 表示冲突的条目是已经提交的条目，Raft
      // 协议要求已提交的条目不应该发生冲突。
      LEPTON_CRITICAL("entry {} conflict with committed entry [committed({})]", ci, committed_);
    } else {
      // 如果冲突位置在未提交的条目中，则从冲突的位置开始，将冲突后的新条目追加到日志中，确保不会丢失已提交的日志
      auto offset = log_slice.prev.index + 1;
      assert(ci >= offset);
      auto start = static_cast<std::ptrdiff_t>(ci - offset);
      if (start > log_slice.entries.size()) {
        LEPTON_CRITICAL("index, {}, is out of range [{}]", start, log_slice.entries.size());
      }
      auto sub_entries = pb::repeated_entry(std::make_move_iterator(log_slice.entries.begin() + start),
                                            std::make_move_iterator(log_slice.entries.end()));
      /*
      截断不影响 lastnewi 的语义：

当发生截断（如 ci > l.committed）时，函数会追加
ents[ci-offset:]（即冲突位置之后的新 entries）。

这些 entries 的索引范围是 index+1 到
index+len(ents)，即使被截断，最终覆盖的日志范围仍会延伸到 index + len(ents)。

因此，lastnewi 的值是逻辑上的“预期最后索引”，而非实际追加的条目数，这符合 Raft
协议中 Leader 强制覆盖 Follower 日志的规则。

正确性验证：

假设 index=10，len(ents)=5，则 lastnewi=15。

若冲突发生在 ci=12，截断后追加 ents[1:]（4 个 entries），其索引为 12 到 15。

最终日志的最后一个索引仍是 15，与 lastnewi 一致。

设计意图：

lastnewi 用于通知调用方这些 entries 应覆盖到的位置，而非实际追加的数量。

Raft 要求 Leader 确保 Follower 的日志最终与 Leader 一致，即使需要截断。lastnewi
反映的是 Leader 视角的日志状态，而非 Follower 的实际操作细节
       */
      append(std::move(sub_entries));
    }
    commit_to(std::min(committed, lastnewi));
    return lastnewi;
  }
  return new_error(logic_error::INVALID_PARAM, "log index and log term not match current term");
}

std::uint64_t raft_log::append(pb::repeated_entry&& entries) {
  if (entries.empty()) {
    return last_index();
  }
  assert(entries[0].index() > 0);
  if (auto after = entries[0].index() - 1; after < committed_) {
    LEPTON_CRITICAL("after({}) is out of range [committed({})]", after, committed_);
  }
  unstable_.truncate_and_append(std::move(entries));
  return last_index();
}

std::uint64_t raft_log::find_conflict(pb::span_entry entries) {
  for (const auto& entry : entries) {
    if (!match_term(pb::pb_entry_id(entry))) {
      if (entry->index() <= last_index()) {
        LOG_INFO(logger_,
                 "found conflict at index {} [existing term: {}, conflicting term: "
                 "{}]",
                 entry->index(), zero_term_on_err_compacted(entry->index()), entry->term());
      }
      return entry->index();
    }
  }
  return 0;
}

std::tuple<std::uint64_t, std::uint64_t> raft_log::find_conflict_by_term(std::uint64_t index, std::uint64_t term) {
  for (; index > 0; index--) {
    auto our_term = this->term(index);
    if (our_term.has_error()) {
      return {index, 0};
    }
    if (our_term.value() <= term) {
      return {index, our_term.value()};
    }
  }
  return {0, 0};
}

pb::repeated_entry raft_log::next_committed_ents(bool allow_unstable) const {
  if (applying_ents_paused_) {
    return {};
  }
  if (has_next_or_in_progress_snapshot()) {
    // See comment in hasNextCommittedEnts.
    return {};
  }
  auto lo = applying_ + 1;
  auto hi = max_appliable_index(allow_unstable) + 1;
  if (lo >= hi) {
    // Nothing to apply.
    return {};
  }
  auto max_size = max_applying_ents_size_ - applying_ents_size_;
  if (max_size <= 0) {
    LEPTON_CRITICAL("applying entry size ({}-{})={} not positive", max_applying_ents_size_, applying_ents_size_,
                    max_size);
  }
  auto entries = leaf::try_handle_some(
      [&]() -> leaf::result<pb::repeated_entry> {
        BOOST_LEAF_AUTO(v, slice(lo, hi, max_size));
        return v;
      },
      [&](const lepton_error& e) -> leaf::result<pb::repeated_entry> {
        LEPTON_CRITICAL(e.message);
        return new_error(e);
      });
  assert(entries.has_value());
  return entries.value();
}

bool raft_log::has_next_committed_ents(bool allow_unstable) const {
  if (applying_ents_paused_) {
    // Entry application outstanding size limit reached.
    return false;
  }

  if (has_next_or_in_progress_snapshot()) {
    // If we have a snapshot to apply, don't also return any committed
    // entries. Doing so raises questions about what should be applied
    // first.
    return false;
  }
  auto lo = applying_ + 1;
  auto hi = max_appliable_index(allow_unstable) + 1;
  return lo < hi;
}

std::uint64_t raft_log::max_appliable_index(bool allow_unstable) const {
  auto hi = committed_;
  if (!allow_unstable) {
    hi = std::min(hi, unstable_.offset() - 1);
  }
  return hi;
}

leaf::result<raftpb::snapshot> raft_log::snapshot() const {
  if (unstable_.has_snapshot()) {
    const auto& v = unstable_.snapshot_view();
    return raftpb::snapshot{v};
  }
  return storage_->snapshot();
}

std::uint64_t raft_log::first_index() const {
  if (auto result = unstable_.maybe_first_index(); result.has_value()) {
    return result.value();
  }

  if (auto index = storage_->first_index(); index.has_value()) {
    return index.value();
  }
  LEPTON_CRITICAL("unreachable case");  // TODO(bdarnell)
  return 0;
}

std::uint64_t raft_log::last_index() const {
  if (auto result = unstable_.maybe_last_index(); result.has_value()) {
    return result.value();
  }

  if (auto index = storage_->last_index(); index.has_value()) {
    return index.value();
  } else {
    LEPTON_CRITICAL("unreachable case");  // TODO(bdarnell)
    return 0;
  }
}

leaf::result<std::uint64_t> raft_log::storage_last_index() const { return storage_->last_index(); }

void raft_log::commit_to(std::uint64_t tocommit) {
  // never decrease commit
  if (committed_ < tocommit) {
    if (last_index() < tocommit) {
      LEPTON_CRITICAL(
          "tocommit({}) is out of range [lastIndex({})]. Was the raft log "
          "corrupted, truncated, or lost?",
          tocommit, last_index());
    }
    committed_ = tocommit;
  }
}

void raft_log::applied_to(std::uint64_t i, pb::entry_encoding_size size) {
  if (committed_ < i || i < applied_) {
    LEPTON_CRITICAL("applied({}) is out of range [committed({}), applied({})]", i, committed_, applied_);
  }
  applied_ = i;
  applying_ = std::max(applying_, i);
  if (applying_ents_size_ > size) {
    applying_ents_size_ -= size;
  } else {
    // Defense against underflow.
    applying_ents_size_ = 0;
  }
  applying_ents_paused_ = applying_ents_size_ >= max_applying_ents_size_;
}

void raft_log::accept_applying(std::uint64_t i, pb::entry_encoding_size size, bool allow_unstable) {
  if (committed_ < i || i < applied_) {
    LEPTON_CRITICAL("applied({}) is out of range [prevApplying({}), committed({})]", i, applying_, committed_);
  }
  applying_ = i;
  applying_ents_size_ += size;
  // Determine whether to pause entry application until some progress is
  // acknowledged. We pause in two cases:
  // 1. the outstanding entry size equals or exceeds the maximum size.
  // 2. the outstanding entry size does not equal or exceed the maximum size,
  //    but we determine that the next entry in the log will push us over the
  //    limit. We determine this by comparing the last entry returned from
  //    raftLog.nextCommittedEnts to the maximum entry that the method was
  //    allowed to return had there been no size limit. If these indexes are
  //    not equal, then the returned entries slice must have been truncated to
  //    adhere to the memory limit.
  applying_ents_paused_ = applying_ents_size_ >= max_applying_ents_size_ || i < max_appliable_index(allow_unstable);
}

pb::entry_id raft_log::last_entry_id() const {
  auto index = last_index();
  leaf::result<std::uint64_t> r = leaf::try_handle_some(
      [&]() -> leaf::result<std::uint64_t> {
        BOOST_LEAF_AUTO(v, term(index));
        return v;
      },
      [&](const lepton_error& e) -> leaf::result<std::uint64_t> {
        LEPTON_CRITICAL("unexpected error when getting the last term at {}: ({})", index, e.message);
        return new_error(e);
      });
  assert(!r.has_error());
  return pb::entry_id{r.value(), index};
}

leaf::result<std::uint64_t> raft_log::term(std::uint64_t i) const {
  // Check the unstable log first, even before computing the valid term range,
  // which may need to access stable Storage. If we find the entry's term in
  // the unstable log, we know it was in the valid range.
  if (auto t = unstable_.maybe_term(i); t.has_value()) {
    return t.value();
  }

  // The valid term range is [firstIndex-1, lastIndex]. Even though the entry at
  // firstIndex-1 is compacted away, its term is available for matching purposes
  // when doing log appends.
  auto first_idx = first_index();
  assert(first_idx > 0);
  auto dummy_index = first_idx - 1;
  auto last_idx = last_index();
  if (i < dummy_index) {
    return new_error(storage_error::COMPACTED);
  }
  if (i > last_idx) {
    return new_error(storage_error::UNAVAILABLE);
  }

  leaf::result<std::uint64_t> r = leaf::try_handle_some(
      [&]() -> leaf::result<std::uint64_t> {
        BOOST_LEAF_AUTO(v, storage_->term(i));
        return v;
      },
      [](const lepton_error& e) -> leaf::result<std::uint64_t> {
        if (e.err_code == storage_error::COMPACTED || e.err_code == storage_error::UNAVAILABLE) {
          return new_error(e);
        }
        LEPTON_CRITICAL(e.message);
        return new_error(e);
      });
  return r;
}

leaf::result<pb::repeated_entry> raft_log::entries(std::uint64_t i, std::uint64_t max_size) {
  if (i > last_index()) {
    return {};
  }
  return slice(i, last_index() + 1, max_size);
}

// 仅测试场景使用
pb::repeated_entry raft_log::all_entries() {
  auto ents = leaf::try_handle_some(
      [&]() -> leaf::result<pb::repeated_entry> {
        BOOST_LEAF_AUTO(v, entries(first_index(), NO_LIMIT));
        return v;
      },
      [&](const lepton_error& e) -> leaf::result<pb::repeated_entry> {
        // try again if there was a racing compaction
        if (e.err_code == storage_error::COMPACTED) {
          return all_entries();
        }
        LEPTON_CRITICAL(e.message);
      });
  assert(ents.has_value());
  return ents.value();
}

bool raft_log::is_up_to_date(const pb::entry_id& their) {
  auto our = last_entry_id();
  return their.term > our.term || (their.term == our.term && their.index >= our.index);
}

bool raft_log::match_term(const pb::entry_id& id) {
  auto result = term(id.index);
  if (result.has_error()) {
    return false;
  }
  auto result_val = *result;
  return result_val == id.term;
}

bool raft_log::maybe_commit(const pb::entry_id& at) {
  // NB: term should never be 0 on a commit because the leader campaigned at
  // least at term 1. But if it is 0 for some reason, we don't consider this a
  // term match.
  if (at.term != 0 && at.index > committed_ && match_term(at)) {
    commit_to(at.index);
    return true;
  }
  return false;
}

void raft_log::restore(raftpb::snapshot&& snapshot) {
  LOG_INFO(logger_, "log [{}] starts to restore snapshot [index: {}, term: {}]", string(), snapshot.metadata().index(),
           snapshot.metadata().term());
  committed_ = snapshot.metadata().index();
  unstable_.restore(std::move(snapshot));
}

leaf::result<void> raft_log::scan(std::uint64_t lo, std::uint64_t hi, pb::entry_encoding_size page_size,
                                  std::function<leaf::result<void>(const pb::repeated_entry& entries)> callback) const {
  while (lo < hi) {
    BOOST_LEAF_AUTO(v, slice(lo, hi, page_size));
    if (v.empty()) {
      return new_error(logic_error::EMPTY_ARRAY);
    }
    LEPTON_LEAF_CHECK(callback(v));
    lo += static_cast<std::uint64_t>(v.size());
  }
  return {};
}

leaf::result<pb::repeated_entry> raft_log::slice(std::uint64_t lo, std::uint64_t hi,
                                                 pb::entry_encoding_size max_size) const {
  LEPTON_LEAF_CHECK(must_check_out_of_bounds(lo, hi));
  if (lo == hi) {
    return {};
  }
  const auto unstable_offset = unstable_.offset();
  if (lo >= unstable_offset) {
    auto ents = unstable_.slice(lo, hi);
    ents = pb::limit_entry_size(ents, max_size);
    // NB: use the full slice expression to protect the unstable slice from
    // appends to the returned ents slice.
    return pb::convert_span_entry(ents);
  }

  const auto cut = std::min(hi, unstable_offset);

  auto storage_entries = leaf::try_handle_some(
      [&]() -> leaf::result<pb::repeated_entry> {
        BOOST_LEAF_AUTO(v, storage_->entries(lo, cut, max_size););
        return v;
      },
      [&](const lepton_error& e) -> leaf::result<pb::repeated_entry> {
        if (e.err_code.category() == storage_error_category()) {
          if (e.err_code == storage_error::COMPACTED) {
            return new_error(e);
          }
          if (e.err_code == storage_error::UNAVAILABLE) {
            LEPTON_CRITICAL("entries:[{},{}] is unavaliable from storage", lo, std::min(hi, unstable_offset));
          }
        }
        // TODO(pavelkalinnikov): handle errors uniformly
        LEPTON_CRITICAL(e.message);
        return new_error(e);
      });
  if (storage_entries.has_error()) {
    return storage_entries;
  }
  if (hi <= unstable_offset) {
    return storage_entries;
  }

  // Fast path to check if ents has reached the size limitation. Either the
  // returned slice is shorter than requested (which means the next entry would
  // bring it over the limit), or a single entry reaches the limit.
  if (auto size = static_cast<std::uint64_t>(storage_entries->size()); size < std::min(hi, unstable_offset) - lo) {
    return storage_entries;
  }

  // Slow path computes the actual total size, so that unstable entries are cut
  // optimally before being copied to ents slice.
  auto size = pb::ent_size(storage_entries.value());
  if (size >= max_size) {
    return storage_entries;
  }

  auto unstable = unstable_.entries_span(std::max(lo, unstable_offset), hi);
  unstable = pb::limit_entry_size(unstable, max_size - size);
  // Total size of unstable may exceed maxSize-size only if len(unstable) == 1.
  // If this happens, ignore this extra entry.
  if ((unstable.size() == 1) && ((size + pb::ent_size(unstable)) > max_size)) {
    return storage_entries;
  }
  // Otherwise, total size of unstable does not exceed maxSize-size, so total
  // size of ents+unstable does not exceed maxSize. Simply concatenate them.
  return pb::extend(storage_entries.value(), unstable);
}

leaf::result<void> raft_log::must_check_out_of_bounds(std::uint64_t lo, std::uint64_t hi) const {
  if (lo > hi) {
    LEPTON_CRITICAL("invalid slice {} > {}", lo, hi);
  }
  auto fi = first_index();
  if (lo < fi) {
    return new_error(storage_error::COMPACTED);
  }

  auto li = last_index();
  auto length = li + 1 - fi;
  if (hi > length + fi) {
    LEPTON_CRITICAL("slice[{},{}] out of bounds [{},{}]", lo, hi, fi, li);
  }
  return {};
}

std::uint64_t raft_log::zero_term_on_err_compacted(std::uint64_t i) const {
  leaf::result<std::uint64_t> r = leaf::try_handle_some(
      [&]() -> leaf::result<std::uint64_t> {
        BOOST_LEAF_AUTO(v, term(i));
        return v;
      },
      [](const lepton_error& e) -> leaf::result<std::uint64_t> {
        if (e.err_code == storage_error::COMPACTED) {
          return 0;
        }
        if (e.err_code == storage_error::UNAVAILABLE) {
          return 0;
        }
        LEPTON_CRITICAL("unexpected error {}", e.message);
      });
  assert(!r.has_error());
  return r.value();
}

leaf::result<raft_log> new_raft_log_with_size(pro::proxy<storage_builer>&& storage,
                                              std::shared_ptr<lepton::logger_interface> logger,
                                              pb::entry_encoding_size max_applying_ents_size) {
  if (!storage.has_value()) {
    return new_error(logic_error::NULL_POINTER, "storage must not be nil");
  }

  BOOST_LEAF_AUTO(first_index, storage->first_index());
  BOOST_LEAF_AUTO(last_index, storage->last_index());
  return raft_log{std::move(storage), first_index, last_index, max_applying_ents_size, std::move(logger)};
}
}  // namespace lepton
