#include "raft_log.h"

#include <fmt/core.h>
#include <fmt/format.h>

#include <algorithm>
#include <cassert>
#include <utility>

#include "absl/types/span.h"
#include "config.h"
#include "error.h"
#include "leaf.hpp"
#include "protobuf.h"
#include "raft.pb.h"
#include "spdlog/spdlog.h"

namespace lepton {
raft_log::raft_log(pro::proxy_view<storage_builer> storage,
                   std::uint64_t offset, std::uint64_t committed,
                   std::uint64_t applied, std::uint64_t max_next_ents_size)
    : storage_(storage),
      unstable_(offset),
      committed_(committed),
      applied_(applied),
      max_next_ents_size_(max_next_ents_size) {}

std::string raft_log::string() {
  return fmt::format(
      "committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d",
      committed_, applied_, unstable_.offset(),
      unstable_.entries_view().size());
}

std::uint64_t raft_log::first_index() const {
  if (auto result = unstable_.maybe_first_index(); result.has_value()) {
    return result.value();
  }

  if (auto index = storage_->first_index(); index.has_value()) {
    return index.value();
  }
  panic("unreachable case");  // TODO(bdarnell)
  return 0;
}

std::uint64_t raft_log::last_index() {
  if (auto result = unstable_.maybe_last_index(); result.has_value()) {
    return result.value();
  }

  if (auto index = storage_->last_index(); index.has_value()) {
    return index.value();
  } else {
    panic("unreachable case");  // TODO(bdarnell)
    return 0;
  }
}

void raft_log::commit_to(std::uint64_t tocommit) {
  // never decrease commit
  if (committed_ < tocommit) {
    if (last_index() < tocommit) {
      spdlog::critical(
          "tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log "
          "corrupted, truncated, or lost?",
          tocommit, last_index());
    }
    committed_ = tocommit;
  }
}

void raft_log::applied_to(std::uint64_t i) {
  if (i == 0) {
    return;
  }
  if (committed_ < i || i < applied_) {
    spdlog::critical(
        "applied(%d) is out of range [prevApplied(%d), committed(%d)]", i,
        applied_, committed_);
  }
  applied_ = i;
}

leaf::result<std::uint64_t> raft_log::term(std::uint64_t i) {
  // the valid term range is [index of dummy entry, last index]
  auto first_idx = first_index();
  assert(first_idx > 0);
  auto dummy_index = first_idx - 1;
  auto last_idx = last_index();
  if (i < dummy_index || i > last_idx) {
    // TODO: return an error instead?
    return 0;
  }

  if (auto t = unstable_.maybe_term(i); t.has_value()) {
    return t.value();
  }
  leaf::result<std::uint64_t> r = leaf::try_handle_some(
      [&]() -> leaf::result<std::uint64_t> {
        BOOST_LEAF_AUTO(v, storage_->term(i));
        return v;
      },
      [](const lepton_error& e) -> leaf::result<std::uint64_t> {
        if (e.err_code == storage_error::COMPACTED ||
            e.err_code == storage_error::UNAVAILABLE) {
          return new_error(e);
        }
        panic(e.message);
        return new_error(e);
      });
  return r;
}

std::uint64_t raft_log::last_term() {
  leaf::result<std::uint64_t> r = leaf::try_handle_some(
      [&]() -> leaf::result<std::uint64_t> {
        BOOST_LEAF_AUTO(v, term(last_index()));
        return v;
      },
      [](const lepton_error& e) -> leaf::result<std::uint64_t> {
        spdlog::critical("unexpected error when getting the last term ({})",
                         e.message);
        return new_error(e);
      });
  assert(!r.has_error());
  return r.value();
}

bool raft_log::match_term(std::uint64_t i, std::uint64_t t) {
  auto result = term(i);
  if (result.has_error()) {
    return false;
  }
  return result.value() == t;
}

bool raft_log::is_up_to_date(std::uint64_t lasti, std::uint64_t term) {
  auto curr_last_term = last_term();
  if (term > curr_last_term) {
    return true;
  }
  if (term == curr_last_term && lasti >= last_index()) {
    return true;
  }
  return false;
}

std::uint64_t raft_log::zero_term_on_err_compacted(std::uint64_t i) {
  leaf::result<std::uint64_t> r = leaf::try_handle_some(
      [&]() -> leaf::result<std::uint64_t> {
        BOOST_LEAF_AUTO(v, term(i));
        return v;
      },
      [](const lepton_error& e) -> leaf::result<std::uint64_t> {
        if (e.err_code == storage_error::COMPACTED) {
          return 0;
        }
        panic(fmt::format("unexpected error {}", e.message));
      });
  assert(!r.has_error());
  return r.value();
}

bool raft_log::maybe_commit(std::uint64_t max_index, std::uint64_t term) {
  if (max_index > committed_ && zero_term_on_err_compacted(max_index) == term) {
    commit_to(max_index);
    return true;
  }
  return false;
}

void raft_log::restore(pb::snapshot_ptr&& snapshot) {
  spdlog::info("log [%s] starts to restore snapshot [index: %d, term: %d]",
               string(), snapshot->metadata().index(),
               snapshot->metadata().term());
  committed_ = snapshot->metadata().index();
  unstable_.restore(std::move(snapshot));
}

std::uint64_t raft_log::find_conflict(
    absl::Span<const raftpb::entry* const> entries) {
  for (const auto& entry : entries) {
    if (!match_term(entry->index(), entry->term())) {
      if (entry->index() <= last_index()) {
        spdlog::info(
            "found conflict at index {} [existing term: {}, conflicting term: "
            "{}]",
            entry->index(), zero_term_on_err_compacted(entry->index()),
            entry->term());
      }
      return entry->index();
    }
  }
  return 0;
}

std::uint64_t raft_log::find_conflict_by_term(std::uint64_t index,
                                              std::uint64_t term) {
  if (auto li = last_index(); li > term) {
    spdlog::warn(
        "index(%d) is out of range [0, lastIndex(%d)] in findConflictByTerm",
        index, li);
    // NB: such calls should not exist, but since there is a straightfoward
    // way to recover, do it.
    //
    // It is tempting to also check something about the first index, but
    // there is odd behavior with peers that have no log, in which case
    // lastIndex will return zero and firstIndex will return one, which
    // leads to calls with an index of zero into this method.
    return index;
  }
  while (true) {
    auto log_term = this->term(index);
    if (log_term.has_error()) {
      break;
    }
    if (log_term.value() <= term) {
      break;
    }
    assert(index > 0);
    index--;
  }
  return index;
}

absl::Span<const raftpb::entry* const> raft_log::unstable_entries() {
  if (unstable_.entries_view().empty()) {
    return {};
  }
  return unstable_.entries_span();
}

leaf::result<void> raft_log::must_check_out_of_bounds(std::uint64_t lo,
                                                      std::uint64_t hi) {
  if (lo > hi) {
    spdlog::critical("invalid slice {} > {}", lo, hi);
  }
  auto fi = first_index();
  if (lo < fi) {
    return new_error(storage_error::COMPACTED);
  }

  auto li = last_index();
  auto length = li + 1 - fi;
  if (hi > length + fi) {
    spdlog::critical("slice[{},{}] out of bounds [{},{}]", lo, hi, fi, li);
  }
  return {};
}

leaf::result<pb::repeated_entry> raft_log::slice(std::uint64_t lo,
                                                 std::uint64_t hi,
                                                 std::uint64_t max_size) {
  BOOST_LEAF_CHECK(must_check_out_of_bounds(lo, hi));
  if (lo == hi) {
    return {};
  }
  const auto unstable_offset = unstable_.offset();
  pb::repeated_entry ents;
  if (lo < unstable_offset) {
    auto storage_entries = leaf::try_handle_some(
        [&]() -> leaf::result<pb::repeated_entry> {
          BOOST_LEAF_AUTO(v, storage_->entries(
                                 lo, std::min(hi, unstable_offset), max_size););
          return std::move(v);
        },
        [&](const lepton_error& e) -> leaf::result<pb::repeated_entry> {
          if (e.err_code.category() == storage_error_category()) {
            if (e.err_code == storage_error::COMPACTED) {
              return new_error(e);
            }
            if (e.err_code == storage_error::UNAVAILABLE) {
              spdlog::critical("entries:[{},{}] is unavaliable from storage",
                               lo, std::min(hi, unstable_offset));
            }
          }
          panic(e.message);
          return new_error(e);
        });
    if (storage_entries.has_error()) {
      return storage_entries;
    }
    // check if ents has reached the size limitation
    if (storage_entries->size() < std::min(hi, unstable_offset) - lo) {
      return storage_entries;
    }
    ents = std::move(storage_entries.value());
  }
  if (hi > unstable_offset) {
    auto unstable = unstable_.entries_span(std::max(lo, unstable_offset), hi);
    if (!ents.empty()) {
      for (const auto& entry : unstable) {
        ents.Add()->CopyFrom(*entry);
      }
    }
  }
  return ents;
}

pb::repeated_entry raft_log::next_ents() {
  auto off = std::max(applied_ + 1, first_index());
  if (committed_ + 1 > off) {
    auto entries = leaf::try_handle_some(
        [&]() -> leaf::result<pb::repeated_entry> {
          BOOST_LEAF_AUTO(v, slice(off, committed_ + 1, max_next_ents_size_));
          return v;
        },
        [&](const lepton_error& e) -> leaf::result<pb::repeated_entry> {
          panic(e.message);
          return new_error(e);
        });
    assert(entries.has_error());
    return entries.value();
  }
  return {};
}

bool raft_log::has_next_ents() const {
  auto off = std::max(applied_ + 1, first_index());
  return committed_ + 1 > off;
}

leaf::result<pb::repeated_entry> raft_log::entries(std::uint64_t i,
                                                   std::uint64_t max_size) {
  if (i > last_index()) {
    return {};
  }
  return slice(i, last_index(), max_size);
}

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
        panic(e.message);
      });
  assert(ents.has_error());
  return ents.value();
}

leaf::result<raftpb::snapshot> raft_log::snapshot() const {
  if (unstable_.snapshot_view() != nullptr) {
    const auto& v = *unstable_.snapshot_view();
    return raftpb::snapshot{v};
  }
  return storage_->snapshot();
}

std::uint64_t raft_log::append(pb::repeated_entry&& entries) {
  if (entries.empty()) {
    return last_index();
  }
  assert(entries[0].index() > 0);
  if (auto after = entries[0].index() - 1; after < committed_) {
    spdlog::critical("after({}) is out of range [committed({})]", after,
                     committed_);
  }
  unstable_.truncate_and_append(std::move(entries));
  return last_index();
}

leaf::result<std::uint64_t> raft_log::maybe_append(
    std::uint64_t index, std::uint64_t log_term, std::uint64_t committed,
    pb::repeated_entry&& enrties) {
  if (match_term(index, log_term)) {
    auto lastnewi = index + static_cast<std::uint64_t>(enrties.size());
    auto ci = find_conflict(absl::MakeSpan(enrties));
    if (ci == 0) {
      // do nothing
    } else if (ci <= committed_) {
      // 表示冲突的条目是已经提交的条目，Raft
      // 协议要求已提交的条目不应该发生冲突。
      spdlog::critical("entry %d conflict with committed entry [committed(%d)]",
                       ci, committed_);
    } else {
      // 如果冲突位置在未提交的条目中，则从冲突的位置开始，将冲突后的新条目追加到日志中，确保不会丢失已提交的日志
      auto offset = index + 1;
      assert(ci >= offset);
      auto start = static_cast<std::ptrdiff_t>(ci - offset);
      if (start > enrties.size()) {
        spdlog::critical("index, %d, is out of range [%d]", start,
                         enrties.size());
      }
      auto sub_entries =
          pb::repeated_entry(std::make_move_iterator(enrties.begin() + start),
                             std::make_move_iterator(enrties.end()));
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
    commit_to(std::min(committed_, lastnewi));
    return lastnewi;
  }
  return new_error(logic_error::INVALID_PARAM,
                   "log index and log term not match current term");
}

leaf::result<raft_log> new_raft_log_with_size(
    pro::proxy_view<storage_builer> storage, std::uint64_t max_next_ents_size) {
  if (!storage.has_value()) {
    return new_error(encoding_error::NULL_POINTER, "storage must not be nil");
  }

  BOOST_LEAF_AUTO(first_index, storage->first_index());
  BOOST_LEAF_AUTO(last_index, storage->last_index());
  return raft_log{storage, last_index + 1, first_index - 1, first_index - 1,
                  max_next_ents_size};
}
}  // namespace lepton
