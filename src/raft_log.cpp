#include "raft_log.h"

#include <fmt/core.h>
#include <fmt/format.h>

#include <cassert>
#include <utility>

#include "absl/types/span.h"
#include "error.h"
#include "leaf.hpp"
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

std::uint64_t raft_log::first_index() {
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
      [](const lepton_error<storage_error>& e) -> leaf::result<std::uint64_t> {
        if (e.err_code == storage_error::COMPACTED ||
            e.err_code == storage_error::UNAVAILABLE) {
          return new_error(e);
        }
        panic(e.message);
        return new_error(e);
      },
      [](const lepton_error<system_error>& e) -> leaf::result<std::uint64_t> {
        panic(e.message);
        return new_error(e);
      },
      [](const lepton_error<encoding_error>& e) -> leaf::result<std::uint64_t> {
        panic(e.message);
        return new_error(e);
      },
      [](const lepton_error<logic_error>& e) -> leaf::result<std::uint64_t> {
        panic(e.message);
        return new_error(e);
      });
  return r;
}

bool raft_log::match_term(std::uint64_t i, std::uint64_t t) {
  auto result = term(i);
  if (result.has_error()) {
    return false;
  }
  return result.value() == t;
}

std::uint64_t raft_log::zero_term_on_err_compacted(std::uint64_t i) {
  leaf::result<std::uint64_t> r = leaf::try_handle_some(
      [&]() -> leaf::result<std::uint64_t> {
        BOOST_LEAF_AUTO(v, term(i));
        return v;
      },
      [](const lepton_error<storage_error>& e) -> leaf::result<std::uint64_t> {
        if (e.err_code == storage_error::COMPACTED) {
          return 0;
        }
        panic(fmt::format("unexpected error {}", e.message));
        return new_error(e);
      });
  assert(!r.has_error());
  return r.value();
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
