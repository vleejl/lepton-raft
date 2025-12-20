#pragma once
#ifndef _LEPTON_MEMORY_STORAGE_H_
#define _LEPTON_MEMORY_STORAGE_H_
#include <mutex>
#include <optional>

#include "basic/utility_macros.h"
#include "error/lepton_error.h"
#include "raft.pb.h"
#include "raft_core/pb/types.h"
namespace lepton::core {
// MemoryStorage implements the Storage interface backed by an
// in-memory array.
class memory_storage {
 private:
  NOT_COPYABLE(memory_storage)
  auto _first_index() const;
  auto _last_index() const;

 public:
  memory_storage();
  memory_storage(memory_storage&& ms)
      : hard_state_(std::move(ms.hard_state_)), snapshot_(std::move(ms.snapshot_)), ents_(std::move(ms.ents_)){};
#ifdef LEPTON_TEST
  explicit memory_storage(const pb::repeated_entry& ents) : ents_(ents) {}

  raftpb::snapshot& snapshot_ref() { return snapshot_; }

  auto& mutable_ents() { return ents_; }
#endif
  leaf::result<std::tuple<raftpb::hard_state, raftpb::conf_state>> initial_state() const;

  const pb::repeated_entry& entries_view() const;

  leaf::result<void> set_hard_state(raftpb::hard_state&& hard_state);

  leaf::result<pb::repeated_entry> entries(std::uint64_t lo, std::uint64_t hi, std::uint64_t max_size) const;

  leaf::result<pb::span_entry> entries_view(std::uint64_t lo, std::uint64_t hi, std::uint64_t max_size) const;

  leaf::result<std::uint64_t> term(std::uint64_t i) const;

  leaf::result<std::uint64_t> last_index() const;

  leaf::result<std::uint64_t> first_index() const;

  leaf::result<raftpb::snapshot> snapshot() const;

  // ApplySnapshot overwrites the contents of this Storage object with
  // those of the given snapshot.
  leaf::result<void> apply_snapshot(raftpb::snapshot&& snapshot);

  // CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
  // can be used to reconstruct the state at that point.
  // If any configuration changes have been made since the last compaction,
  // the result of the last ApplyConfChange must be passed in.
  leaf::result<raftpb::snapshot> create_snapshot(std::uint64_t i, std::optional<raftpb::conf_state> cs,
                                                 std::string&& data);

  // Compact discards all log entries prior to compactIndex.
  // It is the application's responsibility to not attempt to compact an index
  // greater than raftLog.applied.
  leaf::result<void> compact(std::uint64_t compact_index);

  // Append the new entries to storage.
  // TODO (xiangli): ensure the entries are continuous and
  // entries[0].Index > ms.entries[0].Index
  leaf::result<void> append(pb::repeated_entry&& entries);

 private:
  // Protects access to all fields. Most methods of MemoryStorage are
  // run on the raft goroutine, but Append() is run on an application
  // goroutine.
  mutable std::mutex mutex_;

  raftpb::hard_state hard_state_;
  raftpb::snapshot snapshot_;

  // ents[i] has raft log position i+snapshot.Metadata.Index
  pb::repeated_entry ents_;
};
}  // namespace lepton::core

#endif  // _LEPTON_MEMORY_STORAGE_H_
