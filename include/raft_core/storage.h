#ifndef _LEPTON_STORAGE_H_
#define _LEPTON_STORAGE_H_
#include <proxy.h>
#include <raft.pb.h>

#include <cstdint>

#include "lepton_error.h"
#include "types.h"
namespace lepton::core {

// InitialState returns the saved HardState and ConfState information.
// 描述：此方法返回保存的硬件状态 (HardState) 和配置状态 (ConfState)。
// 返回值：
// pb.HardState：包含关于 Raft 状态机的当前硬件状态，如当前的任期号、投票等。
// pb.ConfState：配置状态，表示当前的配置，可能包括节点的集合。
// error：如果出错，则返回错误。
PRO_DEF_MEM_DISPATCH(storage_initial_state, initial_state);

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
// 描述：此方法返回从 lo 到 hi 范围内的日志条目，lo 是起始索引，hi
// 是结束索引。maxSize 限制返回的日志条目的总大小，确保不会返回过多数据。
// 返回值：
// []pb.Entry：日志条目的切片，表示指定范围内的日志条目。
// error：如果出错，则返回错误。
PRO_DEF_MEM_DISPATCH(storage_entries, entries);

PRO_DEF_MEM_DISPATCH(storage_entries_view, entries_view);

// Term returns the term of entry i, which must be in the range
// [FirstIndex()-1, LastIndex()]. The term of the entry before
// FirstIndex is retained for matching purposes even though the
// rest of that entry may not be available.
// 描述：此方法返回指定索引 i 的日志条目的任期号。这个任期号应该在
// [FirstIndex()-1, LastIndex()] 范围内。
// 返回值： uint64：指定索引 i
// 的日志条目的任期号。
// error：如果出错，则返回错误。
PRO_DEF_MEM_DISPATCH(storage_term, term);

// LastIndex returns the index of the last entry in the log.
// 描述：此方法返回日志中最后一个条目的索引。
// 返回值：
// uint64：日志中最后一个条目的索引。
// error：如果出错，则返回错误。
PRO_DEF_MEM_DISPATCH(storage_last_index, last_index);

// FirstIndex returns the index of the first log entry that is
// possibly available via Entries (older entries have been incorporated
// into the latest Snapshot; if storage only contains the dummy entry the
// first log entry is not available).
// 描述：此方法返回日志中第一个条目的索引。日志条目的索引会随着快照的应用而变化，某些条目可能被归档到快照中。
// 返回值：
// uint64：日志中第一个条目的索引。
PRO_DEF_MEM_DISPATCH(storage_first_index, first_index);

// Snapshot returns the most recent snapshot.
// If snapshot is temporarily unavailable, it should return
// ErrSnapshotTemporarilyUnavailable, so raft state machine could know that
// Storage needs some time to prepare snapshot and call Snapshot later.
// 描述：此方法返回最近的快照。如果当前快照不可用，应该返回
// ErrSnapshotTemporarilyUnavailable 错误，Raft
// 状态机会知道需要稍后再次调用此方法。 返回值： pb.Snapshot：最近的快照。
PRO_DEF_MEM_DISPATCH(storage_snapshot, snapshot);

// Storage is an interface that may be implemented by the application
// to retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will
// become inoperable and refuse to participate in elections; the
// application is responsible for cleanup and recovery in this case.

// TODO(tbg): split this into two interfaces, LogStorage and StateStorage.
// 可以参考 hashicorp raft 的定义实现

// clang-format off
struct storage_builer : pro::facade_builder 
  ::add_convention<storage_initial_state, leaf::result<std::tuple<raftpb::hard_state, raftpb::conf_state>>() const> 
  ::add_convention<storage_entries, leaf::result<pb::repeated_entry>(std::uint64_t lo, std::uint64_t hi, std::uint64_t max_size) const> 
  ::add_convention<storage_entries_view, leaf::result<pb::span_entry>(std::uint64_t lo, std::uint64_t hi, std::uint64_t max_size) const>   
  ::add_convention<storage_term, leaf::result<std::uint64_t>(std::uint64_t i) const> 
  ::add_convention<storage_last_index, leaf::result<std::uint64_t>() const> 
  ::add_convention<storage_first_index, leaf::result<std::uint64_t>() const> 
  ::add_convention<storage_snapshot, leaf::result<raftpb::snapshot>() const> 
  ::add_skill<pro::skills::as_view>
  ::build{};
// clang-format on
}  // namespace lepton::core

#endif  // _LEPTON_STORAGE_H_
