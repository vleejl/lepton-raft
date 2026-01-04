#include "raft_core/memory_storage.h"

#include <cassert>
#include <cstddef>
#include <cstdint>

#include "basic/log.h"
#include "raft_core/pb/protobuf.h"

namespace lepton::core {

// first index + 1 是因为第一个 entry 是 dummy entry
auto memory_storage::_first_index() const {
  assert(!ents_.empty());
  return ents_[0].index() + 1;
}

auto memory_storage::_last_index() const {
  assert(!ents_.empty());
  return ents_[0].index() + static_cast<std::uint64_t>(ents_.size()) - 1;
}

memory_storage::memory_storage() {
  // When starting from scratch populate the list with a dummy entry at term
  // zero.
  // ​1. 解决日志索引从 1 开始的问题: 在 Raft
  // 协议中，日志条目的索引（Index）从 1 开始​（而不是0）。为了统一逻辑，MemoryStorage
  // 的实现会在初始化时插入一个索引为 0 的 dummy entry，使得实际的有效日志条目从索引 1 开始存储

  // 2. 处理日志压缩（Compaction）:当 Raft
  // 生成快照（Snapshot）后，旧的日志条目会被删除（压缩）。此时，MemoryStorage
  // 会保留 dummy entry 作为基准偏移量，确保后续的日志索引计算正确。
  /*
  例如：
    初始日志：[dummy(0), entry(1), entry(2), entry(3)]
    生成快照后，保留 entry(3) 的快照，并删除索引 ≤3 的日志。
    压缩后的日志变为：[dummy(3)]（新的 dummy 的
  Index=3，表示日志已压缩到此处）。 后续新追加的日志索引从 4
  开始，避免索引混乱。
  */

  // 3. 简化偏移量计算: 参见 slice 函数的实现，使用 dummy entry 的 log index
  // 作为基准offset

  // 4. 处理空日志的场景:如果 MemoryStorage 中只有 dummy
  // entry（例如刚初始化或日志被完全压缩），代码会返回
  // ErrUnavailable，表示没有有效日志可用
  auto entry = ents_.Add();
  entry->set_term(0);
  entry->set_index(0);
  entry->set_type(raftpb::EntryType::ENTRY_NORMAL);
  assert(!ents_.empty());
}

leaf::result<std::tuple<raftpb::HardState, raftpb::ConfState>> memory_storage::initial_state() const {
  return {hard_state_, snapshot_.metadata().conf_state()};
}

const pb::repeated_entry& memory_storage::entries_view() const { return ents_; }

leaf::result<void> memory_storage::set_hard_state(raftpb::HardState&& hard_state) {
  std::lock_guard<std::mutex> guard(mutex_);
  hard_state_ = std::move(hard_state);
  return {};
}

leaf::result<pb::repeated_entry> memory_storage::entries(std::uint64_t lo, std::uint64_t hi,
                                                         std::uint64_t max_size) const {
  std::lock_guard<std::mutex> guard(mutex_);
  const auto offset = ents_[0].index();
  if (lo <= offset) {
    return new_error(storage_error::COMPACTED);
  }
  const auto last_log_index = _last_index();
  if (hi > last_log_index + 1) {
    LEPTON_CRITICAL("entries' hi({}) is out of bound lastindex({})", hi, last_log_index);
  }

  // only cotanin dummy entry
  if (ents_.size() == 1) {
    return new_error(storage_error::UNAVAILABLE);
  }

  const int start = static_cast<int>(lo - offset);
  const int end = static_cast<int>(hi - offset);

  // 3. 手动拷贝子范围
  pb::repeated_entry sub_entries;
  sub_entries.Reserve(end - start);  // 预分配内存
  for (int i = start; i < end; ++i) {
    sub_entries.Add()->CopyFrom(ents_.Get(i));  // 深拷贝
  }
  return pb::limit_entry_size(sub_entries, max_size);
}

leaf::result<pb::span_entry> memory_storage::entries_view(std::uint64_t lo, std::uint64_t hi,
                                                          std::uint64_t max_size) const {
  std::lock_guard<std::mutex> guard(mutex_);
  const auto offset = ents_[0].index();
  if (lo <= offset) {
    return new_error(storage_error::COMPACTED);
  }
  const auto last_log_index = _last_index();
  if (hi > last_log_index + 1) {
    LEPTON_CRITICAL("entries' hi({}) is out of bound lastindex({})", hi, last_log_index);
  }

  // only cotanin dummy entry
  if (ents_.size() == 1) {
    return new_error(storage_error::UNAVAILABLE);
  }

  const int start = static_cast<int>(lo - offset);
  const int end = static_cast<int>(hi - offset);

  auto ents_view = absl::MakeSpan(ents_.data() + start, static_cast<std::size_t>(end - start));
  return pb::limit_entry_size(ents_view, max_size);
}

leaf::result<std::uint64_t> memory_storage::term(std::uint64_t i) const {
  std::lock_guard<std::mutex> guard(mutex_);
  const auto offset = ents_[0].index();
  if (i < offset) {
    return new_error(storage_error::COMPACTED);
  }
  const auto last_log_index = _last_index();
  if (i > last_log_index) {
    return new_error(storage_error::UNAVAILABLE);
  }
  return ents_[static_cast<int>(i - offset)].term();
}

leaf::result<std::uint64_t> memory_storage::last_index() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return _last_index();
}

leaf::result<std::uint64_t> memory_storage::first_index() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return _first_index();
}

leaf::result<raftpb::Snapshot> memory_storage::snapshot() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return snapshot_;
}

leaf::result<void> memory_storage::apply_snapshot(raftpb::Snapshot&& snapshot) {
  std::lock_guard<std::mutex> guard(mutex_);
  const auto index = snapshot.metadata().index();
  if (auto meta_index = snapshot_.metadata().index(); index <= meta_index) {
    return new_error(storage_error::SNAP_OUT_OF_DATE);
  }
  snapshot_ = std::move(snapshot);
  ents_.Clear();
  auto entry = ents_.Add();
  entry->set_term(snapshot_.metadata().term());
  entry->set_index(index);
  entry->set_type(raftpb::EntryType::ENTRY_NORMAL);
  return {};
}

leaf::result<raftpb::Snapshot> memory_storage::create_snapshot(std::uint64_t i, std::optional<raftpb::ConfState> cs,
                                                               std::string&& data) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (i <= snapshot_.metadata().index()) {
    return new_error(storage_error::SNAP_OUT_OF_DATE);
  }
  if (i > _last_index()) {
    LEPTON_CRITICAL("snapshot {} is out of bound lastindex({})", i, _last_index());
  }
  const auto offset = ents_[0].index();
  snapshot_.mutable_metadata()->set_index(i);
  snapshot_.mutable_metadata()->set_term(ents_[static_cast<int>(i - offset)].term());
  if (cs.has_value()) {
    snapshot_.mutable_metadata()->mutable_conf_state()->CopyFrom(cs.value());
  }
  snapshot_.set_data(std::move(data));
  return snapshot_;
}

leaf::result<void> memory_storage::compact(std::uint64_t compact_index) {
  std::lock_guard<std::mutex> guard(mutex_);
  assert(!ents_.empty());
  const auto offset = ents_.begin()->index();
  if (compact_index <= offset) {
    return new_error(storage_error::COMPACTED);
  }
  if (compact_index > _last_index()) {
    LEPTON_CRITICAL("compact {} is out of bound lastindex({})", compact_index, _last_index());
  }
  const auto start = static_cast<int>(compact_index - offset);
  pb::repeated_entry new_ents;
  const auto reserve_size = 1 + ents_.size() - start;
  assert(reserve_size > 0);
  new_ents.Reserve(reserve_size);
  auto first_dummy_entry = new_ents.Add();
  first_dummy_entry->set_index(ents_[start].index());
  first_dummy_entry->set_term(ents_[start].term());
  new_ents.Add(std::make_move_iterator(ents_.begin() + start + 1), std::make_move_iterator(ents_.end()));
  ents_.Swap(&new_ents);
  return {};
}

leaf::result<void> memory_storage::append(pb::repeated_entry&& entries) {
  if (entries.empty()) {
    return {};
  }
  std::lock_guard<std::mutex> guard(mutex_);
  const auto first = _first_index();
  const auto entries_last = entries[0].index() + static_cast<std::uint64_t>(entries.size()) - 1;
  // shortcut if there is no new entry.
  // 如果待追加日志的最大索引小于存储的最小索引，说明这些日志已被压缩丢弃，直接返回
  if (entries_last < first) {
    return {};
  }
  if (first > entries[0].index()) {  // truncate compacted entries
    // 如果存储的最小索引大于待追加日志的起始索引（例如存储最小索引=5，日志起始索引=3），说明日志的前半部分已被压缩。
    // 截断日志切片，只保留索引大于等于 first 的部分。
    entries = pb::extract_range_without_copy(entries, static_cast<int>(first - entries[0].index()), entries.size());
  }
  const auto entries_first_index = entries[0].index();
  const auto ents_first_index = ents_[0].index();
  const auto offset = entries_first_index - ents_first_index;
  const auto ents_size = static_cast<std::uint64_t>(ents_.size());
  if (offset < ents_size) {
    ents_ = pb::extract_range_without_copy(ents_, 0, static_cast<int>(offset));
    ents_.Add(std::make_move_iterator(entries.begin()), std::make_move_iterator(entries.end()));
  } else if (offset == ents_size) {
    ents_.Add(std::make_move_iterator(entries.begin()), std::make_move_iterator(entries.end()));
  } else {
    LEPTON_CRITICAL("entries_first_index: {}, ents_first_index: {}, offset({}) is out of range [len({})]",
                    entries_first_index, ents_first_index, offset, ents_.size());
  }
  return {};
}

}  // namespace lepton::core
