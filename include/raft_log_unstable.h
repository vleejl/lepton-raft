#ifndef _LEPTON_RAFT_LOG_UNSTABLE_H_
#define _LEPTON_RAFT_LOG_UNSTABLE_H_
#include <memory>
#include <vector>

#include "raft.pb.h"
#include "utility_macros.h"
namespace lepton {
// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
// 用于存储那些还没有持久化（写入磁盘存储）的 Raft 日志条目。
class unstable {
  NOT_COPYABLE(unstable)
 public:
  unstable(std::uint64_t offset);
  unstable(unstable&&) = default;

 private:
  // the incoming unstable snapshot, if any.
  std::unique_ptr<raftpb::snapshot> snapshot_;
  // all entries that have not yet been written to storage.
  std::vector<raftpb::message> entries_;
  // 这个字段记录了未稳定日志条目在 Raft
  // 日志中的位置偏移。也就是说，unstable.offset
  // 是这些日志条目相对于持久化存储中日志的起始位置的偏移量。这个字段的存在是为了确保
  // Raft 节点在日志持久化过程中能够准确地定位到这些日志条目的位置。offset
  // 使得在写入存储时，Raft
  // 能够知道从哪个位置开始写入，避免了重复写入或覆盖已存在的日志。
  std::uint64_t offset_;
};
}  // namespace lepton

#endif  // _LEPTON_RAFT_LOG_UNSTABLE_H_
