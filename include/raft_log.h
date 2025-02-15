#ifndef _LEPTON_RAFT_LOG_H_
#define _LEPTON_RAFT_LOG_H_
#include <absl/types/span.h>
#include <proxy.h>
#include <raft.pb.h>

#include <system_error>

#include "config.h"
#include "error.h"
#include "raft_log_unstable.h"
#include "raft_pb.h"
#include "storage.h"
#include "utility_macros.h"

namespace lepton {
class raft_log {
  NOT_COPYABLE(raft_log)
 public:
  raft_log(raft_log&& rhs) = default;
  raft_log(pro::proxy_view<storage_builer> storage, std::uint64_t offset,
           std::uint64_t committed, std::uint64_t applied,
           std::uint64_t max_next_ents_size);

  std::string string();

  std::uint64_t first_index();

  std::uint64_t last_index();

  void commit_to(std::uint64_t tocommit);

  leaf::result<std::uint64_t> term(std::uint64_t i);

  bool match_term(std::uint64_t i, std::uint64_t term);

  std::uint64_t zero_term_on_err_compacted(std::uint64_t i);

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
  std::uint64_t find_conflict(absl::Span<const raftpb::entry* const> entries);

  std::uint64_t append(pb::repeated_entry&& entries);

  // maybeAppend returns (0, false) if the entries cannot be appended.
  // Otherwise, it returns (last index of new entries, true).
  leaf::result<std::uint64_t> maybe_append(std::uint64_t index,
                                           std::uint64_t log_term,
                                           std::uint64_t committed,
                                           pb::repeated_entry&& enrties);

 private:
  // storage contains all stable entries since the last snapshot.
  pro::proxy_view<storage_builer> storage_;

  // unstable contains all unstable entries and snapshot.
  // they will be saved into storage.
  unstable unstable_;

  // committed is the highest log position that is known to be in
  // stable storage on a quorum of nodes.
  // committed 表示已知在一组节点中，最远已提交的日志条目的索引。committed
  // 是指当前集群中已经被大多数节点确认并且处于稳定状态的日志索引。
  // 用途：这是 Raft 日志的一个关键指标，指示了日志的提交进度。在 Raft
  // 中，日志必须得到大多数节点的确认才能提交（即成为 commited 日志）。
  std::uint64_t committed_;

  // applied is the highest log position that the application has
  // been instructed to apply to its state machine.
  // Invariant: applied <= committed
  // 是指已经被应用到状态机的日志条目的最高索引。该字段表示状态机已经处理并执行的日志条目。应用到状态机后，日志条目会被执行并影响系统的状态。
  // 用途：它和 committed 紧密相关，并且是 Raft 协议中的一个重要概念。applied
  // 永远小于等于 committed，确保只有被提交的日志才能被应用。
  std::uint64_t applied_;

  // maxNextEntsSize is the maximum number aggregate byte size of the messages
  // returned from calls to nextEnts.
  // maxNextEntsSize 是每次从 raftLog
  // 获取日志条目时，返回的日志条目的总字节数的最大值。这是为了避免一次性返回太多日志，导致内存使用过高或性能问题。
  // 用途：限制每次从日志中取出的条目大小。这有助于控制系统的内存和带宽，防止一次请求中返回过多日志条目。
  std::uint64_t max_next_ents_size_;
};

leaf::result<raft_log> new_raft_log_with_size(
    pro::proxy_view<storage_builer> storage, std::uint64_t max_next_ents_size);

inline leaf::result<raft_log> new_raft_log(
    pro::proxy_view<storage_builer> storage) {
  return new_raft_log_with_size(storage, NO_LIMIT);
}

}  // namespace lepton

#endif  // _LEPTON_RAFT_LOG_H_
