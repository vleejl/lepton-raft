#ifndef _LEPTON_MAJORITY_H_
#define _LEPTON_MAJORITY_H_

#include <fmt/core.h>
#include <proxy.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "quorum.h"
#include "utility_macros.h"
namespace lepton {
namespace quorum {
// MajorityConfig is a set of IDs that uses majority quorums to make decisions.
// majority_config 表示集群的标准配置，它包含一组正常的投票节点（voters）。
// 在 Raft 协议中，只有当大多数节点（quorum）同意时，日志才会被提交，
// 并且领导者（leader）才能进行日志条目的提议和提交。
// 因此，majority_config 通常是当前正常的配置，它指明了集群中的投票节点。
class majority_config {
  friend class joint_config;
  NOT_COPYABLE(majority_config)
 public:
  majority_config() = default;
  explicit majority_config(const std::set<std::uint64_t> &id_set) : id_set_(id_set) {}
  explicit majority_config(std::set<std::uint64_t> &&id_set) : id_set_(std::move(id_set)) {}

  majority_config(majority_config &&) = default;
  majority_config &operator=(majority_config &&) = default;

  majority_config clone() const {
    const std::set<std::uint64_t> &id_set = id_set_;
    return majority_config{id_set};
  }

  auto empty() const { return id_set_.empty(); }

  auto size() const { return id_set_.size(); }

  const auto &view() const { return id_set_; }

  // 功能：将 MajorityConfig 的节点 ID
  // 列表格式化为字符串，并按升序排列。它会将配置中的所有节点 ID
  // 排序，并生成一个形如 (id1 id2 id3 ...) 的字符串输出。
  // 用途：用于打印日志或调试输出，以便更清晰地查看集群的多数配置。
  std::string string() const {
    std::ostringstream ss;
    ss << '(';
    for (auto iter = id_set_.begin(); iter != id_set_.end(); ++iter) {
      if (iter != id_set_.begin()) {
        ss << ' ';
      }
      ss << *iter;
    }
    ss << ')';
    return ss.str();
  }

  // Describe returns a (multi-line) representation of the commit indexes for
  // the given lookuper.
  // 功能：以多行格式描述 MajorityConfig 中每个节点的提交索引（commit
  // index）。会列出每个节点的 ID
  // 和它的提交索引，且会根据提交索引显示一个“进度条”来显示当前节点的提交进度。
  // 用途：提供有关每个节点在 Raft 集群中提交进度的详细信息，帮助调试集群状态。
  std::string describe(pro::proxy_view<acked_indexer_builder> indexer) const {
    if (id_set_.empty()) {
      return "<empty majority quorum>";
    }

    struct tup {
      std::uint64_t id;
      log_index index;
      bool ok;
      int bar;
    };

    auto n = id_set_.size();
    std::vector<tup> info;
    for (auto id : id_set_) {
      if (auto result = indexer->acked_index(id)) {
        info.emplace_back(tup{id, result.value(), true, 0});
      } else {
        info.emplace_back(tup{id, 0, false, 0});
      }
    }

    std::sort(info.begin(), info.end(), [](const tup &lhs, const tup &rhs) {
      if (lhs.index == rhs.index) {
        return lhs.id < rhs.id;
      }
      return lhs.index < rhs.index;
    });

    for (std::size_t i = 0; i < info.size(); i++) {
      if ((i > 0) && (info[i - 1].index < info[i].index)) {
        info[i].bar = i;
      }
    }

    std::sort(info.begin(), info.end(), [](const tup &lhs, const tup &rhs) { return lhs.id < rhs.id; });
    std::ostringstream ss;
    ss << std::string(n, ' ') << "    idx\n";
    for (const auto &iter : info) {
      if (!iter.ok) {
        ss << '?' << std::string(n, ' ');
      } else {
        ss << std::string(iter.bar, 'x') << '>' << std::string(n - iter.bar, ' ');
      }
      ss << fmt::format(" {:5d}    (id={})\n", iter.index, iter.id);
    }
    return ss.str();
  }

  std::vector<std::uint64_t> slice() const { return std::vector<std::uint64_t>{id_set_.begin(), id_set_.end()}; }

  // CommittedIndex computes the committed index from those supplied via the
  // provided AckedIndexer (for the active config).
  // 功能：计算当前 MajorityConfig 的承诺索引（committed
  // index）。即确定哪个日志条目是大多数节点已经确认并同意提交的。
  // 具体过程：
  // 遍历每个节点的提交索引并按升序排列。
  // 选择排序后的第 n/2 + 1 小的节点索引（保证大多数节点的承诺被采纳）。
  // 用途：计算并返回当前多数配置的承诺索引，这是 Raft
  // 协议中用于决定日志是否已经提交的关键值。
  log_index committed_index(pro::proxy_view<acked_indexer_builder> indexer) const {
    if (id_set_.empty()) {
      // This plays well with joint quorums which, when one half is the zero
      // MajorityConfig, should behave like the other half.
      return INVALID_LOG_INDEX;
    }

    // Fill the slice with the indexes observed. Any unused slots will be
    // left as zero; these correspond to voters that may report in, but
    // haven't yet. We fill from the right (since the zeroes will end up on
    // the left after sorting below anyway).
    std::vector<log_index> s(id_set_.size(), 0);
    int i = static_cast<int>(id_set_.size()) - 1;
    for (const auto &id : id_set_) {
      if (auto result = indexer->acked_index(id)) {
        s[static_cast<std::size_t>(i)] = result.value();
        i--;
      }
    }

    std::sort(s.begin(), s.end());

    // The smallest index into the array for which the value is acked by a
    // quorum. In other words, from the end of the slice, move n/2+1 to the
    // left (accounting for zero-indexing).
    auto pos = id_set_.size() - (id_set_.size() / 2 + 1);
    return s[pos];
  }

  // VoteResult takes a mapping of voters to yes/no (true/false) votes and
  // returns a result indicating whether the vote is pending (i.e. neither a
  // quorum of yes/no has been reached), won (a quorum of yes has been reached),
  // or lost (a quorum of no has been reached).
  // 功能：计算 Raft
  // 集群配置的投票结果。根据每个节点的投票（是/否），判断是否已经达成多数同意（即是否选举或提案已经获得多数支持）。
  // 如果未达到多数同意，但还没有足够的否定票，则返回 VotePending。
  // 如果多数节点投票同意，返回 VoteWon。
  // 如果多数节点投票反对，返回 VoteLost。
  // 用途：用于处理集群中的投票决策，例如领导人选举或配置变更请求。
  vote_result vote_result_statistics(const std::unordered_map<std::uint64_t, bool> &vote) const {
    if (id_set_.empty()) {
      // By convention, the elections on an empty config win. This comes in
      // handy with joint quorums because it'll make a half-populated joint
      // quorum behave like a majority quorum.
      return vote_result::VOTE_WON;
    }

    auto pending_counter = 0;
    auto win_counter = 0;
    for (const auto &iter : id_set_) {
      auto val_iter = vote.find(iter);
      if (val_iter == vote.end()) {
        pending_counter++;
        continue;
      }
      if (val_iter->second) {
        win_counter++;
      } else {
        // lost_counter++;
      }
    }

    auto q = static_cast<int>(id_set_.size() / 2 + 1);
    if (win_counter >= q) {
      return vote_result::VOTE_WON;
    }
    if (win_counter + pending_counter >= q) {
      return vote_result::VOTE_PENDING;
    }
    return vote_result::VOTE_LOST;
  }

 private:
  std::set<std::uint64_t> id_set_;
};
}  // namespace quorum
}  // namespace lepton

#endif  // _LEPTON_MAJORITY_H_
