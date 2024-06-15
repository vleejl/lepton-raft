#ifndef _LEPTON_JOINT_H_
#define _LEPTON_JOINT_H_

#include <array>
#include <cstdint>
#include <set>

#include "majority.h"
#include "proxy.h"
#include "quorum.h"
namespace lepton {
namespace quorum {
// JointConfig 主要用于 Raft
// 协议中处理联合多数配置的场景，其中要求两个配置的多数节点都同意某项操作（如日志提交或配置变更）。它通过封装两个
// MajorityConfig，使得 Raft
// 集群可以在配置变更过程中逐步过渡，确保操作既得到原有节点多数的支持，也得到新配置的多数支持，避免在配置变更过程中出现不一致的情况。
// 通过这些方法，JointConfig 实现了联合多数配置的支持，确保在 Raft
// 集群的配置变化过程中有足够的容错能力和一致性保障。
// *** 主要用作配置变更场景 ***
class joint_config {
 public:
  // JointConfig is a configuration of two groups of (possibly overlapping)
  // majority configurations. Decisions require the support of both majorities
  std::string string() const {
    auto str0 = config_[0].string();
    auto str1 = config_[1].string();
    if (!str1.empty()) {
      return str0 + "&&" + str1;
    }
    return str0;
  }

  // IDs returns a newly initialized map representing the set of voters present
  // in the joint configuration.
  std::set<std::uint64_t> ids() const {
    std::set<std::uint64_t> ids;
    for (const auto &iter : config_) {
      for (auto iter : iter.id_set_) {
        ids.insert(iter);
      }
    }
    return ids;
  }

  // Describe returns a (multi-line) representation of the commit indexes for
  // the given lookuper.
  std::string describe(pro::proxy_view<acked_indexer_builer> indexer) const {
    return majority_config(ids()).describe(indexer);
  }

  // CommittedIndex returns the largest committed index for the given joint
  // quorum. An index is jointly committed if it is committed in both
  // constituent majorities.
  log_index committed_index(
      pro::proxy_view<acked_indexer_builer> indexer) const {
    auto log_idx0 = config_[0].committed_index(indexer);
    auto log_idx1 = config_[1].committed_index(indexer);
    return log_idx0 < log_idx1 ? log_idx0 : log_idx1;
  }

  // VoteResult takes a mapping of voters to yes/no (true/false) votes and
  // returns a result indicating whether the vote is pending, lost, or won. A
  // joint quorum requires both majority quorums to vote in favor.
  vote_result vote_result_statistics(
      const std::map<std::uint64_t, bool> &vote) const {
    auto r1 = config_[0].vote_result_statistics(vote);
    auto r2 = config_[1].vote_result_statistics(vote);

    if (r1 == r2) {
      // If they agree, return the agreed state.
      return r1;
    }

    if ((r1 == vote_result::VOTE_LOST) || (r2 == vote_result::VOTE_LOST)) {
      // If either config has lost, loss is the only possible outcome.
      return vote_result::VOTE_LOST;
    }

    // One side won, the other one is pending, so the whole outcome is.
    return vote_result::VOTE_PENDING;
  }

 private:
  std::array<majority_config, 2> config_;
};
}  // namespace quorum
}  // namespace lepton

#endif  // _LEPTON_JOINT_H_