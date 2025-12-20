#pragma once
#ifndef _LEPTON_JOINT_H_
#define _LEPTON_JOINT_H_

#include <proxy.h>

#include <cassert>
#include <cstdint>
#include <set>
#include <unordered_map>
#include <vector>

#include "raft_core/quorum/majority.h"
#include "raft_core/quorum/quorum.h"
namespace lepton::core {
namespace quorum {
// JointConfig 主要用于 Raft
// 协议中处理联合多数配置的场景，其中要求两个配置的多数节点都同意某项操作（如日志提交或配置变更）。它通过封装两个
// MajorityConfig，使得 Raft
// 集群可以在配置变更过程中逐步过渡，确保操作既得到原有节点多数的支持，也得到新配置的多数支持，避免在配置变更过程中出现不一致的情况。
// 通过这些方法，JointConfig 实现了联合多数配置的支持，确保在 Raft
// 集群的配置变化过程中有足够的容错能力和一致性保障。
// *** 主要用作配置变更场景 ***
class joint_config {
  NOT_COPYABLE(joint_config)
 public:
  joint_config() = delete;
  joint_config(majority_config&& primary_config) : primary_config_(std::move(primary_config)) {}
  joint_config(majority_config&& primary_config, majority_config&& secondary_config)
      : primary_config_(std::move(primary_config)), secondary_config_(std::move(secondary_config)) {}
  joint_config(joint_config&&) = default;
  joint_config& operator=(joint_config&&) = default;
  auto operator<=>(const joint_config&) const = default;

  joint_config clone() const {
    if (secondary_config_) {
      return joint_config{primary_config_.clone(), secondary_config_->clone()};
    } else {
      return joint_config{primary_config_.clone()};
    }
  }

  const majority_config& primary_config_view() const { return primary_config_; }

  bool joint() const {
    if (!secondary_config_) {
      return false;
    }
    if (secondary_config_->empty()) {
      return false;
    }
    return true;
  }

  const majority_config& secondary_config_view() const {
    assert(is_secondary_config_valid());
    return secondary_config_.value();
  }

  bool secondary_config_contain(std::uint64_t id) {
    if (!is_secondary_config_valid()) {
      return false;
    }
    return secondary_config_->id_set_.contains(id);
  }

  bool is_singleton() const {
    return (primary_config_.size() == 1 && !secondary_config_.has_value()) ||
           (primary_config_.size() == 1 && secondary_config_.has_value() && secondary_config_->size() == 1);
  }

  void insert_node_into_primary_config(std::uint64_t id) { primary_config_.id_set_.insert(id); }

  void remove_node_from_primary_config(std::uint64_t id) {
    if (primary_config_.id_set_.contains(id)) {
      primary_config_.id_set_.erase(id);
    }
  }

  std::vector<std::uint64_t> primary_config_slice() const { return primary_config_.slice(); }

  void sync_secondary_with_primary() {
    assert(!is_secondary_config_valid());
    secondary_config_ = primary_config_.clone();
  }

  void reset_secondary() { secondary_config_.reset(); }

  bool is_secondary_config_valid() const { return secondary_config_.has_value(); }

  std::vector<std::uint64_t> secondary_config_slice() const {
    if (secondary_config_) {
      return secondary_config_->slice();
    }
    return std::vector<std::uint64_t>{};
  }

  // JointConfig is a configuration of two groups of (possibly overlapping)
  // majority configurations. Decisions require the support of both majorities
  std::string string() const {
    fmt::memory_buffer buf;

    // 主配置（总是存在）
    std::string primary_str = primary_config_.string();
    fmt::format_to(std::back_inserter(buf), "{}", primary_str);

    // 次配置（如果存在且非空）
    if (secondary_config_ && !secondary_config_->empty()) {
      std::string secondary_str = secondary_config_->string();
      fmt::format_to(std::back_inserter(buf), "&&{}", secondary_str);
    }

    return fmt::to_string(buf);
  }

  // IDs returns a newly initialized map representing the set of voters present
  // in the joint configuration.
  std::vector<std::uint64_t> ids() const {
    size_t total_size = primary_config_.id_set_.size();
    if (secondary_config_) {
      total_size += secondary_config_->id_set_.size();
    }

    std::vector<uint64_t> ids;
    ids.reserve(total_size);

    std::set_union(primary_config_.id_set_.begin(), primary_config_.id_set_.end(),
                   // 相同begin/end作为空集
                   secondary_config_ ? secondary_config_->id_set_.begin() : primary_config_.id_set_.end(),
                   secondary_config_ ? secondary_config_->id_set_.end() : primary_config_.id_set_.end(),
                   std::back_inserter(ids));

    return ids;
  }

  std::set<std::uint64_t> id_set() const {
    std::set<std::uint64_t> ids;
    for (auto iter : primary_config_.id_set_) {
      ids.insert(iter);
    }
    if (secondary_config_ && !secondary_config_->empty()) {
      for (auto iter : secondary_config_->id_set_) {
        ids.insert(iter);
      }
    }
    return ids;
  }

  // Describe returns a (multi-line) representation of the commit indexes for
  // the given lookuper.
  std::string describe(pro::proxy_view<acked_indexer_builder> indexer) const {
    return majority_config(id_set()).describe(indexer);
  }

  // CommittedIndex returns the largest committed index for the given joint
  // quorum. An index is jointly committed if it is committed in both
  // constituent majorities.
  log_index committed_index(pro::proxy_view<acked_indexer_builder> indexer) const {
    auto log_idx0 = primary_config_.committed_index(indexer);
    if (!secondary_config_) {
      return log_idx0;
    }
    auto log_idx1 = secondary_config_->committed_index(indexer);
    return log_idx0 < log_idx1 ? log_idx0 : log_idx1;
  }

  // VoteResult takes a mapping of voters to yes/no (true/false) votes and
  // returns a result indicating whether the vote is pending, lost, or won. A
  // joint quorum requires both majority quorums to vote in favor.
  vote_result vote_result_statistics(const std::unordered_map<std::uint64_t, bool>& vote) const {
    auto r1 = primary_config_.vote_result_statistics(vote);
    auto r2 = secondary_config_.has_value() ? secondary_config_->vote_result_statistics(vote) : vote_result::VOTE_WON;

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
  majority_config primary_config_;
  std::optional<majority_config> secondary_config_;
};
}  // namespace quorum
}  // namespace lepton::core

#endif  // _LEPTON_JOINT_H_