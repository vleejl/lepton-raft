#ifndef _LEPTON_TRACKER_H_
#define _LEPTON_TRACKER_H_
#include <proxy.h>
#include <raft.pb.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <set>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "joint.h"
#include "majority.h"
#include "progress.h"
#include "quorum.h"
#include "utility_macros.h"
namespace lepton {
namespace tracker {
// Config reflects the configuration tracked in a ProgressTracker.
struct config {
 private:
  config(quorum::joint_config&& voters, bool auto_leave, std::optional<std::set<std::uint64_t>>&& learners,
         std::optional<std::set<std::uint64_t>>&& learners_next)
      : voters(std::move(voters)),
        auto_leave(auto_leave),
        learners(std::move(learners)),
        learners_next(std::move(learners_next)) {}
  void remove_id_set(std::uint64_t id, std::optional<std::set<std::uint64_t>>& id_set) {
    if (id_set && id_set->contains(id)) {
      // 如果 id_set 有值，并且 id 存在于 id_set 中，删除该元素
      id_set->erase(id);
    }
  }

  void add_id_set(std::uint64_t id, std::optional<std::set<std::uint64_t>>& id_set) {
    if (!id_set) {
      id_set = std::set<std::uint64_t>{id};
    } else {
      id_set->insert(id);
    }
  }

 public:
  NOT_COPYABLE(config)
  config() : voters(quorum::majority_config{}), auto_leave(false) {}
  config(config&&) = default;
  config& operator=(config&&) = default;

  config clone() const {
    return config{voters.clone(), auto_leave, std::optional<std::set<std::uint64_t>>{learners},
                  std::optional<std::set<std::uint64_t>>{learners_next}};
  }

  bool joint() const { return voters.joint(); }

  void add_leaner_node(std::uint64_t id) { add_id_set(id, learners); }

  void delete_learner(std::uint64_t id) { return remove_id_set(id, learners); }

  void add_leaner_next_node(std::uint64_t id) { add_id_set(id, learners_next); }

  void delete_learner_next(std::uint64_t id) { return remove_id_set(id, learners_next); }

  quorum::joint_config voters;

  // AutoLeave is true if the configuration is joint and a transition to the
  // incoming configuration should be carried out automatically by Raft when
  // this is possible. If false, the configuration will be joint until the
  // application initiates the transition manually.
  bool auto_leave;

  // Learners is a set of IDs corresponding to the learners active in the
  // current configuration.
  //
  // Invariant: Learners and Voters does not intersect, i.e. if a peer is in
  // either half of the joint config, it can't be a learner; if it is a
  // learner it can't be in either half of the joint config. This invariant
  // simplifies the implementation since it allows peers to have clarity about
  // its current role without taking into account joint consensus.
  std::optional<std::set<std::uint64_t>> learners;

  // When we turn a voter into a learner during a joint consensus transition,
  // we cannot add the learner directly when entering the joint state. This is
  // because this would violate the invariant that the intersection of
  // voters and learners is empty. For example, assume a Voter is removed and
  // immediately re-added as a learner (or in other words, it is demoted):
  //
  // Initially, the configuration will be
  //
  //   voters:   {1 2 3}
  //   learners: {}
  //
  // and we want to demote 3. Entering the joint configuration, we naively get
  //
  //   voters:   {1 2} & {1 2 3}
  //   learners: {3}
  //
  // but this violates the invariant (3 is both voter and learner). Instead,
  // we get
  //
  //   voters:   {1 2} & {1 2 3}
  //   learners: {}
  //   next_learners: {3}
  //
  // Where 3 is now still purely a voter, but we are remembering the intention
  // to make it a learner upon transitioning into the final configuration:
  //
  //   voters:   {1 2}
  //   learners: {3}
  //   next_learners: {}
  //
  // Note that next_learners is not used while adding a learner that is not
  // also a voter in the joint config. In this case, the learner is added
  // right away when entering the joint configuration, so that it is caught up
  // as soon as possible.
  std::optional<std::set<std::uint64_t>> learners_next;

  std::string string() const {
    std::ostringstream buf;
    buf << "voters=" << voters.string();

    if (learners && !learners->empty()) {
      buf << " learners=" << quorum::majority_config{learners.value()}.string();
    }
    if (learners_next && !learners_next->empty()) {
      buf << " learners_next=" << quorum::majority_config{learners_next.value()}.string();
    }
    if (auto_leave) {
      buf << " autoleave";
    }

    return buf.str();
  }
};

// ProgressTracker tracks the currently active configuration and the information
// known about the nodes and learners in it. In particular, it tracks the match
// index for each peer which in turn allows reasoning about the committed index.
class progress_tracker {
  progress_tracker(config&& cfg, progress_map&& pgs_map, std::unordered_map<std::uint64_t, bool> votes,
                   std::size_t max_inflight, std::uint64_t max_inflight_bytes)
      : config_(std::move(cfg)),
        progress_map_(std::move(pgs_map)),
        votes_(std::move(votes)),
        max_inflight_(max_inflight),
        max_inflight_bytes_(max_inflight_bytes) {}

 public:
  progress_tracker(std::size_t max_inflight, std::uint64_t max_inflight_bytes)
      : config_(), max_inflight_(max_inflight), max_inflight_bytes_(max_inflight_bytes) {}
  MOVABLE_BUT_NOT_COPYABLE(progress_tracker)

  progress_tracker clone() {
    return progress_tracker{config_.clone(), progress_map_.clone(), votes_, max_inflight_, max_inflight_bytes_};
  }

  const config& config_view() const { return config_; }

#ifdef LEPTON_TEST
  config& mutable_config_view() { return config_; }

  auto votes_size() const { return votes_.size(); }
#endif

  const progress_map& progress_map_view() const { return progress_map_; }

  progress_map& progress_map_mutable_view() { return progress_map_; }

  auto max_inflight() const { return max_inflight_; }

  auto max_inflight_bytes() const { return max_inflight_bytes_; }

  void update_config(config&& cfg) { config_ = std::move(cfg); };

  config&& move_config() { return std::move(config_); }

  void update_progress(progress_map&& map) { progress_map_ = std::move(map); };

  progress_map&& move_progress() { return std::move(progress_map_); }

  // ConfState returns a ConfState representing the active configuration.
  raftpb::conf_state conf_state() {
    auto conf_state = raftpb::conf_state{};

    auto primary_config_slice = config_.voters.primary_config_slice();
    conf_state.mutable_voters()->Add(primary_config_slice.begin(), primary_config_slice.end());

    if (config_.voters.is_secondary_config_valid()) {
      auto secondary_config_slice = config_.voters.secondary_config_slice();

      conf_state.mutable_voters_outgoing()->Add(secondary_config_slice.begin(), secondary_config_slice.end());
    }

    if (config_.learners) {
      auto learners_slice = quorum::majority_config{config_.learners.value()}.slice();
      conf_state.mutable_learners()->Add(learners_slice.begin(), learners_slice.end());
    }
    if (config_.learners_next) {
      auto learners_next_slice = quorum::majority_config{config_.learners_next.value()}.slice();
      conf_state.mutable_learners_next()->Add(learners_next_slice.begin(), learners_next_slice.end());
    }
    return conf_state;
  }

  // IsSingleton returns true if (and only if) there is only one voting member
  // (i.e. the leader) in the current configuration.
  auto is_singleton() const { return config_.voters.is_singleton(); }

  // Committed returns the largest log index known to be committed based on
  // what the voting members of the group have acknowledged.
  std::uint64_t committed() {
    return config_.voters.committed_index(pro::proxy_view<quorum::acked_indexer_builder>{&progress_map_});
  }

  // using Visit invokes the supplied closure for all tracked progresses in stable order.
  // 使用 const 引用而不是指针，是为了进一步降低内存风险
  void visit(std::function<void(std::uint64_t id, progress& p)> f) {
    size_t n = progress_map_.map_.size();

    // 使用vector直接分配内存
    std::vector<uint64_t> ids;
    ids.reserve(n);  // 预先分配n个空间，避免多次分配

    // 将 Progress 中的 ID 取出并填充到 ids 数组
    for (const auto& entry : progress_map_.map_) {
      ids.push_back(entry.first);
    }

    // 排序 ID
    std::sort(ids.begin(), ids.end());

    // 调用提供的函数（闭包）对每个元素进行处理
    for (const auto id : ids) {
      f(id, progress_map_.map_.at(id));
    }
  }

  bool quorum_active() {
    std::unordered_map<std::uint64_t, bool> votes_;
    visit([&votes_](std::uint64_t id, progress& p) {
      if (p.is_learner()) {
        return;
      }
      votes_.insert_or_assign(id, p.recent_active());
    });
    return config_.voters.vote_result_statistics(votes_) == quorum::vote_result::VOTE_WON;
  }

  // VoterNodes returns a sorted slice of voters.
  std::vector<std::uint64_t> voter_nodes() const { return config_.voters.ids(); }

  std::vector<std::uint64_t> learner_nodes() const {
    if (config_.learners) {
      return std::vector<std::uint64_t>{config_.learners->begin(), config_.learners->end()};
    }
    return std::vector<std::uint64_t>{};
  }

  // ResetVotes prepares for a new round of vote counting via recordVote.
  // 记录首次投票，而 忽略重复的投票操作。
  void reset_votes() { votes_.clear(); }

  void record_vote(std::uint64_t id, bool v) { votes_.insert_or_assign(id, v); }

  std::tuple<std::uint64_t, std::uint64_t, quorum::vote_result> tally_votes() {
    std::uint64_t granted = 0;
    std::uint64_t rejected = 0;
    for (const auto& iter : progress_map_.map_) {
      if (iter.second.is_learner()) {
        continue;
      }
      auto it = votes_.find(iter.first);
      if (it == votes_.end()) {
        continue;
      }
      if (it->second) {
        granted++;
      } else {
        rejected++;
      }
    }
    auto result = config_.voters.vote_result_statistics(votes_);
    return {granted, rejected, result};
  }

 private:
  config config_;
  progress_map progress_map_;
  std::unordered_map<std::uint64_t, bool> votes_;
  std::size_t max_inflight_;
  std::uint64_t max_inflight_bytes_;
};
}  // namespace tracker
}  // namespace lepton

#endif  // _LEPTON_TRACKER_H_
