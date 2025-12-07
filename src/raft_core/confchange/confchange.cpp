#include "confchange.h"

#include <fmt/core.h>

#include <cstddef>
#include <magic_enum/magic_enum.hpp>
#include <optional>
#include <set>

#include "fmt/format.h"
#include "inflights.h"
#include "leaf.h"
#include "lepton_error.h"
#include "majority.h"
#include "progress.h"
#include "tracker.h"

namespace lepton::core {
namespace confchange {

leaf::result<void> foreach_id_set(const std::set<std::uint64_t> &set, const tracker::progress_map::type &prs) {
  for (const auto &id : set) {
    if (!prs.contains(id)) {
      return new_error(logic_error::KEY_NOT_FOUND, fmt::format("no progress for {}", id));
    }
  }
  return {};
};

leaf::result<void> foreach_id_set(const std::optional<std::set<std::uint64_t>> &set,
                                  const tracker::progress_map::type &prs) {
  if (set) {
    return foreach_id_set(set.value(), prs);
  }
  return {};
};

// checkInvariants makes sure that the config and progress are compatible with
// each other. This is used to check both what the Changer is initialized with,
// as well as what it returns.
// 验证 Raft 配置和进度状态的一致性。
// 它确保了配置（cfg）和进度（prs）之间的相互兼容性，即它检查配置中定义的选民、学习者以及配置中的其他信息是否与当前的进度状态相匹配。
leaf::result<void> check_invariants(const tracker::config &cfg, const tracker::progress_map &prs) {
  // NB: intentionally allow the empty config. In production we'll never see a
  // non-empty config (we prevent it from being created) but we will need to
  // be able to *create* an initial config, for example during bootstrap (or
  // during tests). Instead of having to hand-code this, we allow
  // transitioning from an empty config into any other legal and non-empty
  // config.
  auto vote_ids = cfg.voters.id_set();
  auto &prs_view = prs.view();
  if (auto ret = foreach_id_set(vote_ids, prs_view); !ret) {
    return ret;
  }
  if (auto ret = foreach_id_set(cfg.learners, prs_view); !ret) {
    return ret;
  }
  if (auto ret = foreach_id_set(cfg.learners_next, prs_view); !ret) {
    return ret;
  }

  quorum::majority_config empty_config;
  const quorum::majority_config &empty_config_view = empty_config;
  // Any staged learner was staged because it could not be directly added due
  // to a conflicting voter in the outgoing config.
  if (!cfg.learners_next.empty()) {
    const quorum::majority_config &secondary_config =
        cfg.voters.is_secondary_config_valid() ? cfg.voters.secondary_config_view() : empty_config_view;
    for (const auto &id : cfg.learners_next) {
      if (!secondary_config.view().contains(id)) {
        return new_error(raft_error::CONFIG_INVALID, fmt::format("{} is in LearnersNext, but not Voters[1]", id));
      }
      if (prs_view.contains(id) && prs_view.at(id).is_learner()) {
        return new_error(raft_error::CONFIG_INVALID,
                         fmt::format("{} is in LearnersNext, but is already marked as learner", id));
      }
    }
  }

  // Conversely Learners and Voters doesn't intersect at all.
  if (!cfg.learners.empty()) {
    const quorum::majority_config &primary_config = cfg.voters.primary_config_view();
    const quorum::majority_config &secondary_config =
        cfg.voters.is_secondary_config_valid() ? cfg.voters.secondary_config_view() : empty_config_view;
    for (const auto &id : cfg.learners) {
      if (primary_config.view().contains(id)) {
        return new_error(raft_error::CONFIG_INVALID, fmt::format("{} is in Learners and Voters[1]", id));
      }
      if (secondary_config.view().contains(id)) {
        return new_error(raft_error::CONFIG_INVALID, fmt::format("{} is in Learners and Voters[1]", id));
      }
      if (prs_view.contains(id) && !prs_view.at(id).is_learner()) {
        return new_error(raft_error::CONFIG_INVALID,
                         fmt::format("{} is in Learners, but is not marked as learner", id));
      }
    }
  }

  if (!cfg.joint()) {
    // We enforce that empty maps are nil instead of zero.
    if (cfg.voters.is_secondary_config_valid()) {
      return new_error(raft_error::CONFIG_INVALID, "cfg.Voters[1] must be nil when not joint");
    }
    if (!cfg.learners_next.empty()) {
      return new_error(raft_error::CONFIG_INVALID, "cfg.LearnersNext must be nil when not joint");
    }
    if (cfg.auto_leave) {
      return new_error(raft_error::CONFIG_INVALID, "AutoLeave must be false when not joint");
    }
  }
  return {};
}

// checkAndReturn calls checkInvariants on the input and returns either the
// resulting error or the input.
changer::result check_and_return(tracker::config &&cfg, tracker::progress_map &&prs) {
  LEPTON_LEAF_CHECK(check_invariants(cfg, prs));
  return {std::move(cfg), std::move(prs)};
}

changer::result changer::check_and_copy() const {
  return check_and_return(tracker_.config_view().clone(), tracker_.progress_map_view().clone());
}

void changer::init_progress(std::uint64_t id, bool is_learner, tracker::config &cfg, tracker::progress_map &prs) const {
  if (!is_learner) {
    cfg.voters.insert_node_into_primary_config(id);
  } else {
    cfg.add_leaner_node(id);
  }

  prs.add_progress(
      id, tracker::progress{// Initializing the Progress with the last index means that the
                            // follower
                            // can be probed (with the last index).
                            //
                            // TODO(tbg): seems awfully optimistic. Using the first index would be
                            // better. The general expectation here is that the follower has no
                            // log
                            // at all (and will thus likely need a snapshot), though the app may
                            // have applied a snapshot out of band before adding the replica (thus
                            // making the first index the better choice).
                            std::max(last_index_, static_cast<std::uint64_t>(1)),  // invariant: Match < Next
                            tracker::inflights{tracker_.max_inflight(), tracker_.max_inflight_bytes()}, is_learner,
                            // When a node is first added, we should mark it as recently active.
                            // Otherwise, CheckQuorum may cause us to step down if it is invoked
                            // before the added node has had a chance to communicate with us.
                            true});
}

void changer::make_voters(std::uint64_t id, tracker::config &cfg, tracker::progress_map &prs) const {
  if (!prs.view().contains(id)) {
    init_progress(id, false, cfg, prs);
    return;
  }
  prs.refresh_learner(id, false);
  cfg.delete_learner(id);
  cfg.delete_learner_next(id);
  cfg.voters.insert_node_into_primary_config(id);
}

void remove_node_from_tracker_config(std::uint64_t id, tracker::config &cfg) {
  cfg.voters.remove_node_from_primary_config(id);
  cfg.delete_learner(id);
  cfg.delete_learner_next(id);
}

void remove_node_from_tracker_progress(std::uint64_t id, tracker::progress_map &prs) { prs.delete_progress(id); }

void changer::make_learners(std::uint64_t id, tracker::config &cfg, tracker::progress_map &prs) const {
  if (!prs.view().contains(id)) {
    init_progress(id, true, cfg, prs);
    prs.refresh_learner(id, true);
    return;
  }
  if (prs.view().at(id).is_learner()) {
    return;
  }

  // Remove any existing voter in the incoming config...(primary config)
  remove_node_from_tracker_config(id, cfg);
  // progress still in prs...
  // Use LearnersNext if we can't add the learner to Learners directly, i.e.
  // if the peer is still tracked as a voter in the outgoing config. It will
  // be turned into a learner in LeaveJoint().
  //
  // Otherwise, add a regular learner right away.
  if (cfg.voters.secondary_config_contain(id)) {
    cfg.add_leaner_next_node(id);
  } else {
    prs.refresh_learner(id, true);
    cfg.add_leaner_node(id);
  }
}

void changer::remove(std::uint64_t id, tracker::config &cfg, tracker::progress_map &prs) const {
  if (!prs.view().contains(id)) {
    return;
  }
  remove_node_from_tracker_config(id, cfg);

  // If the peer is still a voter in the outgoing config, keep the Progress.
  if (!cfg.voters.secondary_config_contain(id)) {
    remove_node_from_tracker_progress(id, prs);
  }
}

leaf::result<void> changer::apply(absl::Span<const raftpb::conf_change_single *const> ccs, tracker::config &cfg,
                                  tracker::progress_map &prs) const {
  for (const auto &cc : ccs) {
    if (!cc->has_node_id()) {
      continue;
    }
    if (cc->node_id() == 0) {
      // etcd replaces the NodeID with zero if it decides (downstream of
      // raft) to not apply a change, so we have to have explicit code
      // here to ignore these.
      continue;
    }
    switch (cc->type()) {
      case raftpb::conf_change_type::CONF_CHANGE_ADD_NODE: {
        make_voters(cc->node_id(), cfg, prs);
        break;
      }
      case raftpb::conf_change_type::CONF_CHANGE_ADD_LEARNER_NODE: {
        make_learners(cc->node_id(), cfg, prs);
        break;
      }
      case raftpb::conf_change_type::CONF_CHANGE_REMOVE_NODE: {
        remove(cc->node_id(), cfg, prs);
        break;
      }
      case raftpb::conf_change_type::CONF_CHANGE_UPDATE_NODE: {
        break;
      }
      default:
        return new_error(raft_error::CONFIG_INVALID, fmt::format("unexpected conf type {}", enum_name(cc->type())));
    }
  }
  if (cfg.voters.primary_config_view().empty()) {
    return new_error(raft_error::CONFIG_INVALID, "removed all voters");
  }
  return {};
}

changer::result changer::enter_joint(bool auto_leave, absl::Span<const raftpb::conf_change_single *const> ccs) const {
  BOOST_LEAF_AUTO(v, check_and_copy());
  auto &[cfg, prs] = v;
  if (cfg.joint()) {
    return new_error(raft_error::CONFIG_INVALID, "config is already joint");
  }
  if (cfg.voters.primary_config_view().empty()) {
    // We allow adding nodes to an empty config for convenience (testing and
    // bootstrap), but you can't enter a joint state.
    return new_error(raft_error::CONFIG_INVALID, "can't make a zero-voter config joint");
  }
  // Clear the outgoing config.
  // actually no need clean outgoing config because of check_and_copy before
  if (cfg.voters.is_secondary_config_valid()) {
    return new_error(raft_error::CONFIG_INVALID, "cfg.Voters[1] must be nil when not joint");
  }
  // Copy incoming to outgoing.
  cfg.voters.sync_secondary_with_primary();
  LEPTON_LEAF_CHECK(apply(ccs, cfg, prs));

  cfg.auto_leave = auto_leave;
  return check_and_return(std::move(cfg), std::move(prs));
}

changer::result changer::leave_joint() const {
  BOOST_LEAF_AUTO(v, check_and_copy());
  auto &[cfg, prs] = v;
  if (!cfg.joint()) {
    return new_error(raft_error::CONFIG_INVALID, "can't leave a non-joint config");
  }
  if (!cfg.voters.is_secondary_config_valid()) {
    return new_error(raft_error::CONFIG_INVALID, "configuration is not joint");
  }
  for (const auto &id : cfg.learners_next) {
    cfg.add_leaner_node(id);
    prs.refresh_learner(id, true);
  }
  cfg.learners_next.clear();

  for (const auto &id : cfg.voters.secondary_config_view().view()) {
    auto is_voter = cfg.voters.primary_config_view().view().contains(id);
    auto is_learner = cfg.learners.contains(id);
    if (!is_voter && !is_learner) {
      prs.delete_progress(id);
    }
  }
  cfg.voters.reset_secondary();
  cfg.auto_leave = false;
  return check_and_return(std::move(cfg), std::move(prs));
}

auto symdiff(const std::set<std::uint64_t> &set1, const std::set<std::uint64_t> &set2) {
  size_t intersection_count = 0;
  for (const auto &id : set1) {
    if (!set2.contains(id)) {
      ++intersection_count;
    }
  }
  for (const auto &id : set2) {
    if (!set1.contains(id)) {
      ++intersection_count;
    }
  }
  return intersection_count;
}

changer::result changer::simple(absl::Span<const raftpb::conf_change_single *const> ccs) const {
  BOOST_LEAF_AUTO(v, check_and_copy());
  auto &[cfg, prs] = v;
  if (cfg.joint()) {
    return new_error(raft_error::CONFIG_INVALID, "can't apply simple config change in joint config");
  }
  LEPTON_LEAF_CHECK(apply(ccs, cfg, prs));
  // 这里约束了投票数的前后变化小于等于1，也就是能保证多数派的 voter 没有变化
  if (auto n =
          symdiff(tracker_.config_view().voters.primary_config_view().view(), cfg.voters.primary_config_view().view());
      n > 1) {
    return new_error(raft_error::CONFIG_INVALID, "more than one voter changed without entering joint config");
  }
  return check_and_return(std::move(cfg), std::move(prs));
}

}  // namespace confchange
}  // namespace lepton::core
