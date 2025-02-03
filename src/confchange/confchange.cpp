#include "confchange.h"

#include <fmt/core.h>

#include <optional>
#include <set>

#include "error.h"
#include "leaf.hpp"
#include "majority.h"
#include "progress.h"
#include "tracker.h"

namespace lepton {
namespace confchange {

leaf::result<void> foreach_id_set(const std::set<std::uint64_t> &set,
                                  const tracker::progress_map::type &prs) {
  for (const auto &id : set) {
    if (!prs.contains(id)) {
      return leaf::new_error(fmt::format("no progress for {}", id));
    }
  }
  return {};
};

leaf::result<void> foreach_id_set(
    const std::optional<std::set<std::uint64_t>> &set,
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
leaf::result<void> check_invariants(const tracker::config &cfg,
                                    const tracker::progress_map &prs) {
  // NB: intentionally allow the empty config. In production we'll never see a
  // non-empty config (we prevent it from being created) but we will need to
  // be able to *create* an initial config, for example during bootstrap (or
  // during tests). Instead of having to hand-code this, we allow
  // transitioning from an empty config into any other legal and non-empty
  // config.
  auto vote_ids = cfg.voters.id_set();
  auto prs_view = prs.view();
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
  if (cfg.learners_next) {
    const quorum::majority_config &secondary_config =
        cfg.voters.is_secondary_config_valid()
            ? cfg.voters.secondary_config_view()
            : empty_config_view;
    for (const auto &id : cfg.learners_next.value()) {
      if (!secondary_config.view().contains(id)) {
        return leaf::new_error(
            fmt::format("{} is in LearnersNext, but not Voters[1]", id));
      }
      if (prs_view[id].is_learner()) {
        return leaf::new_error(fmt::format(
            "{}  is in LearnersNext, but is already marked as learner", id));
      }
    }
  }

  // Conversely Learners and Voters doesn't intersect at all.
  if (cfg.learners) {
    const quorum::majority_config &primary_config =
        cfg.voters.primary_config_view();
    const quorum::majority_config &secondary_config =
        cfg.voters.is_secondary_config_valid()
            ? cfg.voters.secondary_config_view()
            : empty_config_view;
    for (const auto &id : cfg.learners_next.value()) {
      if (primary_config.view().contains(id)) {
        return leaf::new_error(
            fmt::format("{} is in Learners and Voters[1]", id));
      }
      if (secondary_config.view().contains(id)) {
        return leaf::new_error(
            fmt::format("{} is in Learners and Voters[1]", id));
      }
      if (prs_view[id].is_learner()) {
        return leaf::new_error(fmt::format(
            "{}  is in LearnersNext, but is already marked as learner", id));
      }
    }
  }

  if (!cfg.voters.joint()) {
    // We enforce that empty maps are nil instead of zero.
    if (cfg.voters.is_secondary_config_valid()) {
      return leaf::new_error("cfg.Voters[1] must be nil when not joint");
    }
    if (cfg.learners_next) {
      return leaf::new_error("cfg.LearnersNext must be nil when not joint");
    }
    if (cfg.auto_leave) {
      return leaf::new_error("AutoLeave must be false when not joint");
    }
  }
  return {};
}

leaf::result<std::tuple<tracker::config, tracker::progress_map>>
changer::check_and_copy() {
  auto cfg = tracker_.config_view().clone();
  auto prs = tracker_.progress_map_view().clone();
}

}  // namespace confchange
}  // namespace lepton
