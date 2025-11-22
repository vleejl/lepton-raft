#ifndef _LEPTON_CONFCHANGE_H_
#define _LEPTON_CONFCHANGE_H_
#include <absl/types/span.h>
#include <raft.pb.h>

#include <tuple>

#include "lepton_error.h"
#include "progress.h"
#include "tracker.h"
#include "utility_macros.h"
namespace lepton {
namespace confchange {
// Changer facilitates configuration changes. It exposes methods to handle
// simple and joint consensus while performing the proper validation that allows
// refusing invalid configuration changes before they affect the active
// configuration.
// Changer 结构体的作用是管理和处理 Raft 配置变更。它通过暴露方法来处理
// 单一和联合共识，并执行适当的验证，以便在更改配置之前拒绝无效的配置更改。

// 具体作用:
// 1. 单一共识与联合共识：Raft
// 协议的配置变更过程涉及单一共识（例如，向集群添加一个新的节点）和联合共识（涉及多个节点的配置变更，例如，扩展或减少集群成员）。Changer
// 负责协调这两种类型的共识，并确保它们顺利完成。
// 2. 拒绝无效的配置变更：Changer
// 执行配置变更的有效性检查，确保它们不会违反 Raft
// 协议的原则。只有在集群能够承受的情况下，配置变更才会被提交，否则变更会被拒绝。
class changer {
 public:
  using result = leaf::result<std::tuple<tracker::config, tracker::progress_map>>;
  NOT_COPYABLE_NOT_MOVABLE(changer)
  changer() = delete;

  void increase_last_index() { last_index_++; }

 private:
  // checkAndCopy copies the tracker's config and progress map (deeply enough
  // for the purposes of the Changer) and returns those copies.
  // It returns an error if checkInvariants does.
  result check_and_copy() const;

  // initProgress initializes a new progress for the given node or learner.
  void init_progress(std::uint64_t id, bool is_learner, tracker::config &cfg, tracker::progress_map &prs) const;

  // makeVoter adds or promotes the given ID to be a voter in the incoming
  // majority config.
  void make_voters(std::uint64_t id, tracker::config &cfg, tracker::progress_map &prs) const;

  // makeLearner makes the given ID a learner or stages it to be a learner once
  // an active joint configuration is exited.
  //
  // The former happens when the peer is not a part of the outgoing config, in
  // which case we either add a new learner or demote a voter in the incoming
  // config.
  //
  // The latter case occurs when the configuration is joint and the peer is a
  // voter in the outgoing config. In that case, we do not want to add the peer
  // as a learner because then we'd have to track a peer as a voter and learner
  // simultaneously. Instead, we add the learner to LearnersNext, so that it
  // will be added to Learners the moment the outgoing config is removed by
  // LeaveJoint().
  void make_learners(std::uint64_t id, tracker::config &cfg, tracker::progress_map &prs) const;

  // remove this peer as a voter or learner from the incoming config.
  void remove(std::uint64_t id, tracker::config &cfg, tracker::progress_map &prs) const;
  // apply a change to the configuration. By convention, changes to voters are
  // always made to the incoming majority config Voters[0]. Voters[1] is either
  // empty or preserves the outgoing majority configuration while in a joint
  // state.
  leaf::result<void> apply(absl::Span<const raftpb::conf_change_single *const> ccs, tracker::config &cfg,
                           tracker::progress_map &prs) const;

 public:
  changer(tracker::progress_tracker &&tracker, std::uint64_t last_index)
      : tracker_(std::move(tracker)), last_index_(last_index) {}

  void update_tracker_config(tracker::config &&cfg) { tracker_.update_config(std::move(cfg)); }

  tracker::config &&move_config() { return tracker_.move_config(); }

  const tracker::config &config_view() { return tracker_.config_view(); }

  void update_tracker_progress(tracker::progress_map &&map) { tracker_.update_progress(std::move(map)); }

  tracker::progress_map &&move_progress() { return tracker_.move_progress(); }

  const tracker::progress_map &progress_view() { return tracker_.progress_map_view(); }
  // EnterJoint verifies that the outgoing (=right) majority config of the joint
  // config is empty and initializes it with a copy of the incoming (=left)
  // majority config. That is, it transitions from
  //
  //	(1 2 3)&&()
  //
  // to
  //
  //	(1 2 3)&&(1 2 3).
  //
  // The supplied changes are then applied to the incoming majority config,
  // resulting in a joint configuration that in terms of the Raft thesis[1]
  // (Section 4.3) corresponds to `C_{new,old}`.
  //
  // [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
  result enter_joint(bool auto_leave, absl::Span<const raftpb::conf_change_single *const> ccs) const;

  // LeaveJoint transitions out of a joint configuration. It is an error to call
  // this method if the configuration is not joint, i.e. if the outgoing
  // majority config Voters[1] is empty.
  //
  // The outgoing majority config of the joint configuration will be removed,
  // that is, the incoming config is promoted as the sole decision maker. In the
  // notation of the Raft thesis[1] (Section 4.3), this method transitions from
  // `C_{new,old}` into `C_new`.
  //
  // At the same time, any staged learners (LearnersNext) the addition of which
  // was held back by an overlapping voter in the former outgoing config will be
  // inserted into Learners.
  //
  // [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
  result leave_joint() const;

  // Simple carries out a series of configuration changes that (in aggregate)
  // mutates the incoming majority config Voters[0] by at most one. This method
  // will return an error if that is not the case, if the resulting quorum is
  // zero, or if the configuration is in a joint state (i.e. if there is an
  // outgoing configuration).
  result simple(absl::Span<const raftpb::conf_change_single *const> ccs) const;

 private:
  tracker::progress_tracker tracker_;
  std::uint64_t last_index_;
};
}  // namespace confchange
}  // namespace lepton

#endif  // _LEPTON_CONFCHANGE_H_
