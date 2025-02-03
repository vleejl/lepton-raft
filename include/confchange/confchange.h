#ifndef _LEPTON_CONFCHANGE_H_
#define _LEPTON_CONFCHANGE_H_
#include <tuple>

#include "error.h"
#include "leaf.hpp"
#include "progress.h"
#include "raft.pb.h"
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
  NOT_COPYABLE(changer)
  // checkAndCopy copies the tracker's config and progress map (deeply enough
  // for the purposes of the Changer) and returns those copies.
  // It returns an error if checkInvariants does.
  leaf::result<std::tuple<tracker::config, tracker::progress_map>>
  check_and_copy();
  leaf::result<void> apply();

 public:
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
  leaf::result<std::tuple<tracker::config, tracker::progress_map>> enter_joint(
      bool auto_leave,
      std::initializer_list<raftpb::conf_change_single> values) {}

 private:
  tracker::progress_tracker tracker_;
  std::uint64_t last_index_;
};
}  // namespace confchange
}  // namespace lepton

#endif  // _LEPTON_CONFCHANGE_H_
