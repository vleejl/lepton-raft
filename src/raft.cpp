#include "raft.h"

#include <cassert>
#include <utility>
#include <vector>

#include "absl/strings/str_join.h"
#include "confchange.h"
#include "error.h"
#include "fmt/format.h"
#include "protobuf.h"
#include "raft.pb.h"
#include "raft_log.h"
#include "restore.h"
#include "spdlog/spdlog.h"
#include "status.h"

namespace lepton {
leaf::result<raft> new_raft(const config& c) {
  BOOST_LEAF_CHECK(c.validate());
  BOOST_LEAF_AUTO(raftlog, new_raft_log_with_size(
                               c.storage, c.max_uncommitted_entries_size));
  BOOST_LEAF_AUTO(inital_states, c.storage->initial_state());
  auto& [hard_state, conf_state] = inital_states;
  raft r{c.id,
         std::move(raftlog),
         c.max_size_per_msg,
         c.max_uncommitted_entries_size,
         c.max_inflight_msgs,
         c.election_tick,
         c.heartbeat_tick,
         c.check_quorum,
         c.pre_vote,
         c.read_only_opt,
         c.disable_proposal_forwarding};
  BOOST_LEAF_AUTO(
      restore_result,
      confchange::restor(
          conf_state,
          confchange::changer{tracker::progress_tracker{c.max_inflight_msgs},
                              r.raft_log_handle_.last_index()}));
  auto [cfg, prs] = std::move(restore_result);
  pb::assert_conf_states_equivalent(
      conf_state, r.switch_to_config(std::move(cfg), std::move(prs)));

  if (pb::is_empty_hard_state(hard_state)) {
    r.load_state(hard_state);
  }
  if (c.applied_index > 0) {
    r.raft_log_handle_.applied_to(c.applied_index);
  }
  r.become_follower(r.term_, NONE);
  std::vector<std::string> node_strs;
  for (const auto& n : r.prs_.vote_nodes()) {
    node_strs.push_back(fmt::format("{}", n));
  }
  SPDLOG_INFO(
      "newRaft {} [peers: [{}], term: {}, commit: {}, applied: {}, lastindex: "
      "{}, lastterm: {}]",
      r.id_, absl::StrJoin(node_strs, ","), r.term_,
      r.raft_log_handle_.committed(), r.raft_log_handle_.applied(),
      r.raft_log_handle_.last_index(), r.raft_log_handle_.last_term());
  return r;
}

leaf::result<void> step_follower(raft& r, raftpb::message&& m) {
  // TODO
}

void raft::load_state(const raftpb::hard_state& state) {
  if (state.commit() < raft_log_handle_.committed() ||
      state.commit() > raft_log_handle_.last_index()) {
    LEPTON_CRITICAL("{} state.commit {} is out of range [{}, {}]", id_,
                    state.commit(), raft_log_handle_.committed(),
                    raft_log_handle_.last_index());
  }
  raft_log_handle_.commit_to(state.commit());
  term_ = state.term();
  vote_id_ = state.vote();
}

bool raft::past_election_timeout() {
  return election_elapsed_ >= election_timeout_;
}

void raft::send(raftpb::message&& message) {
  // TODO
}

bool raft::maybe_send_append(std::uint64_t id, bool send_if_empty) {
  auto pr_iter = prs_.progress_map_mutable_view().mutable_view().find(id);
  assert(pr_iter != prs_.progress_map_mutable_view().mutable_view().end());
  auto& pr = pr_iter->second;
  if (pr.is_paused()) {
    return false;
  }

  raftpb::message msg;
  msg.set_to(id);
  // TODO
}

void raft::send_append(std::uint64_t id) { maybe_send_append(id, true); }

void raft::bcast_append() {
  auto& progress_map = prs_.progress_map_view().view();
  for (const auto& [id, pr] : progress_map) {
    if (id == id_) {
      continue;
    }
    send_append(id);
  }
}

bool raft::maybe_commit() {
  auto mci = prs_.committed();
  return raft_log_handle_.maybe_commit(mci, term_);
}

void raft::tick_election() {
  election_elapsed_++;
  if (promotable() && past_election_timeout()) {
    election_elapsed_ = 0;
    raftpb::message m;
    m.set_from(id_);
    m.set_type(raftpb::message_type::MSG_HUP);
    step(std::move(m));
  }
}

void raft::reset(std::uint64_t term) {
  // TODO
}

void raft::abort_leader_transfer() { leader_transferee_ = NONE; }

raftpb::conf_state raft::switch_to_config(tracker::config&& cfg,
                                          tracker::progress_map&& pgs_map) {
  prs_.update_config(std::move(cfg));
  prs_.update_progress(std::move(pgs_map));

  SPDLOG_INFO("{} switched to configuration {}", id_,
              prs_.config_view().string());
  auto cs = prs_.conf_state();
  auto& progress_map = prs_.progress_map_view().view();
  auto iter_pr = progress_map.find(id_);
  auto exist = iter_pr != progress_map.end();

  // Update whether the node itself is a learner, resetting to false when the
  // node is removed.
  if (exist && iter_pr->second.is_learner()) {
    is_learner_ = true;
  }

  // etcd-raft 的设计遵循
  // ​事件驱动模型，状态变更通常由外部消息或定时器触发，而非在配置变更函数中立即执行。例如：
  // 1. ​配置变更后的首次心跳：Leader
  // 发送心跳时会携带新配置，但其他节点可能已不再响应（因 Leader
  // 不在配置中）。
  // 2. ​后续消息处理：在 raft.Step
  // 方法中，若检测到自身角色与配置冲突，会调用

  // raft.becomeFollower 切换状态 当新配置移除了当前 Leader 节点或将其降级为
  // Learner 时，函数通过以下逻辑触发降级：
  if ((!exist || is_learner_) && state_type_ == state_type::STATE_LEADER) {
    // This node is leader and was removed or demoted. We prevent demotions
    // at the time writing but hypothetically we handle them the same way as
    // removing the leader: stepping down into the next Term.
    //
    // TODO(tbg): step down (for sanity) and ask follower with largest Match
    // to TimeoutNow (to avoid interruption). This might still drop some
    // proposals but it's better than nothing.
    //
    // TODO(tbg): test this branch. It is untested at the time of writing.
    return cs;
  }

  // The remaining steps only make sense if this node is the leader and there
  // are other nodes.
  if (state_type_ != state_type::STATE_LEADER || cs.voters_size() == 0) {
    return cs;
  }

  if (maybe_commit()) {
    // If the configuration change means that more entries are committed now,
    // broadcast/append to everyone in the updated config.
    bcast_append();
  } else {
    // Otherwise, still probe the newly added replicas; there's no reason to
    // let them wait out a heartbeat interval (or the next incoming
    // proposal).
    prs_.visit([&](std::uint64_t id, const tracker::progress& p) {
      maybe_send_append(id, false /* sendIfEmpty */);
    });
  }

  // 若正在进行的 Leadership 转移目标（leadTransferee）被移除，则取消转移
  // If the the leadTransferee was removed or demoted, abort the leadership
  // transfer.
  if (auto exist =
          prs_.config_view().voters.id_set().contains(leader_transferee_);
      !exist && leader_transferee_ != 0) {
    abort_leader_transfer();
  }
  return cs;
}

bool raft::promotable() {
  auto pr_iter = prs_.progress_map_view().view().find(id_);
  assert(pr_iter != prs_.progress_map_view().view().end());
  auto& pr = pr_iter->second;
  return !pr.is_learner() && !raft_log_handle_.has_pending_snapshot();
}

void raft::become_follower(std::uint64_t term, std::uint64_t lead) {
  step_func_ = step_follower;
  reset(term);
  tick_func_ = [this]() { tick_election(); };
  lead_ = lead;
  state_type_ = state_type::STATE_FOLLOWER;
}

leaf::result<void> raft::step(raftpb::message&& m) {
  // TODO
}
}  // namespace lepton