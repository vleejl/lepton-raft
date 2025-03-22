#include "raft.h"

#include <cassert>
#include <utility>
#include <vector>

#include "absl/strings/str_join.h"
#include "confchange.h"
#include "config.h"
#include "error.h"
#include "fmt/format.h"
#include "protobuf.h"
#include "raft.pb.h"
#include "raft_log.h"
#include "read_only.h"
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
  for (const auto& n : r.trk_.vote_nodes()) {
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

leaf::result<void> step_candidate(raft& r, raftpb::message&& m) {
  // Only handle vote responses corresponding to our candidacy (while in
  // StateCandidate, we may get stale MsgPreVoteResp messages in this term from
  // our pre-candidate state).
  auto post_vote_resp_func = [&]() {
    const auto [gr, rj, res] = r.poll(r.id_, m.type(), !m.reject());
    switch (res) {
      case quorum::vote_result::VOTE_WON:
        if (r.state_type_ == state_type::STATE_PRE_CANDIDATE) {
          r.campaign(campaign_type::CAMPAIGN_ELECTION);
        } else {
          r.become_leader();
          r.bcast_append();
        }
        break;
      case quorum::vote_result::VOTE_LOST:
        // pb.MsgPreVoteResp contains future term of pre-candidate
        // m.Term > r.Term; reuse r.Term
        r.become_follower(r.term_, NONE);
        break;
      case quorum::vote_result::VOTE_PENDING:
        break;
    }
  };
  switch (m.type()) {
    case raftpb::MSG_PROP: {
      SPDLOG_INFO("{} no leader at term {}; dropping proposal", r.id_, r.term_);
      return new_error(logic_error::PROPOSAL_DROPPED);
    }
    case raftpb::MSG_APP: {
      r.become_follower(m.term(), m.from());  // always m.Term == r.Term
      r.handle_append_entries(std::move(m));
      break;
    }
    case raftpb::MSG_VOTE_RESP: {
      assert(r.state_type_ == state_type::STATE_CANDIDATE);
      post_vote_resp_func();
      break;
    }
    case raftpb::MSG_SNAP: {
      r.become_follower(m.term(), m.from());  // always m.Term == r.Term
      r.handle_snapshot(std::move(m));
      break;
    }
    case raftpb::MSG_HEARTBEAT: {
      r.become_follower(m.term(), m.from());  // always m.Term == r.Term
      r.handle_heartbeat(std::move(m));
      break;
    }
    case raftpb::MSG_PRE_VOTE_RESP: {
      assert(r.state_type_ == state_type::STATE_PRE_CANDIDATE);
      post_vote_resp_func();
      break;
    }
    default:
      break;
      // clang-format off
      // ==============================================================
      // Candidate 未处理消息类型的说明（由其他角色或机制处理）
      // ==============================================================
      //
      // ------------------- 选举与角色转换相关 -------------------
      // MSG_HUP:         触发选举的消息（仅 Follower 超时后转换为 Candidate 时处理）
      //                   Candidate 已处于选举中，重复触发会导致状态冲突
      // MSG_PRE_VOTE:    预投票请求（由 Pre-Candidate 处理，Candidate 已进入正式选举阶段）

      // ------------------- Leader 专属消息 -------------------
      // MSG_BEAT:        Leader 心跳触发信号（仅 Leader 自身定时器处理）
      // MSG_CHECK_QUORUM: Leader 的多数派检查（Candidate 无权限维护 Leader 状态）
      // MSG_HEARTBEAT_RESP: 心跳响应（由 Leader 确认 Follower 存活，Candidate 无需处理）

      // ------------------- 日志复制与网络优化 -------------------
      // MSG_APP_RESP:    日志追加响应（由 Leader 处理，Candidate 不负责日志复制）
      // MSG_UNREACHABLE: 节点不可达标记（由 Leader 优化重试策略，Candidate 不管理网络）
      // MSG_SNAP_STATUS: 快照传输状态（由 Leader 跟踪快照进度，Candidate 不处理快照）

      // ------------------- 领导权转移与只读请求 -------------------
      // MSG_TRANSFER_LEADER: 领导权转移请求（由 Leader 处理，Candidate 无权限转移）
      // MSG_TIMEOUT_NOW: 强制选举命令（由 Leader 发送给 Follower，Candidate 不处理）
      // MSG_READ_INDEX:  只读请求（由 Leader 处理，Candidate 无法保证线性一致性）
      // MSG_READ_INDEX_RESP: 只读响应（由 Leader 返回，Candidate 不处理客户端请求）

      // ------------------- 存储层交互消息 -------------------
      // MSG_STORAGE_APPEND:      存储层日志追加请求（由 Leader/存储线程处理）
      // MSG_STORAGE_APPEND_RESP: 存储层追加响应（由 Leader 确认持久化结果）
      // MSG_STORAGE_APPLY:       存储层状态机应用请求（由提交线程处理）
      // MSG_STORAGE_APPLY_RESP:  存储层应用响应（由 Leader 跟踪状态机进度）
      // MSG_FORGET_LEADER:       忘记 Leader（由 Follower 处理，Candidate 无需清理 Leader 状态）
      //
      // ==============================================================
      // 设计原则：Candidate 仅处理选举投票响应（MSG_VOTE_RESP/MSG_PRE_VOTE_RESP）
      // 和可能使其退位为 Follower 的消息（如更高任期的 MSG_APP/MSG_HEARTBEAT）
      // ==============================================================
      // clang-format on
  }
  return {};
}

leaf::result<void> step_follower(raft& r, raftpb::message&& m) {
  switch (m.type()) {
    case raftpb::MSG_PROP: {  // client request, 客户端提案
      if (r.lead_ == NONE) {  // 若 Follower 无 Leader（如网络分区），直接拒绝。
        SPDLOG_INFO("{} no leader at term {}; dropping proposal", r.id_,
                    r.term_);
        return new_error(logic_error::PROPOSAL_DROPPED);
      } else if (r.disable_proposal_forwarding_) {
        // 若配置禁止转发（disableProposalForwarding），拒绝提案（避免脑裂时多
        // Leader 写入）。
        SPDLOG_INFO(
            "{} not forwarding to leader {} at term {}; dropping "
            "proposal",
            r.id_, r.lead_, r.term_);
        return new_error(logic_error::PROPOSAL_DROPPED);
      }
      m.set_to(r.lead_);
      r.send(std::move(m));
      break;
    }
    case raftpb::MSG_APP: {  // Leader 日志追加
      // 收到 Leader 的日志追加请求后，重置选举计时器（避免发起无用选举）
      r.election_elapsed_ = 0;
      r.lead_ = m.from();
      // 调用 handleAppendEntries
      // 处理日志一致性（检查日志连续性、追加新条目、更新提交索引）。
      r.handle_append_entries(std::move(m));
      break;
    }
    case raftpb::MSG_HEARTBEAT: {
      r.election_elapsed_ = 0;  // 重置选举计时器，确认 Leader 存活。
      r.lead_ = m.from();
      r.handle_heartbeat(std::move(
          m));  // 调用 handleHeartbeat 更新提交索引并可能触发日志提交。
      break;
    }
    case raftpb::MSG_SNAP: {
      // 收到快照消息，重置选举计时器，处理快照消息。
      r.election_elapsed_ = 0;
      r.lead_ = m.from();
      r.handle_snapshot(std::move(m));
      break;
    }
    case raftpb::MSG_TRANSFER_LEADER: {
      if (r.lead_ == NONE) {
        SPDLOG_INFO("{} no leader at term {}; dropping leader transfer msg",
                    r.id_, r.term_);
        return {};
      }
      m.set_to(r.lead_);
      r.send(std::move(m));
      break;
    }
    case raftpb::MSG_TIMEOUT_NOW: {  // 立即超时选举
      // 收到 Leader 的 MsgTimeoutNow 后，立即发起选举（用于 Leader
      // 主动转移权力）。 直接进入选举阶段（跳过预投票），因为此时 Leader
      // 已明确授权。
      SPDLOG_INFO(
          "{} [term {}] received MsgTimeoutNow from {} and starts an election "
          "to get leadership.",
          r.id_, r.term_, m.from());
      // Leadership transfers never use pre-vote even if r.preVote is true; we
      // know we are not recovering from a partition so there is no need for the
      // extra round trip.
      r.hup(campaign_type::CAMPAIGN_TRANSFER);
      break;
    }
    case raftpb::MSG_READ_INDEX: {
      if (r.lead_ == NONE) {
        SPDLOG_INFO("{} no leader at term {}; dropping index reading msg",
                    r.id_, r.term_);
        return {};
      }
      m.set_to(r.lead_);
      r.send(std::move(m));
      break;
    }
    case raftpb::MSG_READ_INDEX_RESP: {
      if (m.entries_size() != 1) {
        SPDLOG_INFO("{} invalid format of MsgReadIndexResp from {}", r.id_,
                    m.from());
        return {};
      }
      r.read_states_.emplace_back(
          read_state{m.entries(0).index(), m.entries(0).data()});
      break;
    }
    case raftpb::MSG_FORGET_LEADER: {  // 强制 Follower 忘记当前
                                       // Leader（用于网络分区恢复后清理旧
                                       // Leader）。
      if (r.read_only_.read_only_opt ==
          read_only_option::READ_ONLY_LEASE_BASED) {
        // 若使用 ReadOnlyLeaseBased（租约机制需持续 Leader 心跳），忽略此消息。
        SPDLOG_INFO("{} [term {}] ignoring MsgForgetLeader", r.id_, r.term_);
        return {};
      }
      if (r.lead_ != NONE) {
        SPDLOG_INFO("%x forgetting leader %x at term %d", r.id_, r.lead_,
                    r.term_);
        return {};
      }
      break;
    }
    default:
      break;
      // clang-format off
      // ==============================================================
      // Follower 未处理消息类型的说明（由其他角色或机制处理）
      // ==============================================================
      //
      // ------------------- 选举与角色转换相关 -------------------
      // MSG_HUP:         触发选举的消息（仅 Follower 超时后转换为 Candidate 时处理）
      // MSG_VOTE:        正式投票请求（由 Candidate 处理，Follower 无投票权）
      // MSG_VOTE_RESP:   投票响应结果（由 Candidate 收集，Follower 不参与）
      // MSG_PRE_VOTE:    预投票请求（由 Candidate 处理，Follower 不参与预投票）
      // MSG_PRE_VOTE_RESP: 预投票响应（由 Candidate 收集，Follower 不参与）

      // ------------------- Leader 专属消息 -------------------
      // MSG_BEAT:        Leader 心跳触发信号（仅 Leader 自身定时器处理）
      // MSG_CHECK_QUORUM: Leader 的多数派检查（仅 Leader 维护自身状态）
      // MSG_HEARTBEAT_RESP: 心跳响应（由 Leader 确认 Follower 存活）

      // ------------------- 日志复制与网络优化 -------------------
      // MSG_APP_RESP:    日志追加响应（由 Leader 更新 nextIndex/matchIndex）
      // MSG_UNREACHABLE: 节点不可达标记（由 Leader 优化重试策略）
      // MSG_SNAP_STATUS: 快照传输状态（由 Leader 跟踪快照进度）

      // ------------------- 存储层交互消息 -------------------
      // MSG_STORAGE_APPEND:      存储层日志追加请求（由 Leader/存储线程处理）
      // MSG_STORAGE_APPEND_RESP: 存储层追加响应（由 Leader 确认持久化结果）
      // MSG_STORAGE_APPLY:       存储层状态机应用请求（由提交线程处理）
      // MSG_STORAGE_APPLY_RESP:  存储层应用响应（由 Leader 跟踪状态机进度）
      //
      // ==============================================================
      // 设计原则：Follower 仅处理 Leader 指令和客户端请求转发
      // 其他消息由协议角色分离机制保证处理正确性
      // =================================================================
      // clang-format on
  }
  return {};
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
  if (message.from() != NONE) {
    message.set_from(id_);
  }

  const auto msg_type = message.type();
  // 处理选举类消息
  if (msg_type == raftpb::message_type::MSG_VOTE ||
      msg_type == raftpb::message_type::MSG_VOTE_RESP ||
      msg_type == raftpb::message_type::MSG_PRE_VOTE ||
      msg_type == raftpb::message_type::MSG_PRE_VOTE_RESP) {
    if (message.term() == 0) {
      // All {pre-,}campaign messages need to have the term set when
      // sending.
      // - MsgVote: m.Term is the term the node is campaigning for,
      //   non-zero as we increment the term when campaigning.
      // - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
      //   granted, non-zero for the same reason MsgVote is
      // - MsgPreVote: m.Term is the term the node will campaign,
      //   non-zero as we use m.Term to indicate the next term we'll be
      //   campaigning for
      // - MsgPreVoteResp: m.Term is the term received in the original
      //   MsgPreVote if the pre-vote was granted, non-zero for the
      //   same reasons MsgPreVote is
      LEPTON_CRITICAL("term should be set when msg type:{}",
                      magic_enum::enum_name(msg_type));
    }
  } else {  // 非选举类消息，必须自动设置term
    if (message.term() != 0) {
      LEPTON_CRITICAL("term should not be set when msg type:{}",
                      magic_enum::enum_name(msg_type));
    }
    // do not attach term to MsgProp, MsgReadIndex
    // proposals are a way to forward to the leader and
    // should be treated as local message.
    // MsgReadIndex is also forwarded to leader.
    // MsgProp 和 MsgReadIndex 不设置 Term，因为它们可能被转发给 Leader，由
    // Leader 处理时再填充正确 Term。
    if (msg_type != raftpb::message_type::MSG_PROP &&
        msg_type != raftpb::message_type::MSG_READ_INDEX) {
      message.set_term(term_);
    }
  }
  msgs_.Add(std::move(message));
}

void raft::hup(campaign_type t) {
  // TODO
}

void raft::handle_append_entries(raftpb::message&& message) {
  // TODO
}

void raft::handle_heartbeat(raftpb::message&& message) {
  // TODO
}

void raft::handle_snapshot(raftpb::message&& message) {
  // TODO
}

bool raft::maybe_send_append(std::uint64_t id, bool send_if_empty) {
  auto pr_iter = trk_.progress_map_mutable_view().mutable_view().find(id);
  assert(pr_iter != trk_.progress_map_mutable_view().mutable_view().end());
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
  auto& progress_map = trk_.progress_map_view().view();
  for (const auto& [id, pr] : progress_map) {
    if (id == id_) {
      continue;
    }
    send_append(id);
  }
}

bool raft::maybe_commit() {
  auto mci = trk_.committed();
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
  trk_.update_config(std::move(cfg));
  trk_.update_progress(std::move(pgs_map));

  SPDLOG_INFO("{} switched to configuration {}", id_,
              trk_.config_view().string());
  auto cs = trk_.conf_state();
  auto& progress_map = trk_.progress_map_view().view();
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
    trk_.visit([&](std::uint64_t id, const tracker::progress& p) {
      maybe_send_append(id, false /* sendIfEmpty */);
    });
  }

  // 若正在进行的 Leadership 转移目标（leadTransferee）被移除，则取消转移
  // If the the leadTransferee was removed or demoted, abort the leadership
  // transfer.
  if (auto exist =
          trk_.config_view().voters.id_set().contains(leader_transferee_);
      !exist && leader_transferee_ != 0) {
    abort_leader_transfer();
  }
  return cs;
}

bool raft::promotable() {
  auto pr_iter = trk_.progress_map_view().view().find(id_);
  assert(pr_iter != trk_.progress_map_view().view().end());
  auto& pr = pr_iter->second;
  return !pr.is_learner() && !raft_log_handle_.has_pending_snapshot();
}

void raft::campaign(campaign_type t) {
  // TODO
}

std::tuple<std::uint64_t, std::uint64_t, quorum::vote_result> raft::poll(
    std::uint64_t id, raftpb::message_type vt, bool vote) {
  if (vote) {
    SPDLOG_INFO("{} received {} from {} at term {}", id_,
                magic_enum::enum_name(vt), id, term_);
  } else {
    SPDLOG_INFO("{} received {} rejiction from {} at term {}", id_,
                magic_enum::enum_name(vt), id, term_);
  }
  trk_.record_vote(id, vote);
  return trk_.tally_votes();
}

void raft::become_leader() {
  // TODO
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