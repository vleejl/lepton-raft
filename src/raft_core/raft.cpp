#include "raft.h"

#include <spdlog/spdlog.h>

#include <cassert>
#include <optional>
#include <random>
#include <utility>
#include <vector>

#include "absl/strings/str_join.h"
#include "absl/types/span.h"
#include "conf_change.h"
#include "confchange.h"
#include "config.h"
#include "enum_name.h"
#include "fmt/format.h"
#include "inflights.h"
#include "leaf.h"
#include "lepton_error.h"
#include "log.h"
#include "logger.h"
#include "progress.h"
#include "protobuf.h"
#include "proxy.h"
#include "raft.pb.h"
#include "raft_log.h"
#include "read_only.h"
#include "restore.h"
#include "state.h"
#include "state_trace.h"
#include "storage.h"
#include "tracker.h"
#include "types.h"

namespace lepton {
leaf::result<raft> new_raft(config&& c) {
  LEPTON_LEAF_CHECK(c.validate());
  pro::proxy_view<storage_builer> storage_view = c.storage;
  BOOST_LEAF_AUTO(raftlog,
                  new_raft_log_with_size(std::move(c.storage), c.logger,
                                         static_cast<pb::entry_encoding_size>(c.max_committed_size_per_ready)));
  BOOST_LEAF_AUTO(inital_states, storage_view->initial_state());
  auto& [hard_state, conf_state] = inital_states;
  raft r{c.id,
         std::move(raftlog),
         static_cast<pb::entry_encoding_size>(c.max_size_per_msg),
         static_cast<pb::entry_payload_size>(c.max_uncommitted_entries_size),
         c.max_inflight_msgs,
         c.max_inflight_bytes,
         c.election_tick,
         c.heartbeat_tick,
         c.check_quorum,
         c.pre_vote,
         c.read_only_opt,
         c.disable_proposal_forwarding,
         c.disable_conf_change_validation,
         c.step_down_on_removal,
         std::move(c.logger)};

  trace_init_state(r);

  auto last_id = r.raft_log_handle_.last_entry_id();
  BOOST_LEAF_AUTO(restore_result, confchange::restor(conf_state, confchange::changer{r.trk_.clone(), last_id.index}));
  auto [cfg, trk] = std::move(restore_result);
  pb::assert_conf_states_equivalent(conf_state, r.switch_to_config(std::move(cfg), std::move(trk)));

  if (!pb::is_empty_hard_state(hard_state)) {
    r.load_state(hard_state);
  }
  if (c.applied_index > 0) {
    r.raft_log_handle_.applied_to(c.applied_index, 0 /* size */);
  }
  r.become_follower(r.term_, NONE);

  std::vector<std::string> node_strs;
  for (const auto& n : r.trk_.voter_nodes()) {
    node_strs.push_back(fmt::format("{}", n));
  }

  // TODO(pav-kv): it should be ok to simply print %+v for lastID.
  LOG_INFO(r.logger_,
           "newRaft {} [peers: [{}], term: {}, commit: {}, applied: {}, lastindex: "
           "{}, lastterm: {}]",
           r.id_, absl::StrJoin(node_strs, ","), r.term_, r.raft_log_handle_.committed(), r.raft_log_handle_.applied(),
           last_id.index, last_id.term);
  return r;
}

void release_pending_read_index_message(raft& r) {
  if (r.pending_read_index_messages_.empty()) {
    // Fast path for the common case to avoid a call to storage.LastIndex()
    // via committedEntryInCurrentTerm.
    return;
  }
  if (!r.committed_entry_in_current_term()) {
    LOG_ERROR(r.logger_, "pending MsgReadIndex should be released only after first commit in current term");
    return;
  }
  for (auto&& m : r.pending_read_index_messages_) {
    // 发送响应消息
    send_msg_read_index_response(r, std::move(m));
  }
  r.pending_read_index_messages_.Clear();
}

void send_msg_read_index_response(raft& r, raftpb::message&& m) {
  // thinking: use an internally defined context instead of the user given context.
  // We can express this in terms of the term and index instead of a user-supplied value.
  // This would allow multiple reads to piggyback on the same message.
  switch (r.read_only_.read_only_opt()) {
    // If more than the local vote is needed, go through a full broadcast.
    case read_only_option::READ_ONLY_SAFE: {
      std::string ctx = m.entries().begin()->data();
      r.read_only_.add_request(r.raft_log_handle_.committed(), std::move(m));
      // The local node automatically acks the request.
      r.read_only_.recv_ack(r.id_, ctx);
      r.bcast_heartbeat_with_ctx(std::move(ctx));
      break;
    }
    case read_only_option::READ_ONLY_LEASE_BASED: {
      auto resp = r.response_to_read_index_req(std::move(m), r.raft_log_handle_.committed());
      if (resp.to() != NONE) {
        // 发送响应消息
        r.send(std::move(resp));
      }
      break;
    }
    default:
      assert(false);
  }
}

leaf::result<void> step_leader(raft& r, raftpb::message&& m) {
  const auto msg_type = m.type();
  // These message types do not require any progress for m.From.
  switch (msg_type) {
    case raftpb::MSG_BEAT: {
      r.bcast_heartbeat();
      return {};
    }
    case raftpb::MSG_CHECK_QUORUM: {
      // MsgCheckQuorum
      // 消息用于领导者主动检查自己是否仍然拥有集群的多数节点（即法定人数）的支持
      if (!r.trk_.quorum_active()) {
        // 如果返回
        // false，说明集群的多数派不再支持当前领导者。可能出现网络分区或者其他问题
        LOG_WARN(r.logger_, "{} stepped down to follower since quorum is not active", r.id_);
        r.become_follower(r.term_, NONE);
      }
      // Mark everyone (but ourselves) as inactive in preparation for the next
      // CheckQuorum.
      // 将除自己（领导者）外的所有节点的RecentActive标记为false，为下一次MsgCheckQuorum的活跃性统计做准备。
      r.trk_.visit([&](std::uint64_t id, tracker::progress& pr) {
        if (id != r.id_) {
          pr.set_recent_active(false);
        }
      });
      return {};
    }
    case raftpb::MSG_PROP: {
      // 是客户端提交的提案消息（如数据写入或配置变更请求）
      // Raft 要求提案必须携带日志条目，否则触发 panic（协议级错误）
      if (m.entries().empty()) {
        LEPTON_CRITICAL("{} stepped empty MsgProp", r.id_);
        assert(false);
      }
      // 若当前节点已被移出集群配置（不在 Progress 中），拒绝提案。
      if (!r.trk_.progress_map_view().view().contains(r.id_)) {
        // If we are not currently a member of the range (i.e. this node
        // was removed from the configuration while serving as leader),
        // drop any new proposals.
        return new_error(raft_error::PROPOSAL_DROPPED);
      }
      // 若正在进行领导权转移（leadTransferee 非空），拒绝新提案，避免状态混乱。
      if (r.leader_transferee_ != NONE) {
        LOG_DEBUG(r.logger_,
                  "{} [term {}] transfer leadership to {} is in progress; dropping "
                  "proposal",
                  r.id_, r.term_, r.leader_transferee_);
        return new_error(raft_error::PROPOSAL_DROPPED);
      }

      // 配置变更（ConfChange）处理
      for (int i = 0; i < m.entries_size(); i++) {
        auto& e = *m.mutable_entries(i);
        std::optional<raftpb::conf_change_v2> cc;
        switch (e.type()) {
          case raftpb::ENTRY_CONF_CHANGE: {
            raftpb::conf_change ccc;
            if (e.has_data()) {
              if (!ccc.ParseFromString(e.data())) {
                LEPTON_CRITICAL("entry hex:{} conf change parse from string failed ", e.data());
                assert(false);
              }
            }
            cc.emplace(pb::conf_change_as_v2(std::move(ccc)));
            break;
          }
          case raftpb::ENTRY_CONF_CHANGE_V2: {
            raftpb::conf_change_v2 ccc;
            if (e.has_data()) {
              if (!ccc.ParseFromString(e.data())) {
                LEPTON_CRITICAL("entry hex:{} conf change v2 parse from string failed ", e.data());
                assert(false);
              }
            }
            cc.emplace(std::move(ccc));
            break;
          }
          default:
            break;
        }
        if (cc) {
          // 存在未提交的配置变更（pendingConfIndex > applied），防止并发变更导致状态冲突。
          auto already_pending = r.pending_conf_index_ > r.raft_log_handle_.applied();
          // 当前是否处于联合共识状态
          auto already_joint = r.trk_.config_view().voters.is_secondary_config_valid();
          // 是否为退出联合共识的请求
          auto wants_leave_joint = cc->changes_size() == 0;

          std::string failed_check;
          if (already_pending) {
            failed_check = fmt::format("possible unapplied conf change at index {} (applied to {})",
                                       r.pending_conf_index_, r.raft_log_handle_.applied());
          } else if (already_joint && !wants_leave_joint) {
            failed_check = "must transition out of joint config first";
          } else if (!already_joint && wants_leave_joint) {
            failed_check = "not in joint state; refusing empty conf change";
          }

          if (!failed_check.empty() && !r.disable_conf_change_validation_) {
            LOG_INFO(r.logger_, "{} ignoring conf change {} at config {}: {}", r.id_, pb::describe_conf_change_v2(*cc),
                     r.trk_.config_view().string(), failed_check);
            e = raftpb::entry{};
            e.set_type(raftpb::ENTRY_NORMAL);
          } else {
            r.pending_conf_index_ = r.raft_log_handle_.last_index() + static_cast<std::uint64_t>(i) + 1;
            trace_change_conf_event(*cc, r);
          }
        }
      }

      pb::repeated_entry entries;
      m.mutable_entries()->Swap(&entries);
      if (!r.append_entry(std::move(entries))) {
        return new_error(raft_error::PROPOSAL_DROPPED);
      }
      r.bcast_append();
      return {};
    }
    case raftpb::MSG_READ_INDEX: {
      // only one voting member (the leader) in the cluster
      if (r.trk_.is_singleton()) {
        if (auto resp = r.response_to_read_index_req(std::move(m), r.raft_log_handle_.committed()); resp.to() != NONE) {
          r.send(std::move(resp));
        }
        return {};
      }

      // Postpone read only request when this leader has not committed
      // any log entry at its term.
      if (!r.committed_entry_in_current_term()) {
        r.pending_read_index_messages_.Add(std::move(m));
        return {};
      }
      send_msg_read_index_response(r, std::move(m));
      return {};
    }
    case raftpb::MSG_FORGET_LEADER: {
      return {};  // noop on leader
    }
    default:
      break;
  }

  // All other message types require a progress for m.From (pr).
  const auto msg_from = m.from();
  auto pr_iter = r.trk_.progress_map_mutable_view().mutable_view().find(msg_from);
  if (pr_iter == r.trk_.progress_map_mutable_view().mutable_view().end()) {
    // If we are not currently a member of the range (i.e. this node
    // was removed from the configuration while serving as leader),
    // drop any new proposals.
    LOG_DEBUG(r.logger_, "{} no progress available for {}", r.id_, msg_from);
    LOG_INFO(r.logger_, "{} [term {}] ignoring message from unknown node {}", r.id_, r.term_, msg_from);
    return {};
  }
  auto& pr = pr_iter->second;
  switch (msg_type) {
    // Follower → Leader的日志追加响应（场景：日志复制完成通知，发送角色：Follower，接收角色：Leader
    case raftpb::MSG_APP_RESP: {
      // NB: this code path is also hit from (*raft).advance, where the leader steps
      // an MsgAppResp to acknowledge the appended entries in the last Ready.

      // 将对应跟随者（Progress）标记为活跃状态，避免被领导者误认为宕机
      pr.set_recent_active(true);
      if (m.reject()) {
        // RejectHint is the suggested next base entry for appending (i.e.
        // we try to append entry RejectHint+1 next), and LogTerm is the
        // term that the follower has at index RejectHint. Older versions
        // of this library did not populate LogTerm for rejections and it
        // is zero for followers with an empty log.
        //
        // Under normal circumstances, the leader's log is longer than the
        // follower's and the follower's log is a prefix of the leader's
        // (i.e. there is no divergent uncommitted suffix of the log on the
        // follower). In that case, the first probe reveals where the
        // follower's log ends (RejectHint=follower's last index) and the
        // subsequent probe succeeds.
        //
        // However, when networks are partitioned or systems overloaded,
        // large divergent log tails can occur. The naive attempt, probing
        // entry by entry in decreasing order, will be the product of the
        // length of the diverging tails and the network round-trip latency,
        // which can easily result in hours of time spent probing and can
        // even cause outright outages. The probes are thus optimized as
        // described below.

        // ​​RejectHint​​：跟随者建议的起始索引（即跟随者日志的最后一条索引）。
        // ​​LogTerm​​：跟随者在RejectHint处的任期。
        LOG_DEBUG(r.logger_, "{} received MsgAppResp(rejected, hint: (index {}, term {})) from {} for index {}", r.id_,
                  m.reject_hint(), m.log_term(), msg_from, m.index());
        auto next_probe = m.reject_hint();
        if (m.log_term() > 0) {
          // If the follower has an uncommitted log tail, we would end up
          // probing one by one until we hit the common prefix.
          //
          // For example, if the leader has:
          //
          //   idx        1 2 3 4 5 6 7 8 9
          //              -----------------
          //   term (L)   1 3 3 3 5 5 5 5 5
          //   term (F)   1 1 1 1 2 2
          //
          // Then, after sending an append anchored at (idx=9,term=5) we
          // would receive a RejectHint of 6 and LogTerm of 2. Without the
          // code below, we would try an append at index 6, which would
          // fail again.
          //
          // However, looking only at what the leader knows about its own
          // log and the rejection hint, it is clear that a probe at index
          // 6, 5, 4, 3, and 2 must fail as well:
          //
          // For all of these indexes, the leader's log term is larger than
          // the rejection's log term. If a probe at one of these indexes
          // succeeded, its log term at that index would match the leader's,
          // i.e. 3 or 5 in this example. But the follower already told the
          // leader that it is still at term 2 at index 6, and since the
          // log term only ever goes up (within a log), this is a contradiction.
          //
          // At index 1, however, the leader can draw no such conclusion,
          // as its term 1 is not larger than the term 2 from the
          // follower's rejection. We thus probe at 1, which will succeed
          // in this example. In general, with this approach we probe at
          // most once per term found in the leader's log.
          //
          // There is a similar mechanism on the follower (implemented in
          // handleAppendEntries via a call to findConflictByTerm) that is
          // useful if the follower has a large divergent uncommitted log
          // tail[1], as in this example:
          //
          //   idx        1 2 3 4 5 6 7 8 9
          //              -----------------
          //   term (L)   1 3 3 3 3 3 3 3 7
          //   term (F)   1 3 3 4 4 5 5 5 6
          //
          // Naively, the leader would probe at idx=9, receive a rejection
          // revealing the log term of 6 at the follower. Since the leader's
          // term at the previous index is already smaller than 6, the leader-
          // side optimization discussed above is ineffective. The leader thus
          // probes at index 8 and, naively, receives a rejection for the same
          // index and log term 5. Again, the leader optimization does not improve
          // over linear probing as term 5 is above the leader's term 3 for that
          // and many preceding indexes; the leader would have to probe linearly
          // until it would finally hit index 3, where the probe would succeed.
          //
          // Instead, we apply a similar optimization on the follower. When the
          // follower receives the probe at index 8 (log term 3), it concludes
          // that all of the leader's log preceding that index has log terms of
          // 3 or below. The largest index in the follower's log with a log term
          // of 3 or below is index 3. The follower will thus return a rejection
          // for index=3, log term=3 instead. The leader's next probe will then
          // succeed at that index.
          //
          // [1]: more precisely, if the log terms in the large uncommitted
          // tail on the follower are larger than the leader's. At first,
          // it may seem unintuitive that a follower could even have such
          // a large tail, but it can happen:
          //
          // 1. Leader appends (but does not commit) entries 2 and 3, crashes.
          //   idx        1 2 3 4 5 6 7 8 9
          //              -----------------
          //   term (L)   1 2 2     [crashes]
          //   term (F)   1
          //   term (F)   1
          //
          // 2. a follower becomes leader and appends entries at term 3.
          //              -----------------
          //   term (x)   1 2 2     [down]
          //   term (F)   1 3 3 3 3
          //   term (F)   1
          //
          // 3. term 3 leader goes down, term 2 leader returns as term 4
          //    leader. It commits the log & entries at term 4.
          //
          //              -----------------
          //   term (L)   1 2 2 2
          //   term (x)   1 3 3 3 3 [down]
          //   term (F)   1
          //              -----------------
          //   term (L)   1 2 2 2 4 4 4
          //   term (F)   1 3 3 3 3 [gets probed]
          //   term (F)   1 2 2 2 4 4 4
          //
          // 4. the leader will now probe the returning follower at index
          //    7, the rejection points it at the end of the follower's log
          //    which is at a higher log term than the actually committed
          //    log.
          auto [next_probe_idx, _] = r.raft_log_handle_.find_conflict_by_term(m.reject_hint(), m.log_term());
          next_probe = next_probe_idx;
        }
        if (pr.maybe_decr_to(m.index(), next_probe)) {
          LOG_DEBUG(r.logger_, "{} decreased progress of {} to [{}]", r.id_, msg_from, pr.string());
          if (pr.state() == tracker::state_type::STATE_REPLICATE) {
            pr.become_probe();
          }
          r.send_append(msg_from);
        } else {
          // TDOO explain
        }
      } else {
        // We want to update our tracking if the response updates our
        // matched index or if the response can move a probing peer back
        // into StateReplicate (see heartbeat_rep_recovers_from_probing.txt
        // for an example of the latter case).
        // NB: the same does not make sense for StateSnapshot - if `m.Index`
        // equals pr.Match we know we don't m.Index+1 in our log, so moving
        // back to replicating state is not useful; besides pr.PendingSnapshot
        // would prevent it.

        // 情况一：响应更新了匹配索引（pr.Match）​​
        // 当 Follower 确认接收了新的日志条目时，Leader 需要更新该 Follower 的 Match 索引，以跟踪其日志复制进度。
        // 当响应中的 m.Index 更新了 Follower 的匹配索引 pr.Match（即 Follower 确认了新的日志条目）。

        // ​​情况二：响应使探测状态的节点回到正常复制状态（StateReplicate）​​
        // 当 Follower 处于探测状态（StateProbe）时，Leader
        // 可能因网络不稳定或日志不一致而逐条发送日志条目。若某次响应表明 Follower 已追上进度，Leader 可将其状态切换回
        // StateReplicate，恢复高效批量复制。
        // 当 Follower 处于 StateProbe（探测状态），且响应表明其日志已对齐到当前探测位置（可切换回批量复制模式）。
        if (pr.maybe_update(m.index()) || (pr.match() == m.index() && pr.state() == tracker::state_type::STATE_PROBE)) {
          if (pr.state() == tracker::state_type::STATE_PROBE) {
            pr.become_replicate();
          } else if ((pr.state() == tracker::state_type::STATE_SNAPSHOT) &&
                     ((pr.match() + 1) >= r.raft_log_handle_.first_index())) {
            // Follower 处于快照传输状态 (StateSnapshot)。
            // 其匹配索引 pr.Match + 1 大于等于 Leader 日志的起始索引（即快照已应用，可继续同步后续日志）。
            // 即使存在未完成的快照传输（PendingSnapshot），只要 Follower
            // 的日志索引足够新，允许其通过日志追赶（而非等待快照完成），即可切换回正常复制状态。

            // Note that we don't take into account PendingSnapshot to
            // enter this branch. No matter at which index a snapshot
            // was actually applied, as long as this allows catching up
            // the follower from the log, we will accept it. This gives
            // systems more flexibility in how they implement snapshots;
            // see the comments on PendingSnapshot.
            LOG_DEBUG(r.logger_, "{} recovered from needing snapshot, resumed sending replication messages to {} [{}]",
                      r.id_, msg_from, pr.string());
            // Transition back to replicating state via probing state
            // (which takes the snapshot into account). If we didn't
            // move to replicating state, that would only happen with
            // the next round of appends (but there may not be a next
            // round for a while, exposing an inconsistent RaftStatus).
            // -- ​​必要性​​
            // ​1. ​验证日志连续性​​：
            // 快照可能覆盖部分日志，切换为 StateProbe 后，Leader 会逐条探测 Follower
            // 的日志是否与快照后的日志连续（例如通过 AppendEntries 心跳验证 prevLogIndex 和 prevLogTerm）。
            // ​​2. 确保安全性​​：
            // 避免因快照传输与日志追加的时序问题导致状态不一致（如快照未完全应用时误判日志位置）。

            // -- 直接切换的风险​
            // 1. 状态更新延迟​​：
            // 若 Leader 暂时没有新日志需要发送（next round of appends 未触发），Follower 的状态会停留在
            // StateSnapshot，导致 RaftStatus 显示不一致（例如监控工具误认为 Follower 仍需快照）。
            // 2. ​​潜在逻辑漏洞​​：
            // 快照可能未完全生效，直接切换会导致后续日志追加失败（如 Follower 的日志索引未正确更新）。
            pr.become_probe();
            pr.become_replicate();
          } else if (pr.state() == tracker::state_type::STATE_REPLICATE) {
            pr.mutable_inflights().free_le(m.index());
          }
        }
        if (r.maybe_commit()) {
          // committed index has progressed for the term, so it is safe
          // to respond to pending read index requests
          release_pending_read_index_message(r);
          r.bcast_append();
        } else if (r.id_ != msg_from && pr.can_bump_commit(r.raft_log_handle_.committed())) {
          // This node may be missing the latest commit index, so send it.
          // NB: this is not strictly necessary because the periodic heartbeat
          // messages deliver commit indices too. However, a message sent now
          // may arrive earlier than the next heartbeat fires.
          r.send_append(msg_from);
        }
        // We've updated flow control information above, which may
        // allow us to send multiple (size-limited) in-flight messages
        // at once (such as when transitioning from probe to
        // replicate, or when freeTo() covers multiple messages). If
        // we have more entries to send, send as many messages as we
        // can (without sending empty messages for the commit index)
        if (r.id_ != msg_from) {
          while (r.maybe_send_append(msg_from, false));
        }
        // Transfer leadership is in progress.
        if ((msg_from == r.leader_transferee_) && (pr.match() >= r.raft_log_handle_.last_index())) {
          LOG_INFO(r.logger_, "{} sent MsgTimeoutNow to {} after received MsgAppResp", r.id_, msg_from);
          r.send_timmeout_now(msg_from);
        }
      }
      break;
    }
    case raftpb::MSG_HEARTBEAT_RESP: {
      pr.set_recent_active(true);
      // 恢复向该 follower 发送日志条目（若之前因流量控制暂停）
      pr.set_msg_app_flow_paused(false);
      // NB: if the follower is paused (full Inflights), this will still send an
      // empty append, allowing it to recover from situations in which all the
      // messages that filled up Inflights in the first place were dropped. Note
      // also that the outgoing heartbeat already communicated the commit index.
      //
      // If the follower is fully caught up but also in StateProbe (as can happen
      // if ReportUnreachable was called), we also want to send an append (it will
      // be empty) to allow the follower to transition back to StateReplicate once
      // it responds.
      //
      // Note that StateSnapshot typically satisfies pr.Match < lastIndex, but
      // `pr.Paused()` is always true for StateSnapshot, so sendAppend is a
      // no-op.
      if ((pr.match() < r.raft_log_handle_.last_index()) || (pr.state() == tracker::state_type::STATE_PROBE)) {
        // 发送 empty append message to the follower
        // 以便其可以恢复到正常的复制状态（StateReplicate）
        r.send_append(msg_from);
      }

      // 仅当配置为 ReadOnlySafe（线性一致读）且心跳响应携带上下文（m.Context，即关联的只读请求 ID）时，继续处理。
      if ((r.read_only_.read_only_opt() != read_only_option::READ_ONLY_SAFE) || (m.context().empty())) {
        return {};
      }

      auto ack_result = r.read_only_.recv_ack(msg_from, m.context());
      auto vote_result = r.trk_.config_view().voters.vote_result_statistics(ack_result);
      if (vote_result != quorum::vote_result::VOTE_WON) {
        return {};
      }

      auto rss = r.read_only_.advance(std::move(m));
      for (auto& rs : rss) {
        // C++标准未规定函数参数的求值顺序。需要临时保存这个变量而不是直接传递这个字段
        auto index = rs.index;
        if (auto resp = r.response_to_read_index_req(std::move(rs.req), index); resp.to() != NONE) {
          r.send(std::move(resp));
        }
      }
      break;
    }
    case raftpb::MSG_SNAP_STATUS: {  // 用于管理快照发送后的节点状态转换和流量控制
      if (pr.state() != tracker::state_type::STATE_SNAPSHOT) {
        LOG_DEBUG(r.logger_, "{} [term {}] ignoring MsgSnapStatus from {} in state {}", r.id_, r.term_, msg_from,
                  enum_name(pr.state()));
        return {};
      }
      if (!m.reject()) {
        pr.become_probe();
        LOG_DEBUG(r.logger_, "{} snapshot succeeded, resumed sending replication messages to {} [{}]", r.id_, msg_from,
                  pr.string());
      } else {
        // NB: the order here matters or we'll be probing erroneously from
        // the snapshot index, but the snapshot never applied.
        // 先清除 PendingSnapshot，再切换状态。若顺序颠倒，可能错误地从已废弃的快照索引开始探测。

        // 清除挂起的快照索引，避免后续误用无效快照。
        pr.set_pending_snapshot(0);
        // 进入探测状态，重新尝试日志同步。
        pr.become_probe();
        LOG_DEBUG(r.logger_, "{} snapshot failed, resumed sending replication messages to {} [{}]", r.id_, msg_from,
                  pr.string());
      }
      // If snapshot finish, wait for the MsgAppResp from the remote node before sending
      // out the next MsgApp.
      // If snapshot failure, wait for a heartbeat interval before next try
      pr.set_msg_app_flow_paused(true);
      break;
    }
    case raftpb::MSG_UNREACHABLE: {  // 处理不可达节点的消息
      // During optimistic replication, if the remote becomes unreachable,
      // there is huge probability that a MsgApp is lost.
      if (pr.state() == tracker::state_type::STATE_REPLICATE) {
        pr.become_probe();
      }
      LOG_DEBUG(r.logger_, "{} failed to send message to {} because it is unreachable [{}]", r.id_, msg_from,
                pr.string());
      break;
    }
    case raftpb::MSG_TRANSFER_LEADER: {
      if (r.is_learner_) {
        LOG_DEBUG(r.logger_, "{} is learner. Ignored transferring leadership", r.id_);
        return {};
      }
      auto leader_transferee = m.from();
      auto last_leader_transferee = r.leader_transferee_;
      if (last_leader_transferee != NONE) {
        if (leader_transferee == last_leader_transferee) {
          LOG_DEBUG(r.logger_, "{} [term {}] transfer leadership to {} is in progress, ignores request to same node {}",
                    r.id_, r.term_, last_leader_transferee, leader_transferee);
          return {};
        }
        r.abort_leader_transfer();
        LOG_INFO(r.logger_, "{} [term {}] abort transfer leadership to {} and transfer to {}", r.id_, r.term_,
                 last_leader_transferee, leader_transferee);
      }
      if (leader_transferee == r.id_) {
        LOG_DEBUG(r.logger_, "{} [term {}] transfer leadership to self, ignores request", r.id_, r.term_);
        return {};
      }
      // Transfer leadership to third party.
      LOG_INFO(r.logger_, "{} [term {}] starts to transfer leadership to {}", r.id_, r.term_, leader_transferee);
      // Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
      r.election_elapsed_ = 0;
      r.leader_transferee_ = leader_transferee;
      if (pr.match() == r.raft_log_handle_.last_index()) {
        // If the transferee is up to date, send MsgTimeoutNow to it.
        LOG_INFO(r.logger_, "{} sends MsgTimeoutNow to {} immediately as {} already has up-to-date log", r.id_,
                 leader_transferee, leader_transferee);
        r.send_timmeout_now(leader_transferee);
      } else {
        r.send_append(leader_transferee);
      }
      break;
    }
    default:
      LOG_TRACE(r.logger_, "{} [term {}] ignoring message from {} in state {}", r.id_, r.term_, msg_from,
                enum_name(pr.state()));
      break;
  }
  return {};
}

leaf::result<void> step_candidate(raft& r, raftpb::message&& m) {
  // Only handle vote responses corresponding to our candidacy (while in
  // StateCandidate, we may get stale MsgPreVoteResp messages in this term from
  // our pre-candidate state).
  auto post_vote_resp_func = [&]() {
    const auto [gr, rj, res] = r.poll(m.from(), m.type(), !m.reject());
    LOG_INFO(r.logger_, "{} has received {} {} votes and {} vote rejections", r.id_, gr, enum_name(m.type()), rj);
    switch (res) {
      case quorum::vote_result::VOTE_WON:
        if (r.state_type_ == state_type::PRE_CANDIDATE) {
          r.campaign(campaign_type::ELECTION);
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
  const auto msg_type = m.type();
  switch (msg_type) {
    case raftpb::MSG_PROP: {
      LOG_INFO(r.logger_, "{} no leader at term {}; dropping proposal", r.id_, r.term_);
      return new_error(raft_error::PROPOSAL_DROPPED);
    }
    case raftpb::MSG_APP: {
      r.become_follower(m.term(), m.from());  // always m.Term == r.Term
      r.handle_append_entries(std::move(m));
      break;
    }
    case raftpb::MSG_VOTE_RESP: {
      if (r.state_type_ == state_type::CANDIDATE) {
        post_vote_resp_func();
      }
      break;
    }
    case raftpb::MSG_PRE_VOTE_RESP: {
      if (r.state_type_ == state_type::PRE_CANDIDATE) {
        if (msg_type == raftpb::MSG_PRE_VOTE_RESP) {
          post_vote_resp_func();
        }
      }
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
        LOG_INFO(r.logger_, "{} no leader at term {}; dropping proposal", r.id_, r.term_);
        return new_error(raft_error::PROPOSAL_DROPPED);
      } else if (r.disable_proposal_forwarding_) {
        // 若配置禁止转发（disableProposalForwarding），拒绝提案（避免脑裂时多
        // Leader 写入）。
        LOG_INFO(r.logger_,
                 "{} not forwarding to leader {} at term {}; dropping "
                 "proposal",
                 r.id_, r.lead_, r.term_);
        return new_error(raft_error::PROPOSAL_DROPPED);
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
      r.handle_heartbeat(std::move(m));  // 调用 handleHeartbeat 更新提交索引并可能触发日志提交。
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
        LOG_INFO(r.logger_, "{} no leader at term {}; dropping leader transfer msg", r.id_, r.term_);
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
      LOG_INFO(r.logger_,
               "{} [term {}] received MsgTimeoutNow from {} and starts an election "
               "to get leadership.",
               r.id_, r.term_, m.from());
      // Leadership transfers never use pre-vote even if r.preVote is true; we
      // know we are not recovering from a partition so there is no need for the
      // extra round trip.
      r.hup(campaign_type::TRANSFER);
      break;
    }
    case raftpb::MSG_READ_INDEX: {
      if (r.lead_ == NONE) {
        LOG_INFO(r.logger_, "{} no leader at term {}; dropping index reading msg", r.id_, r.term_);
        return {};
      }
      m.set_to(r.lead_);
      r.send(std::move(m));
      break;
    }
    case raftpb::MSG_READ_INDEX_RESP: {
      if (m.entries_size() != 1) {
        LOG_INFO(r.logger_, "{} invalid format of MsgReadIndexResp from {}", r.id_, m.from());
        return {};
      }
      r.read_states_.emplace_back(read_state{m.index(), m.entries(0).data()});
      break;
    }
    case raftpb::MSG_FORGET_LEADER: {  // 强制 Follower 忘记当前
                                       // Leader（用于网络分区恢复后清理旧
                                       // Leader）。
      if (r.read_only_.read_only_opt() == read_only_option::READ_ONLY_LEASE_BASED) {
        // 若使用 ReadOnlyLeaseBased（租约机制需持续 Leader 心跳），忽略此消息。
        LOG_ERROR(r.logger_, "ignoring MsgForgetLeader due to ReadOnlyLeaseBased", r.id_, r.term_);
        return {};
      }
      if (r.lead_ != NONE) {
        LOG_INFO(r.logger_, "{} forgetting leader {} at term {}", r.id_, r.lead_, r.term_);
        r.lead_ = NONE;  // 清除当前 Leader ID
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

raftpb::hard_state raft::hard_state() const {
  raftpb::hard_state hs;
  hs.set_term(term_);
  if (vote_id_ != 0) {
    hs.set_vote(vote_id_);
  }
  hs.set_commit(raft_log_handle_.committed());
  return hs;
}

basic_status raft::get_basic_status() const {
  return basic_status{
      .id = id_,
      .hard_state = hard_state(),
      .soft_state = soft_state(),
      .applied = raft_log_handle_.applied(),
      .lead_transferee = leader_transferee_,
  };
}

status raft::get_status() const {
  return status{
      .basic_status = get_basic_status(),
      .config = trk_.config_view().clone(),
      .progress = state_type_ == state_type::LEADER ? trk_.progress_map_view().clone() : tracker::progress_map{},
  };
}

void raft::send(raftpb::message&& message) {
  if (message.from() == NONE) {
    message.set_from(id_);
  }

  const auto msg_type = message.type();
  // 处理选举类消息
  if (msg_type == raftpb::message_type::MSG_VOTE || msg_type == raftpb::message_type::MSG_VOTE_RESP ||
      msg_type == raftpb::message_type::MSG_PRE_VOTE || msg_type == raftpb::message_type::MSG_PRE_VOTE_RESP) {
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
      LEPTON_CRITICAL("term should be set when msg type:{}", enum_name(msg_type));
    }
  } else {  // 非选举类消息，必须自动设置term
    if (message.term() != 0) {
      LEPTON_CRITICAL("term should not be set when msg type:{}", enum_name(msg_type));
    }
    // do not attach term to MsgProp, MsgReadIndex
    // proposals are a way to forward to the leader and
    // should be treated as local message.
    // MsgReadIndex is also forwarded to leader.
    // MsgProp 和 MsgReadIndex 不设置 Term，因为它们可能被转发给 Leader，由
    // Leader 处理时再填充正确 Term。
    if (msg_type != raftpb::message_type::MSG_PROP && msg_type != raftpb::message_type::MSG_READ_INDEX) {
      message.set_term(term_);
    }
  }
  if (msg_type == raftpb::message_type::MSG_APP_RESP || msg_type == raftpb::message_type::MSG_VOTE_RESP ||
      msg_type == raftpb::message_type::MSG_PRE_VOTE_RESP) {
    // If async storage writes are enabled, messages added to the msgs slice
    // are allowed to be sent out before unstable state (e.g. log entry
    // writes and election votes) have been durably synced to the local
    // disk.
    //
    // For most message types, this is not an issue. However, response
    // messages that relate to "voting" on either leader election or log
    // appends require durability before they can be sent. It would be
    // incorrect to publish a vote in an election before that vote has been
    // synced to stable storage locally. Similarly, it would be incorrect to
    // acknowledge a log append to the leader before that entry has been
    // synced to stable storage locally.
    //
    // Per the Raft thesis, section 3.8 Persisted state and server restarts:
    //
    // > Raft servers must persist enough information to stable storage to
    // > survive server restarts safely. In particular, each server persists
    // > its current term and vote; this is necessary to prevent the server
    // > from voting twice in the same term or replacing log entries from a
    // > newer leader with those from a deposed leader. Each server also
    // > persists new log entries before they are counted towards the entries’
    // > commitment; this prevents committed entries from being lost or
    // > “uncommitted” when servers restart
    //
    // To enforce this durability requirement, these response messages are
    // queued to be sent out as soon as the current collection of unstable
    // state (the state that the response message was predicated upon) has
    // been durably persisted. This unstable state may have already been
    // passed to a Ready struct whose persistence is in progress or may be
    // waiting for the next Ready struct to begin being written to Storage.
    // These messages must wait for all of this state to be durable before
    // being published.
    //
    // Rejected responses (m.Reject == true) present an interesting case
    // where the durability requirement is less unambiguous. A rejection may
    // be predicated upon unstable state. For instance, a node may reject a
    // vote for one peer because it has already begun syncing its vote for
    // another peer. Or it may reject a vote from one peer because it has
    // unstable log entries that indicate that the peer is behind on its
    // log. In these cases, it is likely safe to send out the rejection
    // response immediately without compromising safety in the presence of a
    // server restart. However, because these rejections are rare and
    // because the safety of such behavior has not been formally verified,
    // we err on the side of safety and omit a `&& !m.Reject` condition
    // above.
    trace_receive_message(*this, message);
    msgs_after_append_.Add(std::move(message));
  } else {
    if (message.to() == id_) {
      LEPTON_CRITICAL("message should not be self-addressed when sending {}", enum_name(msg_type));
    }
    trace_receive_message(*this, message);
    msgs_.Add(std::move(message));
  }
}

void raft::send_append(std::uint64_t id) { maybe_send_append(id, true); }

bool raft::maybe_send_append(std::uint64_t to, bool send_if_empty) {
  auto pr_iter = trk_.progress_map_mutable_view().mutable_view().find(to);
  assert(pr_iter != trk_.progress_map_mutable_view().mutable_view().end());
  auto& pr = pr_iter->second;
  if (pr.is_paused()) {
    return false;
  }
  auto pr_next = pr.next();
  assert(pr_next > 0);
  auto prev_index = pr_next - 1;
  auto prev_term = raft_log_handle_.term(prev_index);
  if (!prev_term) {
    // The log probably got truncated at >= pr.Next, so we can't catch up the
    // follower log anymore. Send a snapshot instead.
    return maybe_send_snapshot(to, pr);
  }

  pb::repeated_entry ents;
  bool has_error = false;
  // In a throttled StateReplicate only send empty MsgApp, to ensure progress.
  // Otherwise, if we had a full Inflights and all inflight messages were in
  // fact dropped, replication to that follower would stall. Instead, an empty
  // MsgApp will eventually reach the follower (heartbeats responses prompt the
  // leader to send an append), allowing it to be acked or rejected, both of
  // which will clear out Inflights.
  if (pr.state() != tracker::state_type::STATE_REPLICATE || !pr.ref_inflights().full()) {
    auto ents_result = raft_log_handle_.entries(pr_next, max_msg_size_);
    if (ents_result) {
      ents = std::move(*ents_result);
    } else {
      has_error = true;
    }
  }

  if (ents.empty() && !send_if_empty) {
    return false;
  }

  // TODO(pav-kv): move this check up to where err is returned.
  if (has_error) {  // send a snapshot if we failed to get the entries
    return maybe_send_snapshot(to, pr);
  }

  // Send the actual MsgApp otherwise, and update the progress accordingly.
  raftpb::message msg;
  msg.set_to(to);
  msg.set_type(raftpb::message_type::MSG_APP);
  msg.set_index(prev_index);
  msg.set_log_term(prev_term.value());
  msg.mutable_entries()->Swap(&ents);
  msg.set_commit(raft_log_handle_.committed());
  const auto entries_size = static_cast<std::uint64_t>(msg.entries_size());
  const auto bytes = pb::payloads_size(msg.entries());
  send(std::move(msg));
  pr.sent_entries(entries_size, bytes);
  pr.sent_commit(raft_log_handle_.committed());
  return true;
}

bool raft::maybe_send_snapshot(std::uint64_t to, tracker::progress& pr) {
  if (!pr.recent_active()) {
    return false;
  }

  auto snap_result = leaf::try_handle_some(
      [&]() -> leaf::result<raftpb::snapshot> {
        BOOST_LEAF_AUTO(result, raft_log_handle_.snapshot());
        return result;
      },
      [&](const lepton::lepton_error& err) -> leaf::result<raftpb::snapshot> {
        if (err == storage_error::SNAPSHOT_TEMPORARILY_UNAVAILABLE) {
          LOG_DEBUG(logger_, "{} failed to send snapshot to {} because snapshot is temporarily unavailable", id_, to);
          return new_error(err);
        }
        LEPTON_CRITICAL(err.message);
        return new_error(err);
      });
  ;
  // 预期这里只会出现 SNAPSHOT_TEMPORARILY_UNAVAILABLE
  if (!snap_result) {
    return false;
  }
  if (pb::is_empty_snap(snap_result.value())) {
    LEPTON_CRITICAL("need non-empty snapshot");
  }

  auto sindex = snap_result->metadata().index();
  LOG_DEBUG(logger_, "{} [firstindex: {}, commit: {}] sent snapshot[index: {}, term: {}] to {} [{}]", id_,
            raft_log_handle_.first_index(), raft_log_handle_.committed(), sindex, snap_result->metadata().term(), to,
            pr.string());
  pr.become_snapshot(sindex);
  LOG_DEBUG(logger_, "{} paused sending replication messages to {} [{}]", id_, to, pr.string());

  raftpb::message msg;
  msg.set_to(to);
  msg.set_type(raftpb::message_type::MSG_SNAP);
  msg.mutable_snapshot()->Swap(&*snap_result);
  // existing_snap 现在已为空
  assert(!snap_result->has_metadata());
  send(std::move(msg));
  return true;
}

void raft::send_heartbeat(std::uint64_t id, std::string&& ctx) {
  auto pr_iter = trk_.progress_map_mutable_view().mutable_view().find(id);
  // Attach the commit as min(to.matched, r.committed).
  // When the leader sends out heartbeat message,
  // the receiver(follower) might not be matched with the leader
  // or it might not have all the committed entries.
  // The leader MUST NOT forward the follower's commit to
  // an unmatched index.
  auto commit = std::min(pr_iter->second.match(), raft_log_handle_.committed());
  raftpb::message m;
  m.set_to(id);
  m.set_type(raftpb::message_type::MSG_HEARTBEAT);
  m.set_commit(commit);
  if (!ctx.empty()) {
    *m.mutable_context() = std::move(ctx);
  }
  send(std::move(m));
  pr_iter->second.sent_commit(commit);
}

void raft::bcast_append() {
  auto& progress_map = trk_.progress_map_view().view();
  for (const auto& [id, pr] : progress_map) {
    if (id == id_) {
      continue;
    }
    send_append(id);
  }
}

void raft::bcast_heartbeat() { bcast_heartbeat_with_ctx(read_only_.last_pending_request_ctx()); }

void raft::bcast_heartbeat_with_ctx(std::string&& ctx) {
  trk_.visit([&](std::uint64_t id) {
    if (id == id_) {
      return;
    }
    send_heartbeat(id, std::string(ctx));
  });
}

void raft::applied_to(std::uint64_t index, pb::entry_encoding_size size) {
  auto old_applied = raft_log_handle_.applied();
  auto new_applied = std::max(index, old_applied);
  raft_log_handle_.applied_to(new_applied, size);
  if (trk_.config_view().auto_leave && new_applied >= pending_conf_index_ && state_type_ == state_type::LEADER) {
    // If the current (and most recent, at least for this leader's term)
    // configuration should be auto-left, initiate that now. We use a
    // nil Data which unmarshals into an empty ConfChangeV2 and has the
    // benefit that appendEntry can never refuse it based on its size
    // (which registers as zero).
    auto m = leaf::try_handle_some(
        [&]() -> leaf::result<raftpb::message> {
          pb::conf_change_var empty_cc = std::monostate{};
          BOOST_LEAF_AUTO(v, pb::conf_change_to_message(empty_cc));
          return v;
        },
        [&](const lepton::lepton_error& err) -> leaf::result<raftpb::message> {
          LEPTON_CRITICAL("{} failed to convert conf change to raftpb::message, error: {}", id_, err.message);
          return {};
        });
    assert(m);
    // NB: this proposal can't be dropped due to size, but can be
    // dropped if a leadership transfer is in progress. We'll keep
    // checking this condition on each applied entry, so either the
    // leadership transfer will succeed and the new leader will leave
    // the joint configuration, or the leadership transfer will fail,
    // and we will propose the config change on the next advance.
    auto _ = leaf::try_handle_some(
        [&]() -> leaf::result<void> {
          LEPTON_LEAF_CHECK(step(std::move(m.value())));
          LOG_INFO(logger_, "initiating automatic transition out of joint configuration {}",
                   trk_.config_view().string());
          return {};
        },
        [&](const lepton::lepton_error& err) -> leaf::result<void> {
          LEPTON_UNUSED(err);
          LOG_DEBUG(logger_, "not initiating automatic transition out of joint configuration {}: {}",
                    trk_.config_view().string(), err.message);
          return {};
        });
  }
}

void raft::applied_snap(const raftpb::snapshot& snapshot) {
  auto index = snapshot.metadata().index();
  raft_log_handle_.stable_snap_to(index);
  applied_to(index, 0);
}

bool raft::maybe_commit() {
  auto mci = trk_.committed();
  return raft_log_handle_.maybe_commit(pb::entry_id{.term = term_, .index = mci});
}

void raft::reset(std::uint64_t term) {
  if (term_ != term) {
    term_ = term;
    vote_id_ = NONE;
  }
  lead_ = NONE;
  election_elapsed_ = 0;
  heartbeat_elapsed_ = 0;
  reset_randomized_election_timeout();

  abort_leader_transfer();

  trk_.reset_votes();

  trk_.visit([&](std::uint64_t id, tracker::progress& pr) {
    pr = tracker::progress{raft_log_handle_.last_index() + 1,
                           tracker::inflights{trk_.max_inflight(), trk_.max_inflight_bytes()}, pr.is_learner(), false};
    if (id_ == id) {
      pr.set_match(raft_log_handle_.last_index());
    }
  });

  pending_conf_index_ = 0;
  uncommitted_size_ = 0;
  read_only_ = read_only{read_only_.read_only_opt()};
}

bool raft::append_entry(pb::repeated_entry&& entries) {
  auto li = raft_log_handle_.last_index();
  for (int i = 0; i < entries.size(); ++i) {
    entries[i].set_term(term_);
    entries[i].set_index(li + 1 + static_cast<std::uint64_t>(i));
  }
  // Track the size of this uncommitted proposal.
  if (!increase_uncommitted_size(entries)) {
    LOG_WARN(logger_, "{} appending new entries to log would exceed uncommitted entry size limit; dropping proposal",
             id_);
    // Drop the proposal.
    return false;
  }

  trace_replicate(*this, entries);

  // use latest "last" index after truncate/append
  li = raft_log_handle_.append(std::move(entries));
  // The leader needs to self-ack the entries just appended once they have
  // been durably persisted (since it doesn't send an MsgApp to itself). This
  // response message will be added to msgsAfterAppend and delivered back to
  // this node after these entries have been written to stable storage. When
  // handled, this is roughly equivalent to:
  //
  //  r.trk.Progress[r.id].MaybeUpdate(e.Index)
  //  if r.maybeCommit() {
  //  	r.bcastAppend()
  //  }
  // 为了减少一次RPC，且避免死循环（​​自我响应悖论​​：发送 MsgApp
  // 后需要等待自己的响应才能确认持久化 → 但响应又依赖于持久化完成 → 逻辑死结）
  // 所以leader 不能自己给自己发送 MSG_APP 消息
  raftpb::message m;
  m.set_to(id_);
  m.set_type(raftpb::message_type::MSG_APP_RESP);
  m.set_index(li);
  send(std::move(m));
  return true;
}

void raft::tick_election() {
  election_elapsed_++;
  if (promotable() && past_election_timeout()) {
    election_elapsed_ = 0;
    raftpb::message m;
    m.set_from(id_);
    m.set_type(raftpb::message_type::MSG_HUP);
    auto _ = leaf::try_handle_some(
        [&]() -> leaf::result<void> {
          LEPTON_LEAF_CHECK(step(std::move(m)));
          return {};
        },
        [&](const lepton::lepton_error& err) -> leaf::result<void> {
          LEPTON_UNUSED(err);
          LOG_DEBUG(logger_, "error occurred during election: {}", err.message);
          return {};
        });
  }
}

void raft::tick_heartbeat() {
  heartbeat_elapsed_++;
  election_elapsed_++;

  if (election_elapsed_ >= election_timeout_) {
    election_elapsed_ = 0;
    if (check_quorum_) {
      raftpb::message m;
      m.set_from(id_);
      m.set_type(raftpb::message_type::MSG_CHECK_QUORUM);
      auto _ = leaf::try_handle_some(
          [&]() -> leaf::result<void> {
            LEPTON_LEAF_CHECK(step(std::move(m)));
            return {};
          },
          [&](const lepton::lepton_error& err) -> leaf::result<void> {
            LEPTON_UNUSED(err);
            LOG_DEBUG(logger_, "{} failed to send check quorum message: {}", id_, err.message);
            return {};
          });
    }
    // If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
    // 若在 electionTimeout 内未完成转移，Leader 终止流程继续服务，避免阻塞。
    if ((state_type_ == state_type::LEADER) && (leader_transferee_ != NONE)) {
      LOG_DEBUG(logger_, "{} [term {}] leader transfer timeout, become leader again", id_, term_);
      abort_leader_transfer();
    }
  }
  if (state_type_ != state_type::LEADER) {
    return;
  }
  if (heartbeat_elapsed_ >= heartbeat_timeout_) {
    heartbeat_elapsed_ = 0;
    raftpb::message m;
    m.set_from(id_);
    m.set_type(raftpb::message_type::MSG_BEAT);
    auto _ = leaf::try_handle_some(
        [&]() -> leaf::result<void> {
          LEPTON_LEAF_CHECK(step(std::move(m)));
          return {};
        },
        [&](const lepton::lepton_error& err) -> leaf::result<void> {
          LEPTON_UNUSED(err);
          LOG_DEBUG(logger_, "{} error occurred during checking sending heartbeat: {}", id_, err.message);
          return {};
        });
  }
}

void raft::become_follower(std::uint64_t term, std::uint64_t lead) {
  step_func_ = &step_follower;
  reset(term);
  tick_func_ = &raft::tick_election;
  lead_ = lead;
  state_type_ = state_type::FOLLOWER;
  LOG_INFO(logger_, "{} became follower at term {}", id_, term_);

  trace_become_follower(*this);
}

void raft::become_candidate() {
  // TODO(xiangli) remove the panic when the raft implementation is stable
  if (state_type_ == state_type::LEADER) {
    LEPTON_CRITICAL("{} [term {}] invalid transition [leader -> candidate]", id_, term_);
  }
  // Becoming a candidate changes our step functions and state, but
  // doesn't change anything else. In particular it does not increase
  // r.Term or change r.Vote.
  step_func_ = step_candidate;
  reset(term_ + 1);
  tick_func_ = &raft::tick_election;
  vote_id_ = id_;
  state_type_ = state_type::CANDIDATE;
  LOG_INFO(logger_, "{} became candidate at term {}", id_, term_);

  trace_become_candidate(*this);
}

void raft::become_pre_candidate() {
  // TODO(xiangli) remove the panic when the raft implementation is stable
  if (state_type_ == state_type::LEADER) {
    LEPTON_CRITICAL("{} [term {}] invalid transition [leader -> pre-candidate]", id_, term_);
  }
  // Becoming a pre-candidate changes our step functions and state,
  // but doesn't change anything else. In particular it does not increase
  // r.Term or change r.Vote.
  step_func_ = step_candidate;
  trk_.reset_votes();
  tick_func_ = &raft::tick_election;
  lead_ = NONE;
  state_type_ = state_type::PRE_CANDIDATE;
  LOG_INFO(logger_, "{} became pre-candidate at term {}", id_, term_);
}

void raft::become_leader() {
  // TODO(xiangli) remove the panic when the raft implementation is stable
  if (state_type_ == state_type::FOLLOWER) {
    LEPTON_CRITICAL("{} [term {}] invalid transition [follower -> leader]", id_, term_);
  }
  step_func_ = step_leader;
  reset(term_);
  tick_func_ = &raft::tick_heartbeat;
  lead_ = id_;
  state_type_ = state_type::LEADER;
  // Followers enter replicate mode when they've been successfully probed
  // (perhaps after having received a snapshot as a result). The leader is
  // trivially in this state. Note that r.reset() has initialized this
  // progress with the last index already.
  auto& pr = trk_.progress_map_mutable_view().mutable_view().at(id_);
  pr.become_replicate();
  // The leader always has RecentActive == true; MsgCheckQuorum makes sure to
  // preserve this.
  pr.set_recent_active(true);

  // Conservatively set the pendingConfIndex to the last index in the
  // log. There may or may not be a pending config change, but it's
  // safe to delay any future proposals until we commit all our
  // pending log entries, and scanning the entire tail of the log
  // could be expensive.
  pending_conf_index_ = raft_log_handle_.last_index();
  trace_become_leader(*this);
  pb::repeated_entry empty_ents;
  empty_ents.Add();
  if (!append_entry(std::move(empty_ents))) {
    // This won't happen because we just called reset() above.
    LEPTON_CRITICAL("empty entry was dropped");
  }
  // The payloadSize of an empty entry is 0 (see TestPayloadSizeOfEmptyEntry),
  // so the preceding log append does not count against the uncommitted log
  // quota of the new leader. In other words, after the call to appendEntry,
  // r.uncommittedSize is still 0.
  LOG_INFO(logger_, "{} became leader at term {}", id_, term_, term_);
}

// hup 函数用于让当前 Raft 节点发起一次领导者选举（Campaign），但仅在以下条件满足时触发：
// 节点当前不是领导者。
// 节点有资格参与选举（可提升为候选者）。
// 没有未应用的集群配置变更。
void raft::hup(campaign_type t) {
  if (state_type_ == state_type::LEADER) {
    LOG_DEBUG(logger_, "{} [term {}] ignoring MsgHup because already leader", id_, term_);
    return;
  }

  if (!promotable()) {
    LOG_WARN(logger_, "{} is unpromotable and can not campaign", id_);
    return;
  }
  if (has_unapplied_conf_change()) {
    LOG_WARN(logger_, "{} cannot campaign at term {} since there are still pending configuration changes to apply", id_,
             term_);
    return;
  }
  LOG_INFO(logger_, "{} is starting a new election at term {}", id_, term_);
  campaign(t);
}

bool raft::has_unapplied_conf_change() const {
  if (raft_log_handle_.applied() >= raft_log_handle_.committed()) {  // in fact applied == committed
    return false;
  }
  auto found = false;
  // Scan all unapplied committed entries to find a config change. Paginate the
  // scan, to avoid a potentially unlimited memory spike.
  auto lo = raft_log_handle_.applied() + 1;
  auto hi = raft_log_handle_.committed() + 1;
  // Reuse the maxApplyingEntsSize limit because it is used for similar purposes
  // (limiting the read of unapplied committed entries) when raft sends entries
  // via the Ready struct for application.
  // TODO(pavelkalinnikov): find a way to budget memory/bandwidth for this scan
  // outside the raft package.
  auto page_size = raft_log_handle_.max_applying_ents_size();
  auto result = leaf::try_handle_some(
      [&]() -> leaf::result<void> {
        LEPTON_LEAF_CHECK(
            raft_log_handle_.scan(lo, hi, page_size, [&](const pb::entry_view& entries) -> leaf::result<void> {
              for (auto& iter : entries) {
                if (iter.type() == raftpb::entry_type::ENTRY_CONF_CHANGE ||
                    iter.type() == raftpb::entry_type::ENTRY_CONF_CHANGE_V2) {
                  found = true;
                  return new_error(logic_error::LOOP_BREAK);
                }
              }
              return {};
            }));
        return {};
      },
      [&](const lepton::lepton_error& err) -> leaf::result<void> {
        if (err == logic_error::LOOP_BREAK) {
          return {};
        }
        LEPTON_CRITICAL("error scanning unapplied entries [{}, {}): {}", lo, hi, err.message);
        return new_error(err);
      });
  assert(result);
  return found;
}

void raft::campaign(campaign_type t) {
  if (!promotable()) {
    // This path should not be hit (callers are supposed to check), but
    // better safe than sorry.
    LOG_WARN(logger_, "{} is unpromotable; campaign() should have been called", id_);
  }
  std::uint64_t term = 0;
  raftpb::message_type vote_msg_type;
  if (t == campaign_type::PRE_ELECTION) {
    become_pre_candidate();
    vote_msg_type = raftpb::message_type::MSG_PRE_VOTE;
    // PreVote RPCs are sent for the next term before we've incremented r.Term.
    term = term_ + 1;
  } else {
    become_candidate();
    vote_msg_type = raftpb::message_type::MSG_VOTE;
    term = term_;
  }
  auto ids = trk_.voter_nodes();
  for (const auto& id : ids) {
    raftpb::message m;
    m.set_to(id);
    m.set_term(term);

    if (id == id_) {
      m.set_type(pb::vote_resp_msg_type(vote_msg_type));
      // The candidate votes for itself and should account for this self
      // vote once the vote has been durably persisted (since it doesn't
      // send a MsgVote to itself). This response message will be added to
      // msgsAfterAppend and delivered back to this node after the vote
      // has been written to stable storage.
      send(std::move(m));
      continue;
    }
    auto last = raft_log_handle_.last_entry_id();
    LOG_INFO(logger_, "{} [logterm: {}, index: {}] sent {} request to {} at term {}", id_, last.term, last.index,
             enum_name(vote_msg_type), id, term_);
    std::string ctx;
    if (t == campaign_type::TRANSFER) {
      ctx = enum_name(t);
    }
    m.set_type(vote_msg_type);
    m.set_index(last.index);
    m.set_log_term(last.term);
    if (!ctx.empty()) {
      *m.mutable_context() = std::move(ctx);
    }
    send(std::move(m));
  }
}

std::tuple<std::uint64_t, std::uint64_t, quorum::vote_result> raft::poll(std::uint64_t id, raftpb::message_type vt,
                                                                         bool vote) {
  if (vote) {
    LOG_INFO(logger_, "{} received {} from {} at term {}", id_, enum_name(vt), id, term_);
  } else {
    LOG_INFO(logger_, "{} received {} rejection from {} at term {}", id_, enum_name(vt), id, term_);
  }
  trk_.record_vote(id, vote);
  return trk_.tally_votes();
}

leaf::result<void> raft::step(raftpb::message&& m) {
  trace_receive_message(*this, m);
  auto msg_type = m.type();
  LOG_TRACE(logger_, "{} [term {}] [commit {}] ready step message: {} {}", id_, term_, raft_log_handle_.committed(),
            enum_name(msg_type), m.DebugString());
  // Handle the message term, which may result in our stepping down to a follower.
  if (m.term() == 0) {
    // local message
    LOG_TRACE(logger_, "{} [term {}] received local message: {}", id_, term_, m.DebugString());
  } else if (m.term() > term_) {
    if ((m.type() == raftpb::message_type::MSG_VOTE) || (m.type() == raftpb::message_type::MSG_PRE_VOTE)) {
      auto force = m.context() == enum_name(campaign_type::TRANSFER);
      // 当前节点是否在租约期内（即 Leader 有效期内）
      auto in_lease = check_quorum_ && (lead_ != NONE) && (election_elapsed_ < election_timeout_);
      // 若在租约期内且非强制转移，忽略投票请求，防止无效选举
      if (!force && in_lease) {
        // If a server receives a RequestVote request within the minimum election timeout
        // of hearing from a current leader, it does not update its term or grant its vote
        auto last = raft_log_handle_.last_entry_id();
        // TODO(pav-kv): it should be ok to simply print the %+v of the lastEntryID.
        LOG_INFO(logger_,
                 "{} [logterm: {}, index: {}, vote: {}] ignored {} from {} [logterm: {}, index: {}] at term {}: lease "
                 "is not expired (remaining ticks: {})",
                 id_, last.term, last.index, vote_id_, enum_name(m.type()), m.from(), m.log_term(), m.index(), term_,
                 election_timeout_ - election_elapsed_);
        return {};
      }
    }
    if (m.type() == raftpb::message_type::MSG_PRE_VOTE) {
      // Never change our term in response to a PreVote
      LOG_TRACE(logger_, "{} [term {}] ignored PreVote from {} [term: {}, index: {}]", id_, term_, m.from(), m.term(),
                m.index());
    } else if ((msg_type == raftpb::message_type::MSG_PRE_VOTE_RESP) && !m.reject()) {
      // We send pre-vote requests with a term in our future. If the
      // pre-vote is granted, we will increment our term when we get a
      // quorum. If it is not, the term comes from the node that
      // rejected our vote so we should become a follower at the new
      // term.
      LOG_TRACE(logger_, "{} [term: {}] received PreVoteResp from {} [term: {}, index: {}]", id_, term_, m.from(),
                m.term(), m.index());
    } else {
      LOG_INFO(logger_, "{} [term: {}] received a {} message with higher term from {} [term: {}]", id_, term_,
               enum_name(m.type()), m.from(), m.term());
      if (m.type() == raftpb::message_type::MSG_APP || m.type() == raftpb::message_type::MSG_HEARTBEAT ||
          m.type() == raftpb::message_type::MSG_SNAP) {
        // MsgApp/MsgHeartbeat/MsgSnap：来自合法 Leader 的消息，直接更新 Leader。
        become_follower(m.term(), m.from());
      } else {
        // 其他消息（如 MsgVote）：转为 Follower，但 Leader 未知（设为 None）。
        become_follower(m.term(), NONE);
      }
    }
  } else if (m.term() < term_) {
    if ((check_quorum_ || pre_vote_) &&
        (m.type() == raftpb::message_type::MSG_HEARTBEAT || m.type() == raftpb::message_type::MSG_APP)) {
      // We have received messages from a leader at a lower term. It is possible
      // that these messages were simply delayed in the network, but this could
      // also mean that this node has advanced its term number during a network
      // partition, and it is now unable to either win an election or to rejoin
      // the majority on the old term. If checkQuorum is false, this will be
      // handled by incrementing term numbers in response to MsgVote with a
      // higher term, but if checkQuorum is true we may not advance the term on
      // MsgVote and must generate other messages to advance the term. The net
      // result of these two features is to minimize the disruption caused by
      // nodes that have been removed from the cluster's configuration: a
      // removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
      // but it will not receive MsgApp or MsgHeartbeat, so it will not create
      // disruptive term increases, by notifying leader of this node's activeness.
      // The above comments also true for Pre-Vote
      //
      // When follower gets isolated, it soon starts an election ending
      // up with a higher term than leader, although it won't receive enough
      // votes to win the election. When it regains connectivity, this response
      // with "pb.MsgAppResp" of higher term would force leader to step down.
      // However, this disruption is inevitable to free this stuck node with
      // fresh election. This can be prevented with Pre-Vote phase.

      // 通知旧 Leader 当前节点已处于更高 Term，迫使其下台。
      // 当节点因网络分区提升 Term 后，旧 Leader 可能仍在发送心跳或日志请求。通过返回 MsgAppResp（携带当前节点的更高
      // Term），旧 Leader 会发现自己 Term 过低，触发 Step 函数中的 becomeFollower 逻辑，从而下台并更新 Term。
      raftpb::message resp;
      resp.set_to(m.from());
      resp.set_type(raftpb::message_type::MSG_APP_RESP);
      send(std::move(resp));
    } else if (m.type() == raftpb::message_type::MSG_PRE_VOTE) {
      // Before Pre-Vote enable, there may have candidate with higher term,
      // but less log. After update to Pre-Vote, the cluster may deadlock if
      // we drop messages with a lower term.
      // 若 Candidate 在低 Term 发起预投票，而当前节点已处于更高 Term，直接返回拒绝响应。Candidate 收到后会发现 Term
      // 落后，停止预投票流程，避免干扰集群。
      auto cand_last = raft_log_handle_.last_entry_id();
      LOG_INFO(logger_, "{} [logterm: {}, index: {}, vote: {}] rejected {} from {} [logterm: {}, index: {}] at term {}",
               id_, cand_last.term, cand_last.index, vote_id_, enum_name(m.type()), m.from(), m.term(), m.index(),
               term_);
      raftpb::message resp;
      resp.set_to(m.from());
      resp.set_term(term_);
      resp.set_type(raftpb::message_type::MSG_PRE_VOTE_RESP);
      resp.set_reject(true);
      send(std::move(resp));
    } else if (m.type() == raftpb::message_type::MSG_STORAGE_APPEND_RESP) {
      if (m.index() != 0) {
        // 忽略日志追加结果，因为低 Term 的日志可能已被更高 Term 的日志覆盖。记录警告日志。
        // Don't consider the appended log entries to be stable because
        // they may have been overwritten in the unstable log during a
        // later term. See the comment in newStorageAppendResp for more
        // about this race.
        LOG_INFO(logger_, "{} [term: {}] ignored entry appends from a {} message with lower term [term: {}]", id_,
                 term_, enum_name(m.type()), m.term());
      }
      if (m.has_snapshot()) {
        // Even if the snapshot applied under a different term, its
        // application is still valid. Snapshots carry committed
        // (term-independent) state.
        applied_snap(m.snapshot());
      }
    } else {
      // ignore other cases
      LOG_INFO(logger_, "{} [term: {}] ignored a {} message with lower term from {} [term: {}]", id_, term_,
               enum_name(m.type()), m.from(), m.term());
    }
    return {};
  }

  switch (m.type()) {
    case raftpb::message_type::MSG_HUP: {
      if (pre_vote_) {
        hup(campaign_type::PRE_ELECTION);
      } else {
        hup(campaign_type::ELECTION);
      }
      break;
    }
    case raftpb::message_type::MSG_STORAGE_APPEND_RESP: {
      if (m.index() != 0) {
        raft_log_handle_.stable_to({m.log_term(), m.index()});
      }
      if (m.has_snapshot()) {
        applied_snap(m.snapshot());
      }
      break;
    }
    case raftpb::message_type::MSG_STORAGE_APPLY_RESP: {
      if (m.entries_size() > 0) {
        auto index = m.entries().at(m.entries_size() - 1).index();
        applied_to(index, pb::ent_size(m.entries()));
        reduce_uncommitted_size(pb::payloads_size(m.entries()));
      }
      break;
    }
    case raftpb::message_type::MSG_VOTE:
    case raftpb::message_type::MSG_PRE_VOTE: {
      // We can vote if this is a repeat of a vote we've already cast...
      auto can_vote =
          vote_id_ == m.from() ||  // // 已投给该候选者
                                   // ...we haven't voted and we don't think there's a leader yet in this term...
          (vote_id_ == NONE && lead_ == NONE) ||  // 未投票且无 Leader
          // ...or this is a PreVote for a future term...
          // 消息类型是预投票且请求的消息的 Term 更高
          (m.type() == raftpb::message_type::MSG_PRE_VOTE && m.term() > term_);
      // ...and we believe the candidate is up to date.
      auto last = raft_log_handle_.last_entry_id();
      auto cand_last_id = pb::entry_id{.term = m.log_term(), .index = m.index()};
      if (can_vote && raft_log_handle_.is_up_to_date(cand_last_id)) {
        // Note: it turns out that that learners must be allowed to cast votes.
        // This seems counter- intuitive but is necessary in the situation in which
        // a learner has been promoted (i.e. is now a voter) but has not learned
        // about this yet.
        // For example, consider a group in which id=1 is a learner and id=2 and
        // id=3 are voters. A configuration change promoting 1 can be committed on
        // the quorum `{2,3}` without the config change being appended to the
        // learner's log. If the leader (say 2) fails, there are de facto two
        // voters remaining. Only 3 can win an election (due to its log containing
        // all committed entries), but to do so it will need 1 to vote. But 1
        // considers itself a learner and will continue to do so until 3 has
        // stepped up as leader, replicates the conf change to 1, and 1 applies it.
        // Ultimately, by receiving a request to vote, the learner realizes that
        // the candidate believes it to be a voter, and that it should act
        // accordingly. The candidate's config may be stale, too; but in that case
        // it won't win the election, at least in the absence of the bug discussed
        // in:
        // https://github.com/etcd-io/etcd/issues/7625#issuecomment-488798263.
        LOG_INFO(logger_, "{} [logterm: {}, index: {}, vote: {}] cast {} for {} [logterm: {}, index: {}] at term {}",
                 id_, last.term, last.index, vote_id_, enum_name(m.type()), m.from(), m.log_term(), m.index(), term_);
        // When responding to Msg{Pre,}Vote messages we include the term
        // from the message, not the local term. To see why, consider the
        // case where a single node was previously partitioned away and
        // it's local term is now out of date. If we include the local term
        // (recall that for pre-votes we don't update the local term), the
        // (pre-)campaigning node on the other end will proceed to ignore
        // the message (it ignores all out of date messages).
        // The term in the original message and current local term are the
        // same in the case of regular votes, but different for pre-votes.
        raftpb::message resp;
        resp.set_to(m.from());
        resp.set_term(m.term());
        resp.set_type(pb::vote_resp_msg_type(m.type()));
        send(std::move(resp));
        if (m.type() == raftpb::message_type::MSG_VOTE) {
          // Only record real votes.
          election_elapsed_ = 0;
          vote_id_ = m.from();
        }
      } else {
        LOG_INFO(logger_,
                 "{} [logterm: {}, index: {}, vote: {}] rejected {} from {} [logterm: {}, index: {}] at term {}", id_,
                 last.term, last.index, vote_id_, enum_name(m.type()), m.from(), m.log_term(), m.index(), term_);
        raftpb::message resp;
        resp.set_to(m.from());
        resp.set_term(term_);
        resp.set_type(pb::vote_resp_msg_type(m.type()));
        resp.set_reject(true);
        send(std::move(resp));
      }
      break;
    }
    default: {
      auto result = step_func_(*this, std::move(m));
      if (!result) {
        return result;
      }
    }
  }
  return {};
}

void raft::tick() {
  assert(tick_func_ != nullptr);
  (this->*tick_func_)();
}

void raft::handle_append_entries(raftpb::message&& message) {
  // TODO(pav-kv): construct logSlice up the stack next to receiving the
  // message, and validate it before taking any action (e.g. bumping term).
  pb::log_slice log_slice{message.term(), {message.log_term(), message.index()}, std::move(*message.mutable_entries())};

  raftpb::message resp_msg;
  resp_msg.set_to(message.from());
  resp_msg.set_type(raftpb::message_type::MSG_APP_RESP);
  // 若 Leader 的前一条日志索引 a.prev.index 小于本地已提交的索引 r.raftLog.committed
  if (log_slice.prev.index < raft_log_handle_.committed()) {
    resp_msg.set_index(raft_log_handle_.committed());
    send(std::move(resp_msg));
    return;
  }

  // 尝试本地追加日志
  if (auto m_last_index = raft_log_handle_.maybe_append(std::move(log_slice), message.commit());
      m_last_index) {  // 本地追加日志成功： 返回最新日志索引 mlastIndex，发送 MsgAppResp 确认。
    auto last_index = m_last_index.value();
    resp_msg.set_index(last_index);
    send(std::move(resp_msg));
    return;
  }

  // 本地追加日志失败：则发送拒绝消息
  LOG_DEBUG(logger_, "{} [logterm: {}, index: {}] rejected MsgApp [logterm: {}, index: {}] from {}", id_,
            raft_log_handle_.zero_term_on_err_compacted(message.index()), message.index(), message.log_term(),
            message.index(), message.from());

  // Our log does not match the leader's at index m.Index. Return a hint to the
  // leader - a guess on the maximal (index, term) at which the logs match. Do
  // this by searching through the follower's log for the maximum (index, term)
  // pair with a term <= the MsgApp's LogTerm and an index <= the MsgApp's
  // Index. This can help skip all indexes in the follower's uncommitted tail
  // with terms greater than the MsgApp's LogTerm.
  //
  // See the other caller for findConflictByTerm (in stepLeader) for a much more
  // detailed explanation of this mechanism.

  // NB: m.Index >= raftLog.committed by now (see the early return above), and
  // raftLog.lastIndex() >= raftLog.committed by invariant, so min of the two is
  // also >= raftLog.committed. Hence, the findConflictByTerm argument is within
  // the valid interval, which then will return a valid (index, term) pair with
  // a non-zero term (unless the log is empty). However, it is safe to send a zero
  // LogTerm in this response in any case, so we don't verify it here.
  auto result = raft_log_handle_.find_conflict_by_term(std::min(message.index(), raft_log_handle_.last_index()),
                                                       message.log_term());
  auto [hint_index, hint_term] = result;
  resp_msg.set_index(message.index());
  resp_msg.set_reject(true);
  resp_msg.set_reject_hint(hint_index);
  resp_msg.set_log_term(hint_term);
  send(std::move(resp_msg));
}

void raft::handle_heartbeat(raftpb::message&& message) {
  raft_log_handle_.commit_to(message.commit());
  raftpb::message resp_msg;
  resp_msg.set_to(message.from());
  resp_msg.set_type(raftpb::message_type::MSG_HEARTBEAT_RESP);
  resp_msg.set_allocated_context(message.release_context());
  send(std::move(resp_msg));
}

void raft::handle_snapshot(raftpb::message&& message) {
  // MsgSnap messages should always carry a non-nil Snapshot, but err on the
  // side of safety and treat a nil Snapshot as a zero-valued Snapshot.
  raftpb::snapshot snapshot;
  if (message.has_snapshot()) {
    snapshot = std::move(*message.mutable_snapshot());
  } else {
    LOG_WARN(logger_, "{} [commit: {}] unreachable case", id_, raft_log_handle_.committed());
  }
  auto sindex = snapshot.metadata().index();
  auto sterm = snapshot.metadata().term();

  raftpb::message resp_msg;
  resp_msg.set_to(message.from());
  resp_msg.set_type(raftpb::message_type::MSG_APP_RESP);
  if (restore(std::move(snapshot))) {
    LOG_INFO(logger_, "{} [commit: {}] restored snapshot [index: {}, term: {}]", id_, raft_log_handle_.committed(),
             sindex, sterm);
    resp_msg.set_index(raft_log_handle_.last_index());
  } else {
    LOG_INFO(logger_, "{} [commit: {}] ignored snapshot [index: {}, term: {}]", id_, raft_log_handle_.committed(),
             sindex, sterm);
    resp_msg.set_index(raft_log_handle_.committed());
  }
  send(std::move(resp_msg));
}

// 在Raft协议中，要求应用快照的节点必须处于Follower状态
// restore recovers the state machine from a snapshot. It restores the log and the
// configuration of state machine. If this method returns false, the snapshot was
// ignored, either because it was obsolete or because of an error.
bool raft::restore(raftpb::snapshot&& snapshot) {
  if (snapshot.metadata().index() <= raft_log_handle_.committed()) {
    return false;
  }
  // Raft协议的基本约束
  // Raft协议中，只有Follower或Candidate可以被动接受来自Leader的快照。
  // 当一个节点（即使是Leader）发现快照中的任期（Term）比自己的当前任期更高时，必须立即转为Follower。
  // 这种状态转换是协议强制要求的，目的是保证系统的任期单调递增原则和Leader唯一性。
  if (state_type_ != state_type::FOLLOWER) {
    // This is defense-in-depth: if the leader somehow ended up applying a
    // snapshot, it could move into a new term without moving into a
    // follower state. This should never fire, but if it did, we'd have
    // prevented damage by returning early, so log only a loud warning.
    //
    // At the time of writing, the instance is guaranteed to be in follower
    // state when this method is called.
    LOG_WARN(logger_, "{} attempted to restore snapshot as leader; should never happen", id_);
    become_follower(term_ + 1, NONE);
    return false;
  }

  // More defense-in-depth: throw away snapshot if recipient is not in the
  // config. This shouldn't ever happen (at the time of writing) but lots of
  // code here and there assumes that r.id is in the progress tracker.
  auto found = false;
  if (!snapshot.has_metadata() || !snapshot.metadata().has_conf_state()) {
    LOG_WARN(logger_, "{} attempted to restore snapshot with no ConfState; should never happen", id_);
    return false;
  }
  const auto& cs = snapshot.metadata().conf_state();
  auto found_id_func = [&](const pb::repeated_uint64& ids) {
    for (const auto& id : ids) {
      if (id == id_) {
        return true;
      }
    }
    return false;
  };
  if (found_id_func(cs.voters()) || found_id_func(cs.learners()) || found_id_func(cs.voters_outgoing())) {
    found = true;
  }
  if (!found) {
    LOG_WARN(logger_, "{} attempted to restore snapshot but it is not in the ConfState {}; should never happen", id_,
             cs.DebugString());
    return false;
  }

  // Now go ahead and actually restore.

  pb::entry_id entry_id{.term = snapshot.metadata().term(), .index = snapshot.metadata().index()};
  if (raft_log_handle_.match_term(entry_id)) {
    // TODO(pav-kv): can print %+v of the id, but it will change the format.
    auto last = raft_log_handle_.last_entry_id();
    LOG_INFO(logger_,
             "{} [commit: {}, lastindex: {}, lastterm: {}] fast-forwarded commit to snapshot [index: {}, term: {}]",
             id_, raft_log_handle_.committed(), last.index, last.term, entry_id.index, entry_id.term);
    raft_log_handle_.commit_to(snapshot.metadata().index());
    return false;
  }
  raft_log_handle_.restore(std::move(snapshot));

  // Reset the configuration and add the (potentially updated) peers in anew.
  this->trk_ = tracker::progress_tracker(this->trk_.max_inflight(), this->trk_.max_inflight_bytes());
  auto restore_result = leaf::try_handle_some(
      [&]() -> confchange::changer::result {
        BOOST_LEAF_AUTO(result,
                        confchange::restor(cs, confchange::changer{this->trk_.clone(), raft_log_handle_.last_index()}));
        return result;
      },
      [&](const lepton::lepton_error& err) -> confchange::changer::result {
        LEPTON_CRITICAL("unable to restore config {}: {}", cs.DebugString(), err.message);
        return new_error(err);
      });
  assert(restore_result);
  auto [cfg, trk] = std::move(*restore_result);
  pb::assert_conf_states_equivalent(cs, this->switch_to_config(std::move(cfg), std::move(trk)));
  auto last = raft_log_handle_.last_entry_id();
  LOG_INFO(logger_, "{} [commit: {}, lastindex: {}, lastterm: {}] restored snapshot [index: {}, term: {}]", id_,
           raft_log_handle_.committed(), last.index, last.term, entry_id.index, entry_id.term);
  return true;
}

bool raft::promotable() {
  auto pr_iter = trk_.progress_map_view().view().find(id_);
  if (pr_iter == trk_.progress_map_view().view().end()) {
    return false;
  }
  auto& pr = pr_iter->second;
  if (pr.is_learner()) {
    return false;
  }
  if (raft_log_handle_.has_next_or_in_progress_snapshot()) {
    return false;
  }
  return true;
}

raftpb::conf_state raft::apply_conf_change(raftpb::conf_change_v2&& cc) {
  auto change_func = [&]() -> confchange::changer::result {
    confchange::changer changer{trk_.clone(), raft_log_handle_.last_index()};
    if (pb::leave_joint(cc)) {
      return changer.leave_joint();
    } else if (auto [auto_leave, ok] = pb::enter_joint(cc); ok) {
      return changer.enter_joint(auto_leave, absl::MakeSpan(cc.changes()));
    }
    return changer.simple(absl::MakeSpan(cc.changes()));
  };
  auto change_result = leaf::try_handle_some(
      [&]() -> confchange::changer::result {
        BOOST_LEAF_AUTO(result, change_func());
        return result;
      },
      [&](const lepton::lepton_error& err) -> confchange::changer::result {
        // TODO(tbg): return the error to the caller.
        LEPTON_CRITICAL(err.message);
        return new_error(err);
      });
  ;
  assert(change_result);
  auto& [cfg, trk] = change_result.value();
  return switch_to_config(std::move(cfg), std::move(trk));
}

raftpb::conf_state raft::switch_to_config(tracker::config&& cfg, tracker::progress_map&& pgs_map) {
  trace_conf_change_event(cfg, *this);

  trk_.update_config(std::move(cfg));
  trk_.update_progress(std::move(pgs_map));

  LOG_INFO(logger_, "{} switched to configuration {}", id_, trk_.config_view().string());
  auto cs = trk_.conf_state();
  auto& progress_map = trk_.progress_map_view().view();
  auto iter_pr = progress_map.find(id_);
  auto exist = iter_pr != progress_map.end();

  // Update whether the node itself is a learner, resetting to false when the
  // node is removed.
  is_learner_ = exist && iter_pr->second.is_learner();

  // etcd-raft 的设计遵循
  // ​事件驱动模型，状态变更通常由外部消息或定时器触发，而非在配置变更函数中立即执行。例如：
  // 1. ​配置变更后的首次心跳：Leader
  // 发送心跳时会携带新配置，但其他节点可能已不再响应（因 Leader
  // 不在配置中）。
  // 2. ​后续消息处理：在 raft.Step
  // 方法中，若检测到自身角色与配置冲突，会调用

  // raft.becomeFollower 切换状态 当新配置移除了当前 Leader 节点或将其降级为
  // Learner 时，函数通过以下逻辑触发降级：
  if ((!exist || is_learner_) && state_type_ == state_type::LEADER) {
    // This node is leader and was removed or demoted, step down if requested.
    //
    // We prevent demotions at the time writing but hypothetically we handle
    // them the same way as removing the leader.
    //
    // TODO(tbg): ask follower with largest Match to TimeoutNow (to avoid
    // interruption). This might still drop some proposals but it's better than
    // nothing.
    if (step_down_on_removal_) {
      become_follower(term_, NONE);
    }
    return cs;
  }

  // The remaining steps only make sense if this node is the leader and there
  // are other nodes.
  if (state_type_ != state_type::LEADER || cs.voters_size() == 0) {
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
    trk_.visit([&](std::uint64_t id) {
      if (id_ == id) {
        return;
      }
      maybe_send_append(id, false /* sendIfEmpty */);
    });
  }

  // 若正在进行的 Leadership 转移目标（leadTransferee）被移除，则取消转移
  // If the the leadTransferee was removed or demoted, abort the leadership
  // transfer.
  if (auto exist = trk_.config_view().voters.id_set().contains(leader_transferee_); !exist && leader_transferee_ != 0) {
    abort_leader_transfer();
  }
  return cs;
}

void raft::load_state(const raftpb::hard_state& state) {
  if (state.commit() < raft_log_handle_.committed() || state.commit() > raft_log_handle_.last_index()) {
    LEPTON_CRITICAL("{} state.commit {} is out of range [{}, {}]", id_, state.commit(), raft_log_handle_.committed(),
                    raft_log_handle_.last_index());
  }
  raft_log_handle_.commit_to(state.commit());
  term_ = state.term();
  vote_id_ = state.vote();
}

bool raft::past_election_timeout() { return election_elapsed_ >= randomized_election_timeout_; }

// [electiontimeout, 2 * electiontimeout - 1]
void raft::reset_randomized_election_timeout() {
  static std::mt19937 rng(std::random_device{}());  // 静态生成器
  std::uniform_int_distribution<int> dist(0, election_timeout_ - 1);
  randomized_election_timeout_ = election_timeout_ + dist(rng);
}

void raft::send_timmeout_now(std::uint64_t id) {
  raftpb::message m;
  m.set_to(id);
  m.set_type(raftpb::message_type::MSG_TIMEOUT_NOW);
  send(std::move(m));
}

void raft::abort_leader_transfer() { leader_transferee_ = NONE; }

bool raft::committed_entry_in_current_term() const {
  // NB: r.Term is never 0 on a leader, so if zeroTermOnOutOfBounds returns 0,
  // we won't see it as a match with r.Term.
  return raft_log_handle_.zero_term_on_err_compacted(raft_log_handle_.committed()) == term_;
}

raftpb::message raft::response_to_read_index_req(raftpb::message&& req, std::uint64_t read_index) {
  if (req.from() == NONE || req.from() == id_) {
    read_states_.emplace_back(read_state{read_index, std::move(*req.mutable_entries(0)->mutable_data())});
    return {};
  }
  raftpb::message resp;
  resp.set_to(req.from());
  resp.set_type(raftpb::message_type::MSG_READ_INDEX_RESP);
  resp.set_index(read_index);
  req.mutable_entries()->Swap(resp.mutable_entries());
  return resp;
}

bool raft::increase_uncommitted_size(const pb::repeated_entry& entries) {
  auto size = pb::payloads_size(entries);
  if ((uncommitted_size_ > 0) && (size > 0) && (size + uncommitted_size_ > max_uncommitted_size_)) {
    // If the uncommitted tail of the Raft log is empty, allow any size
    // proposal. Otherwise, limit the size of the uncommitted tail of the
    // log and drop any proposal that would push the size over the limit.
    // Note the added requirement s>0 which is used to make sure that
    // appending single empty entries to the log always succeeds, used both
    // for replicating a new leader's initial empty entry, and for
    // auto-leaving joint configurations.
    return false;
  }
  uncommitted_size_ += size;
  return true;
}

void raft::reduce_uncommitted_size(pb::entry_encoding_size size) {
  if (uncommitted_size_ > size) {
    uncommitted_size_ -= size;
  } else {
    // uncommittedSize may underestimate the size of the uncommitted Raft
    // log tail but will never overestimate it. Saturate at 0 instead of
    // allowing overflow.
    uncommitted_size_ = 0;
  }
}

#ifdef LEPTON_TEST
lepton::pb::repeated_message raft::read_messages() {
  advance_messages_after_append();
  auto msgs = std::move(msgs_);
  msgs_.Clear();
  return msgs;
}

void raft::advance_messages_after_append() {
  while (true) {
    auto msgs = take_messages_after_append();
    if (msgs.empty()) {
      break;
    }
    discard(step_or_send(std::move(msgs)));
  }
}

lepton::pb::repeated_message raft::take_messages_after_append() {
  auto msgs = std::move(msgs_after_append_);
  msgs_after_append_.Clear();
  return msgs;
}

leaf::result<void> raft::step_or_send(pb::repeated_message&& m) {
  for (auto& msg : m) {
    if (msg.to() == id_) {
      auto result = step(std::move(msg));
      if (!result) {
        return result;
      }
    } else {
      msgs_.Add(std::move(msg));
    }
  }
  return {};
}
#endif
}  // namespace lepton