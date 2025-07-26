#include "test_raft_protobuf.h"

#include <spdlog/spdlog.h>

#include <cassert>
#include <cstdio>
#include <ostream>
#include <string>
#include <vector>

#include "absl/types/span.h"
#include "conf_change.h"
#include "raft.pb.h"

raftpb::conf_change create_conf_change_v1(std::uint64_t node_id, raftpb::conf_change_type type) {
  raftpb::conf_change cc;
  cc.set_node_id(node_id);
  cc.set_type(type);
  return cc;
}

raftpb::conf_change_v2 create_conf_change_v2(std::uint64_t node_id, raftpb::conf_change_type type) {
  raftpb::conf_change_v2 cc;
  auto change = cc.add_changes();
  change->set_node_id(node_id);
  change->set_type(type);
  return cc;
}

raftpb::conf_change_v2 create_conf_change_v2(std::uint64_t node_id, raftpb::conf_change_type type,
                                             raftpb::conf_change_transition transition) {
  raftpb::conf_change_v2 cc;
  auto change = cc.add_changes();
  change->set_node_id(node_id);
  change->set_type(type);
  cc.set_transition(transition);
  return cc;
}

raftpb::conf_change_v2 create_conf_change_v2(std::vector<conf_change_v2_change> &&changes) {
  raftpb::conf_change_v2 cc;
  for (const auto &change : changes) {
    auto cc_change = cc.add_changes();
    cc_change->set_node_id(change.node_id);
    cc_change->set_type(change.type);
  }
  return cc;
}

raftpb::conf_change_v2 create_conf_change_v2(std::vector<conf_change_v2_change> &&changes,
                                             raftpb::conf_change_transition transition) {
  raftpb::conf_change_v2 cc;
  for (const auto &change : changes) {
    auto cc_change = cc.add_changes();
    cc_change->set_node_id(change.node_id);
    cc_change->set_type(change.type);
  }
  cc.set_transition(transition);
  return cc;
}

raftpb::conf_state create_conf_state(std::vector<std::uint64_t> &&voters, std::vector<std::uint64_t> &&voters_outgoing,
                                     std::vector<std::uint64_t> &&learners, std::vector<std::uint64_t> &&learners_next,
                                     bool auto_leave) {
  raftpb::conf_state cs;
  for (const auto &voter : voters) {
    cs.add_voters(voter);
  }
  for (const auto &voter_outgoing : voters_outgoing) {
    cs.add_voters_outgoing(voter_outgoing);
  }
  for (const auto &learner : learners) {
    cs.add_learners(learner);
  }
  for (const auto &learner_next : learners_next) {
    cs.add_learners_next(learner_next);
  }
  if (auto_leave) {
    cs.set_auto_leave(true);
  }
  return cs;
}

lepton::leaf::result<raftpb::conf_change> test_conf_change_var_as_v1(const lepton::pb::conf_change_var &cc) {
  lepton::pb::conf_change_var copy_cc = cc;
  return lepton::pb::conf_change_var_as_v1(std::move(copy_cc));
}

raftpb::conf_change_v2 test_conf_change_var_as_v2(const lepton::pb::conf_change_var &cc) {
  lepton::pb::conf_change_var copy_cc = cc;
  return lepton::pb::conf_change_var_as_v2(std::move(copy_cc));
}

raftpb::message convert_test_pb_message(test_pb::message &&m) {
  raftpb::message msg;
  msg.set_type(m.msg_type);
  if (m.from != 0) {
    msg.set_from(m.from);
  }
  if (m.to != 0) {
    msg.set_to(m.to);
  }
  if (m.term != 0) {
    msg.set_term(m.term);
  }
  if (m.term != 0) {
    msg.set_term(m.term);
  }
  if (m.log_term != 0) {
    msg.set_log_term(m.log_term);
  }
  if (m.index != 0) {
    msg.set_index(m.index);
  }
  if (m.commit != 0) {
    msg.set_commit(m.commit);
  }
  for (const auto &iter_entry : m.entries) {
    auto entry = msg.add_entries();
    if (iter_entry.term != 0) {
      entry->set_term(iter_entry.term);
    }
    if (iter_entry.index != 0) {
      entry->set_index(iter_entry.index);
    }
    entry->set_data(iter_entry.data);
  }
  if (!m.ctx.empty()) {
    msg.set_context(m.ctx);
  }
  return msg;
}

lepton::pb::entry_ptr create_entry(std::uint64_t index, std::uint64_t term) {
  auto entry = std::make_unique<raftpb::entry>();
  entry->set_index(index);
  entry->set_term(term);
  return entry;
}

raftpb::entry create_entry(std::uint64_t index, std::uint64_t term, std::string &&data) {
  raftpb::entry entry;
  entry.set_index(index);
  entry.set_term(term);
  entry.set_data(data);
  return entry;
}

lepton::pb::repeated_entry create_entries(std::uint64_t index, std::vector<std::uint64_t> terms) {
  lepton::pb::repeated_entry entries;
  for (const auto &term : terms) {
    auto entry = entries.Add();
    entry->set_index(index);
    entry->set_term(term);
    ++index;
  }
  return entries;
}

lepton::pb::repeated_entry create_entries_with_term_range(std::uint64_t index, std::uint64_t term_from,
                                                          std::uint64_t term_to) {
  lepton::pb::repeated_entry entries;
  for (auto term = term_from; term < term_to; ++term) {
    auto entry = entries.Add();
    entry->set_index(index);
    entry->set_term(term);
    ++index;
  }
  return entries;
}

lepton::pb::repeated_entry create_entries(const std::vector<std::tuple<uint64_t, uint64_t>> &entrie_params) {
  lepton::pb::repeated_entry entries;
  for (const auto &[index, term] : entrie_params) {
    auto entry = entries.Add();
    entry->set_index(index);
    entry->set_term(term);
  }
  return entries;
}

lepton::pb::repeated_entry create_entries_with_entry_vec(std::vector<raftpb::entry> &&entries) {
  lepton::pb::repeated_entry resp_entries;
  for (auto &entry : entries) {
    resp_entries.Add(std::move(entry));
  }
  return resp_entries;
}

bool compare_read_states(const std::vector<lepton::read_state> &lhs, const std::vector<lepton::read_state> &rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }

  for (size_t i = 0; i < lhs.size(); ++i) {
    const auto &left = lhs[i];
    const auto &right = rhs[i];

    // 比较索引
    if (left.index != right.index) {
      return false;
    }

    // 比较请求上下文
    if (left.request_ctx != right.request_ctx) {
      return false;
    }
  }

  return true;
}

bool operator==(const std::optional<raftpb::conf_state> &lhs, const std::optional<raftpb::conf_state> &rhs) {
  return compare_optional_conf_state(lhs, rhs);
}

bool compare_optional_conf_state(const std::optional<raftpb::conf_state> &lhs,
                                 const std::optional<raftpb::conf_state> &rhs) {
  if (lhs.has_value()) {
    if (rhs.has_value()) {
      if (lhs->DebugString() != rhs->DebugString()) {
        SPDLOG_ERROR("lhs:{}, rhs:{}", lhs->DebugString().c_str(), rhs->DebugString().c_str());
        return false;
      }
      return true;
    }
    return false;
  } else {
    if (rhs.has_value()) {
      return false;
    }
    return true;  // both are empty
  }
  return false;
}

bool operator==(const raftpb::entry &lhs, const raftpb::entry &rhs) {
  std::cout << lhs.DebugString() << std::endl;
  std::cout << rhs.DebugString() << std::endl;
  if (lhs.term() != rhs.term()) {
    printf("lhs term:%lu, rhs term:%lu\n", lhs.term(), rhs.term());
    return false;
  }
  if (lhs.index() != rhs.index()) {
    return false;
  }
  if (lhs.type() != rhs.type()) {
    return false;
  }
  if (lhs.data() != rhs.data()) {
    return false;
  }
  return true;
}

bool operator==(const raftpb::entry &lhs, const raftpb::entry *const rhs) { return operator==(lhs, *rhs); }

bool operator==(const raftpb::entry *const lhs, const raftpb::entry &rhs) { return operator==(*lhs, rhs); }

bool operator==(const lepton::pb::repeated_entry &lhs, const lepton::pb::span_entry &rhs) {
  return compare_repeated_entry(absl::MakeSpan(lhs), rhs);
}

bool operator==(const lepton::pb::span_entry &lhs, const lepton::pb::repeated_entry &rhs) {
  return compare_repeated_entry(lhs, absl::MakeSpan(rhs));
}

bool compare_repeated_entry(const lepton::pb::span_entry &lhs, const lepton::pb::span_entry &rhs) {
  const auto lhs_size = lhs.size();
  const auto rhs_size = rhs.size();
  if (lhs_size != rhs_size) {
    for (const auto &entry : rhs) {
      SPDLOG_INFO(entry->DebugString());
    }
    return false;
  }
  for (int i = 0; i < lhs_size; ++i) {
    if (*lhs[i] != *rhs[i]) {
      SPDLOG_INFO("lhs index: {}, msg: {}", i, lhs[i]->DebugString());
      SPDLOG_INFO("rhs index: {}, msg: {}", i, rhs[i]->DebugString());
      return false;
    }
  }
  return true;
}

bool operator==(const lepton::pb::span_entry &lhs, const lepton::pb::span_entry &rhs) {
  return compare_repeated_entry(lhs, rhs);
}

bool compare_repeated_entry(const lepton::pb::repeated_entry &lhs, const lepton::pb::repeated_entry &rhs) {
  const auto lhs_size = lhs.size();
  const auto rhs_size = rhs.size();
  if (lhs_size != rhs_size) {
    for (const auto &entry : rhs) {
      SPDLOG_INFO(entry.DebugString());
    }
    return false;
  }
  for (int i = 0; i < lhs_size; ++i) {
    if (lhs[i] != rhs[i]) {
      SPDLOG_INFO("lhs index: {}, msg: {}", i, lhs[i].DebugString());
      SPDLOG_INFO("rhs index: {}, msg: {}", i, rhs[i].DebugString());
      return false;
    }
  }
  return true;
}

bool operator==(const lepton::pb::repeated_entry &lhs, const lepton::pb::repeated_entry &rhs) {
  return compare_repeated_entry(lhs, rhs);
}

bool compare_repeated_message(const lepton::pb::repeated_message &lhs, const lepton::pb::repeated_message &rhs) {
  const auto lhs_size = lhs.size();
  const auto rhs_size = rhs.size();
  if (lhs_size != rhs_size) {
    assert(lhs_size == rhs_size);
    return false;
  }
  for (int i = 0; i < lhs_size; ++i) {
    if (lhs[i].DebugString() != rhs[i].DebugString()) {
      SPDLOG_INFO("lhs index: {}, msg: {}", i, lhs[i].DebugString());
      SPDLOG_INFO("rhs index: {}, msg: {}", i, rhs[i].DebugString());
      return false;
    }
  }
  return true;
}

bool operator==(const lepton::pb::repeated_message &lhs, const lepton::pb::repeated_message &rhs) {
  return compare_repeated_message(lhs, rhs);
}

bool compare_ready(const lepton::ready &lhs, const lepton::ready &rhs) {
  // 1. 比较 soft_state（可选值）
  if (lhs.soft_state.has_value() != rhs.soft_state.has_value()) {
    return false;
  }
  if (lhs.soft_state && rhs.soft_state) {
    // 假设 lepton::soft_state 有 operator==
    if (*lhs.soft_state != *rhs.soft_state) {
      return false;
    }
  }

  // 2. 比较 hard_state（直接比较）
  if (lhs.hard_state.DebugString() != rhs.hard_state.DebugString()) {
    return false;
  }

  // 3. 比较 read_states（使用专用比较函数）
  if (!compare_read_states(lhs.read_states, rhs.read_states)) {
    return false;
  }

  // 4. 比较 entries（使用 span_entry 比较）
  if (!compare_repeated_entry(lhs.entries, rhs.entries)) {
    return false;
  }

  // 5. 比较 snapshot
  if (lhs.snapshot.DebugString() != rhs.snapshot.DebugString()) {
    return false;
  }

  // 6. 比较 committed_entries
  if (!compare_repeated_entry(lhs.committed_entries, rhs.committed_entries)) {
    return false;
  }

  // 7. 比较 messages
  if (!compare_repeated_message(lhs.messages, rhs.messages)) {
    return false;
  }

  // 8. 比较 must_sync
  if (lhs.must_sync != rhs.must_sync) {
    return false;
  }

  return true;
}

raftpb::snapshot create_snapshot(std::uint64_t index, std::uint64_t term) {
  raftpb::snapshot snapshot;
  snapshot.mutable_metadata()->set_index(index);
  snapshot.mutable_metadata()->set_term(term);
  return snapshot;
}

raftpb::snapshot create_snapshot(std::uint64_t index, std::uint64_t term, std::vector<std::uint64_t> &&voters) {
  raftpb::snapshot snapshot = create_snapshot(index, term);
  for (auto voter : voters) {
    snapshot.mutable_metadata()->mutable_conf_state()->add_voters(voter);
  }
  return snapshot;
}

raftpb::snapshot create_snapshot(std::uint64_t index, std::uint64_t term, const std::string &data,
                                 std::optional<raftpb::conf_state> state) {
  raftpb::snapshot snapshot;
  snapshot.set_data(data);
  snapshot.mutable_metadata()->set_index(index);
  snapshot.mutable_metadata()->set_term(term);
  if (state) {
    snapshot.mutable_metadata()->mutable_conf_state()->CopyFrom(*state);
  }
  return snapshot;
}