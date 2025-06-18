#include "test_raft_protobuf.h"

#include <cstdio>
#include <ostream>
#include <string>
#include <vector>

#include "raft.pb.h"

raftpb::conf_change create_conf_change(std::uint64_t node_id, raftpb::conf_change_type type) {
  raftpb::conf_change cc;
  cc.set_node_id(node_id);
  cc.set_type(type);
  return cc;
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
  msg.set_context(m.ctx);
  return msg;
}

lepton::pb::entry_ptr create_entry(std::uint64_t index, std::uint64_t term) {
  auto entry = std::make_unique<raftpb::entry>();
  entry->set_index(index);
  entry->set_term(term);
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

bool operator==(const absl::Span<const raftpb::entry *const> &lhs, const absl::Span<const raftpb::entry *const> &rhs) {
  const auto lhs_size = lhs.size();
  const auto rhs_size = rhs.size();
  if (lhs_size != rhs_size) {
    return false;
  }
  for (int i = 0; i < lhs_size; ++i) {
    if (*lhs[i] != *rhs[i]) {
      return false;
    }
  }
  return true;
}

bool compare_repeated_entry(const lepton::pb::repeated_entry &lhs, const lepton::pb::repeated_entry &rhs) {
  const auto lhs_size = lhs.size();
  const auto rhs_size = rhs.size();
  if (lhs_size != rhs_size) {
    return false;
  }
  for (int i = 0; i < lhs_size; ++i) {
    if (lhs[i] != rhs[i]) {
      return false;
    }
  }
  return true;
}

bool operator==(const lepton::pb::repeated_entry &lhs, const lepton::pb::repeated_entry &rhs) {
  return compare_repeated_entry(lhs, rhs);
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