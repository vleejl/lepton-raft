#include "test_raft_protobuf.h"

#include <cstdio>
#include <ostream>
#include <string>
#include <vector>

#include "raft.pb.h"

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

lepton::pb::repeated_entry create_entries_with_term_range(std::uint64_t index, std::uint64_t term_from, std::uint64_t term_to) {
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

bool operator==(const lepton::pb::repeated_entry &lhs, const lepton::pb::repeated_entry &rhs) {
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

raftpb::snapshot create_snapshot(std::uint64_t index, std::uint64_t term) {
  raftpb::snapshot snapshot;
  snapshot.mutable_metadata()->set_index(index);
  snapshot.mutable_metadata()->set_term(term);
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