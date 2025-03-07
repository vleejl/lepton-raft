#ifndef _LEPTON_TEST_RAFT_PROTOBUF_H_
#define _LEPTON_TEST_RAFT_PROTOBUF_H_
#include "protobuf.h"
#include "raft.pb.h"

lepton::pb::entry_ptr create_entry(std::uint64_t index, std::uint64_t term);
lepton::pb::repeated_entry create_entries(
    const std::vector<std::tuple<uint64_t, uint64_t>> &entrie_params);

bool operator==(const raftpb::entry &lhs, const raftpb::entry &rhs);
bool operator==(const absl::Span<const raftpb::entry *const> &lhs,
                const absl::Span<const raftpb::entry *const> &rhs);
bool operator==(const lepton::pb::repeated_entry &lhs,
                const lepton::pb::repeated_entry &rhs);

raftpb::snapshot create_snapshot(std::uint64_t index, std::uint64_t term);
raftpb::snapshot create_snapshot(std::uint64_t index, std::uint64_t term,
                                 const std::string &data,
                                 std::optional<raftpb::conf_state> state);
#endif  // _LEPTON_TEST_RAFT_PROTOBUF_H_
