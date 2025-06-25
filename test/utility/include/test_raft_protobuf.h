#ifndef _LEPTON_TEST_RAFT_PROTOBUF_H_
#define _LEPTON_TEST_RAFT_PROTOBUF_H_
#include <cstdint>
#include <vector>

#include "raft.pb.h"
#include "types.h"

namespace test_pb {

struct entry {
  std::uint64_t index = 0;
  std::uint64_t term = 0;
  std::string data;
};
struct message {
  raftpb::message_type msg_type;
  std::uint64_t from = 0;
  std::uint64_t to = 0;
  std::uint64_t term = 0;
  std::uint64_t log_term = 0;
  std::uint64_t index = 0;
  std::uint64_t commit = 0;
  std::vector<entry> entries;
  std::string ctx;
};
}  // namespace test_pb

raftpb::conf_change create_conf_change(std::uint64_t node_id, raftpb::conf_change_type type);
raftpb::message convert_test_pb_message(test_pb::message &&);
lepton::pb::entry_ptr create_entry(std::uint64_t index, std::uint64_t term);
lepton::pb::repeated_entry create_entries(std::uint64_t index, std::vector<std::uint64_t> terms);
lepton::pb::repeated_entry create_entries(const std::vector<std::tuple<uint64_t, uint64_t>> &entrie_params);
lepton::pb::repeated_entry create_entries_with_term_range(std::uint64_t index, std::uint64_t term_from,
                                                          std::uint64_t term_to);

bool operator==(const raftpb::entry &lhs, const raftpb::entry &rhs);
bool operator==(const raftpb::entry &lhs, const raftpb::entry *const rhs);
bool operator==(const raftpb::entry *const lhs, const raftpb::entry &rhs);
bool operator==(const lepton::pb::repeated_entry &lhs, const lepton::pb::span_entry &rhs);
bool operator==(const lepton::pb::span_entry &lhs, const lepton::pb::repeated_entry &rhs);
bool compare_repeated_entry(const lepton::pb::span_entry &lhs, const lepton::pb::span_entry &rhs);
bool operator==(const lepton::pb::span_entry &lhs, const lepton::pb::span_entry &rhs);
bool compare_repeated_entry(const lepton::pb::repeated_entry &lhs, const lepton::pb::repeated_entry &rhs);
bool operator==(const lepton::pb::repeated_entry &lhs, const lepton::pb::repeated_entry &rhs);

raftpb::snapshot create_snapshot(std::uint64_t index, std::uint64_t term);
raftpb::snapshot create_snapshot(std::uint64_t index, std::uint64_t term, std::vector<std::uint64_t> &&voters);
raftpb::snapshot create_snapshot(std::uint64_t index, std::uint64_t term, const std::string &data,
                                 std::optional<raftpb::conf_state> state);
#endif  // _LEPTON_TEST_RAFT_PROTOBUF_H_
