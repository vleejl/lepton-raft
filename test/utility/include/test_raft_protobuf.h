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
raftpb::entry create_entry(std::uint64_t index, std::uint64_t term, std::string &&data);
lepton::pb::repeated_entry create_entries(std::uint64_t index, std::vector<std::uint64_t> terms);
lepton::pb::repeated_entry create_entries(const std::vector<std::tuple<uint64_t, uint64_t>> &entrie_params);
lepton::pb::repeated_entry create_entries_with_term_range(std::uint64_t index, std::uint64_t term_from,
                                                          std::uint64_t term_to);
lepton::pb::repeated_entry create_entries_with_entry_vec(std::vector<raftpb::entry> &&entries);

bool operator==(const raftpb::entry &lhs, const raftpb::entry &rhs);
bool operator==(const raftpb::entry &lhs, const raftpb::entry *const rhs);
bool operator==(const raftpb::entry *const lhs, const raftpb::entry &rhs);
bool operator==(const lepton::pb::repeated_entry &lhs, const lepton::pb::span_entry &rhs);
bool operator==(const lepton::pb::span_entry &lhs, const lepton::pb::repeated_entry &rhs);
bool compare_repeated_entry(const lepton::pb::span_entry &lhs, const lepton::pb::span_entry &rhs);
bool operator==(const lepton::pb::span_entry &lhs, const lepton::pb::span_entry &rhs);
bool compare_repeated_entry(const lepton::pb::repeated_entry &lhs, const lepton::pb::repeated_entry &rhs);
bool operator==(const lepton::pb::repeated_entry &lhs, const lepton::pb::repeated_entry &rhs);
bool compare_repeated_message(const lepton::pb::repeated_message &lhs, const lepton::pb::repeated_message &rhs);
bool operator==(const lepton::pb::repeated_message &lhs, const lepton::pb::repeated_message &rhs);

raftpb::snapshot create_snapshot(std::uint64_t index, std::uint64_t term);
raftpb::snapshot create_snapshot(std::uint64_t index, std::uint64_t term, std::vector<std::uint64_t> &&voters);
raftpb::snapshot create_snapshot(std::uint64_t index, std::uint64_t term, const std::string &data,
                                 std::optional<raftpb::conf_state> state);

inline constexpr std::array<raftpb::message_type, 24> all_raftpb_message_types = {
    raftpb::MSG_HUP,
    raftpb::MSG_BEAT,
    raftpb::MSG_PROP,
    raftpb::MSG_APP,
    raftpb::MSG_APP_RESP,
    raftpb::MSG_VOTE,
    raftpb::MSG_VOTE_RESP,
    raftpb::MSG_SNAP,
    raftpb::MSG_HEARTBEAT,
    raftpb::MSG_HEARTBEAT_RESP,
    raftpb::MSG_UNREACHABLE,
    raftpb::MSG_SNAP_STATUS,
    raftpb::MSG_CHECK_QUORUM,
    raftpb::MSG_TRANSFER_LEADER,
    raftpb::MSG_TIMEOUT_NOW,
    raftpb::MSG_READ_INDEX,
    raftpb::MSG_READ_INDEX_RESP,
    raftpb::MSG_PRE_VOTE,
    raftpb::MSG_PRE_VOTE_RESP,
    raftpb::MSG_STORAGE_APPEND,
    raftpb::MSG_STORAGE_APPEND_RESP,
    raftpb::MSG_STORAGE_APPLY,
    raftpb::MSG_STORAGE_APPLY_RESP,
    raftpb::MSG_FORGET_LEADER,
};

// 确保数组大小等于枚举值的数量
static_assert(all_raftpb_message_types.size() == 24, "AllMessageTypes size mismatch");

// 确保数组索引与枚举值匹配
static_assert(all_raftpb_message_types[0] == raftpb::MSG_HUP, "Index 0 mismatch");
static_assert(all_raftpb_message_types[23] == raftpb::MSG_FORGET_LEADER, "Index 23 mismatch");
#endif  // _LEPTON_TEST_RAFT_PROTOBUF_H_
