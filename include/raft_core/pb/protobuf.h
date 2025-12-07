#ifndef _LEPTON_PB_H_
#define _LEPTON_PB_H_
#include <raft.pb.h>

#include "entry_view.h"
#include "enum_name.h"
#include "log.h"
#include "types.h"
namespace lepton::core {

bool operator==(const raftpb::hard_state& lhs, const raftpb::hard_state& rhs);

namespace pb {

pb::repeated_entry convert_span_entry(pb::span_entry span_entries);
void convert_span_entry(pb::repeated_entry& entries, pb::span_entry span_entries);

entry_encoding_size ent_size(const repeated_entry& entries);

entry_encoding_size ent_size(const pb::span_entry& entries);

entry_encoding_size ent_size(const pb::entry_view& entries);

// payloadsSize is the size of the payloads of the provided entries.
entry_payload_size payloads_size(const raftpb::entry& entry);
entry_payload_size payloads_size(const repeated_entry& entries);

entry_id pb_entry_id(const raftpb::entry* const entry_ptr);

bool is_empty_snap(const raftpb::snapshot& snap);

repeated_entry extract_range_without_copy(repeated_entry& src, int start, int end);

pb::span_entry limit_entry_size(pb::span_entry entries, entry_encoding_size max_size);

pb::repeated_entry to_repeated_entry(const pb::entry_view& view);

repeated_entry limit_entry_size(repeated_entry& entries, entry_encoding_size max_size);

// extend appends vals to the given dst slice. It differs from the standard
// slice append only in the way it allocates memory. If cap(dst) is not enough
// for appending the values, precisely size len(dst)+len(vals) is allocated.
//
// Use this instead of standard append in situations when this is the last
// append to dst, so there is no sense in allocating more than needed.
repeated_entry extend(repeated_entry& dst, pb::span_entry vals);

void assert_conf_states_equivalent(const raftpb::conf_state& lhs, const raftpb::conf_state& rhs);

bool is_empty_hard_state(const raftpb::hard_state& hs);

// voteResponseType maps vote and prevote message types to their corresponding responses.
raftpb::message_type vote_resp_msg_type(raftpb::message_type type);

constexpr bool is_local_msg(raftpb::message_type type);

constexpr bool is_response_msg(raftpb::message_type type);

// 辅助函数：创建并显式初始化的数组
template <size_t N>
constexpr auto create_initialized_array() {
  std::array<bool, N> arr = {};
  for (auto& b : arr) {
    b = false;  // 显式初始化为false
  }
  return arr;
}

inline constexpr auto __is_local_msg = []() {
  auto arr = create_initialized_array<RAFTPB_MESSAGE_COUNT>();
  arr[raftpb::MSG_HUP] = true;
  arr[raftpb::MSG_BEAT] = true;
  arr[raftpb::MSG_UNREACHABLE] = true;
  arr[raftpb::MSG_SNAP_STATUS] = true;
  arr[raftpb::MSG_CHECK_QUORUM] = true;
  arr[raftpb::MSG_STORAGE_APPEND] = true;
  arr[raftpb::MSG_STORAGE_APPEND_RESP] = true;
  arr[raftpb::MSG_STORAGE_APPLY] = true;
  arr[raftpb::MSG_STORAGE_APPLY_RESP] = true;
  return arr;
}();

constexpr bool is_local_msg(raftpb::message_type type) {
  if (type < 0 || type >= RAFTPB_MESSAGE_COUNT) {
    LEPTON_CRITICAL("invalid message type: {}", enum_name(type));
  }
  return __is_local_msg[static_cast<std::size_t>(type)];
}

// 响应消息映射表 - 显式初始化
inline constexpr auto __is_response_msg = []() {
  auto arr = create_initialized_array<RAFTPB_MESSAGE_COUNT>();
  arr[raftpb::MSG_APP_RESP] = true;
  arr[raftpb::MSG_VOTE_RESP] = true;
  arr[raftpb::MSG_HEARTBEAT_RESP] = true;
  arr[raftpb::MSG_UNREACHABLE] = true;
  arr[raftpb::MSG_READ_INDEX_RESP] = true;
  arr[raftpb::MSG_PRE_VOTE_RESP] = true;
  arr[raftpb::MSG_STORAGE_APPEND_RESP] = true;
  arr[raftpb::MSG_STORAGE_APPLY_RESP] = true;
  return arr;
}();

constexpr bool is_response_msg(raftpb::message_type type) {
  if (type < 0 || type >= RAFTPB_MESSAGE_COUNT) {
    LEPTON_CRITICAL("invalid message type: {}", magic_enum::enum_name(type));
  }
  return __is_response_msg[static_cast<std::size_t>(type)];
}

inline constexpr bool is_local_msg_target(uint64_t id) { return id == LOCAL_APPEND_THREAD || id == LOCAL_APPLY_THREAD; }

}  // namespace pb

}  // namespace lepton::core

template <>
struct fmt::formatter<lepton::core::pb::repeated_uint64> {
  // 解析格式说明符（这里不需要特殊处理）
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();  // 忽略所有格式说明符
  }

  // 实际格式化函数
  template <typename FormatContext>
  auto format(const lepton::core::pb::repeated_uint64& field, FormatContext& ctx) const {
    auto out = ctx.out();
    *out++ = '[';

    bool first = true;
    for (auto& element : field) {
      if (!first) {
        out = fmt::format_to(out, ", ");
      } else {
        first = false;
      }
      out = fmt::format_to(out, "{}", element);
    }

    *out++ = ']';
    return out;
  }
};

#endif  // _LEPTON_PB_H_
