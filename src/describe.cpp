#include "describe.h"

#include <absl/strings/str_join.h>
#include <fmt/format.h>

#include "conf_change.h"
#include "config.h"
#include "log.h"
#include "magic_enum.hpp"
#include "protobuf.h"
#include "raft.pb.h"
#include "types.h"

namespace lepton {

static std::string format_vector(const std::vector<read_state> &vec) {
  return fmt::format(
      "[{}]", absl::StrJoin(vec | std::views::transform([](const read_state &rs) { return rs.to_string(); }), ", "));
}

std::string describe_soft_state(const soft_state &ss) {
  return fmt::format("Lead:{} State:{}", ss.leader_id, magic_enum::enum_name(ss.raft_state));
}

std::string describe_hard_state(const raftpb::hard_state &hs) {
  return hs.has_vote() ? fmt::format("Term:{} Vote:{} Commit:{}", hs.term(), hs.vote(), hs.commit())
                       : fmt::format("Term:{} Commit:{}", hs.term(), hs.commit());
}

static std::string formattted_describe_entry(const raftpb::entry &ent) {
  switch (ent.type()) {
    case raftpb::ENTRY_NORMAL:
      return ent.data();
    case raftpb::ENTRY_CONF_CHANGE: {
      raftpb::conf_change ccc;
      ccc.ParseFromString(ent.data());
      auto cc_v2 = pb::conf_change_as_v2(std::move(ccc));
      return pb::conf_changes_to_string(cc_v2.changes());
    }
    case raftpb::ENTRY_CONF_CHANGE_V2: {
      raftpb::conf_change_v2 cc_v2;
      cc_v2.ParseFromString(ent.data());
      return pb::conf_changes_to_string(cc_v2.changes());
    }
    default:
      LEPTON_CRITICAL("Unknown entry type: {}", magic_enum::enum_name(ent.type()));
  }
  LEPTON_CRITICAL("Unknown entry type: {}", magic_enum::enum_name(ent.type()));
  return fmt::format("Unknown entry type: {}", magic_enum::enum_name(ent.type()));
}

std::string describe_entry(const raftpb::entry &ent) {
  auto formatted = formattted_describe_entry(ent);
  if (!formatted.empty()) {
    formatted = fmt::format(" {}", formatted);
  }
  return fmt::format("{}/{} {}{}", ent.term(), ent.index(), magic_enum::enum_name(ent.type()), formatted);
}

// DescribeEntries calls DescribeEntry for each Entry, adding a newline to
// each.
std::string describe_entries(const pb::repeated_entry &entries) {
  return fmt::format(
      "{}\n", fmt::join(entries | std::views::transform([](const auto &ent) { return describe_entry(ent); }), "\n"));
}

std::string describe_conf_state(const raftpb::conf_state &state) {
  return fmt::format("Voters:{} VotersOutgoing:{} Learners:{} LearnersNext:{} AutoLeave:{}", state.voters(),
                     state.voters_outgoing(), state.learners(), state.learners_next(), state.auto_leave());
}

std::string describe_snapshot(const raftpb::snapshot &snap) {
  const auto &m = snap.metadata();
  return fmt::format("Index:{} Term:{} ConfState:{}", m.index(), m.term(), describe_conf_state(m.conf_state()));
}

std::string describe_target(uint64_t id) {
  switch (id) {
    case NONE:
      return "None";
    case pb::LOCAL_APPEND_THREAD:
      return "AppendThread";
    case pb::LOCAL_APPLY_THREAD:
      return "ApplyThread";
    default:
      return fmt::format("{:x}", id);
  }
}

std::string describe_message_with_indent(const std::string &indent, const raftpb::message &m) {
  // 主消息行
  std::string result =
      fmt::format("{}{}->{} {} Term:{} Log:{}/{}", indent, describe_target(m.from()), describe_target(m.to()),
                  magic_enum::enum_name(m.type()), m.term(), m.log_term(), m.index());

  // 附加字段
  if (m.reject()) {
    fmt::format_to(std::back_inserter(result), " Rejected (Hint: {})", m.reject_hint());
  }
  if (m.commit() != 0) {
    fmt::format_to(std::back_inserter(result), " Commit:{}", m.commit());
  }
  if (m.vote() != 0) {
    fmt::format_to(std::back_inserter(result), " Vote:{}", m.vote());
  }

  // 处理日志条目
  if (!m.entries().empty()) {
    if (m.entries_size() == 1) {
      fmt::format_to(std::back_inserter(result), " Entries:[{}]", describe_entry(m.entries(0)));
    } else {
      // 多行格式
      result += fmt::format(" Entries:[\n{}  ", indent);
      for (int i = 0; i < m.entries_size(); ++i) {
        if (i > 0) fmt::format_to(std::back_inserter(result), "\n{}  ", indent);
        result += describe_entry(m.entries(i));
      }
      fmt::format_to(std::back_inserter(result), "\n{}]", indent);
    }
  }

  // 处理快照
  if (m.has_snapshot() && !pb::is_empty_snap(m.snapshot())) {
    fmt::format_to(std::back_inserter(result), "\n{}  Snapshot: {}", indent, describe_snapshot(m.snapshot()));
  }

  // 递归处理响应
  if (!m.responses().empty()) {
    result += fmt::format(" Responses:[\n");
    for (const auto &response : m.responses()) {
      result += describe_message_with_indent(indent + "  ", response) + "\n";
    }
    result += fmt::format("{}]", indent);
  }

  return result;
}

// DescribeMessage returns a concise human-readable description of a
// Message for debugging.
std::string describe_message(const raftpb::message &m) { return describe_message_with_indent("", m); }

std::string describe_ready(const ready &rd) {
  fmt::memory_buffer buf;

  // Handle soft_state
  if (rd.soft_state) {
    auto soft_state_str = describe_soft_state(*rd.soft_state);
    fmt::format_to(std::back_inserter(buf), "{}\n", soft_state_str);
  }

  // Handle hard_state
  if (!pb::is_empty_hard_state(rd.hard_state)) {
    auto hard_state_str = describe_hard_state(rd.hard_state);
    fmt::format_to(std::back_inserter(buf), "HardState {}\n", hard_state_str);
  }

  // Handle read_states
  if (!rd.read_states.empty()) {
    auto read_states_str = format_vector(rd.read_states);
    fmt::format_to(std::back_inserter(buf), "ReadStates: {}\n", read_states_str);
  }

  // Handle entries
  if (!rd.entries.empty()) {
    auto entries_str = describe_entries(rd.entries);
    fmt::format_to(std::back_inserter(buf), "Entries:\n{}", entries_str);
  }

  // Handle snapshot
  if (!pb::is_empty_snap(rd.snapshot)) {
    auto snapshot_str = describe_snapshot(rd.snapshot);
    fmt::format_to(std::back_inserter(buf), "Snapshot: {{}}{}\n", snapshot_str);
  }

  // Handle committed_entries
  if (!rd.committed_entries.empty()) {
    auto committed_entries_str = describe_entries(rd.committed_entries);
    fmt::format_to(std::back_inserter(buf), "CommittedEntries:\n{}", committed_entries_str);
  }

  // Handle messages
  if (!rd.messages.empty()) {
    fmt::format_to(std::back_inserter(buf), "Messages:\n");
    for (const auto &msg : rd.messages) {
      auto msg_str = describe_message(msg);
      fmt::format_to(std::back_inserter(buf), "{}\n", msg_str);
    }
  }

  // Final assembly
  if (buf.size() > 0) {
    return fmt::format("Ready MustSync={}:\n{}", rd.must_sync, std::string_view(buf.data(), buf.size()));
  }
  return "<empty Ready>";
}

}  // namespace lepton
