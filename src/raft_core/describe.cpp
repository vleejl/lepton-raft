#include "raft_core/describe.h"

#include <absl/strings/str_join.h>
#include <fmt/format.h>

#include <string>

#include "basic/enum_name.h"
#include "basic/log.h"
#include "raft.pb.h"
#include "raft_core/config.h"
#include "raft_core/pb/conf_change.h"
#include "raft_core/pb/protobuf.h"
#include "raft_core/pb/types.h"

namespace lepton::core {
#ifdef LEPTON_TEST
static auto rafa_state_name(state_type type) {
  switch (type) {
    case state_type::FOLLOWER:
      return "StateFollower";
    case state_type::CANDIDATE:
      return "StateCandidate";
    case state_type::LEADER:
      return "StateLeader";
    case state_type::PRE_CANDIDATE:
      return "StatePreCandidate";
  }
}
#endif

static std::string format_vector(const std::vector<read_state> &vec) {
  return fmt::format(
      "[{}]", absl::StrJoin(vec | std::views::transform([](const read_state &rs) { return rs.to_string(); }), ", "));
}

std::string describe_soft_state(const soft_state &ss) {
#ifdef LEPTON_TEST
  return fmt::format("Lead:{} State:{}", ss.leader_id, rafa_state_name(ss.raft_state));
#else
  return fmt::format("Lead:{} State:{}", ss.leader_id, enum_name(ss.raft_state));
#endif
}

std::string describe_hard_state(const raftpb::hard_state &hs) {
  return hs.has_vote() ? fmt::format("Term:{} Vote:{} Commit:{}", hs.term(), hs.vote(), hs.commit())
                       : fmt::format("Term:{} Commit:{}", hs.term(), hs.commit());
}

std::string safe_quote(const std::string &data) {
  std::ostringstream oss;
  oss << std::quoted(data);  // 使用 std::quoted 自动转义
  return oss.str();
}

static std::string formattted_describe_entry(const raftpb::entry &ent, entry_formatter_func f) {
  if (f == nullptr) {
    f = [](const std::string &data) -> std::string { return safe_quote(data); };
  }
  switch (ent.type()) {
    case raftpb::ENTRY_NORMAL:
      return f(ent.data());
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
      LEPTON_CRITICAL("Unknown entry type: {}", enum_name(ent.type()));
  }
  LEPTON_CRITICAL("Unknown entry type: {}", enum_name(ent.type()));
  return fmt::format("Unknown entry type: {}", enum_name(ent.type()));
}

std::string describe_entry(const raftpb::entry &ent, entry_formatter_func f) {
  auto formatted = formattted_describe_entry(ent, f);
  if (!formatted.empty()) {
    formatted = fmt::format(" {}", formatted);
  }
  return fmt::format("{}/{} {}{}", ent.term(), ent.index(), enum_name(ent.type()), formatted);
}

// DescribeEntries calls DescribeEntry for each Entry, adding a newline to
// each.
std::string describe_entries(const pb::repeated_entry &entries, entry_formatter_func f) {
  return fmt::format(
      "{}\n",
      fmt::join(entries | std::views::transform([&](const auto &ent) { return describe_entry(ent, f); }), "\n"));
}

static std::string convert_repeated_uint64_to_string(const pb::repeated_uint64 &vec) {
  return fmt::format("[{}]", absl::StrJoin(vec, " "));
}

std::string describe_conf_state(const raftpb::conf_state &state) {
  return fmt::format("Voters:{} VotersOutgoing:{} Learners:{} LearnersNext:{} AutoLeave:{}",
                     convert_repeated_uint64_to_string(state.voters()),
                     convert_repeated_uint64_to_string(state.voters_outgoing()),
                     convert_repeated_uint64_to_string(state.learners()),
                     convert_repeated_uint64_to_string(state.learners_next()), state.auto_leave());
}

std::string describe_snapshot(const raftpb::snapshot &snap) {
  const auto &m = snap.metadata();
  return fmt::format("Index:{} Term:{} ConfState:{}", m.index(), m.term(), describe_conf_state(m.conf_state()));
}

static std::string describe_target(uint64_t id) {
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

std::string describe_message_with_indent(const std::string &indent, const raftpb::message &m, entry_formatter_func f) {
  fmt::memory_buffer buf;

  fmt::format_to(std::back_inserter(buf), "{}{}->{} {} Term:{} Log:{}/{}", indent, describe_target(m.from()),
                 describe_target(m.to()), enum_name(m.type()), m.term(), m.log_term(), m.index());

  if (m.reject()) {
    fmt::format_to(std::back_inserter(buf), " Rejected (Hint: {})", m.reject_hint());
  }
  if (m.commit() != 0) {
    fmt::format_to(std::back_inserter(buf), " Commit:{}", m.commit());
  }
  if (m.vote() != 0) {
    fmt::format_to(std::back_inserter(buf), " Vote:{}", m.vote());
  }

  if (m.entries_size() == 1) {
    fmt::format_to(std::back_inserter(buf), " Entries:[{}]", describe_entry(m.entries(0), f));
  } else if (m.entries_size() > 1) {
    fmt::format_to(std::back_inserter(buf), " Entries:[");
    for (int i = 0; i < m.entries_size(); ++i) {
      fmt::format_to(std::back_inserter(buf), "\n{}  {}", indent, describe_entry(m.entries(i), f));
    }
    fmt::format_to(std::back_inserter(buf), "\n{}]", indent);
  }

  if (m.has_snapshot() && !pb::is_empty_snap(m.snapshot())) {
    fmt::format_to(std::back_inserter(buf), "\n{}  Snapshot: {}", indent, describe_snapshot(m.snapshot()));
  }

  if (m.responses_size() > 0) {
    fmt::format_to(std::back_inserter(buf), " Responses:[");
    for (int i = 0; i < m.responses_size(); ++i) {
      fmt::format_to(std::back_inserter(buf), "\n{}", describe_message_with_indent(indent + "  ", m.responses(i), f));
    }
    fmt::format_to(std::back_inserter(buf), "\n{}]", indent);
  }

  return fmt::to_string(buf);
}

// DescribeMessage returns a concise human-readable description of a
// Message for debugging.
std::string describe_message(const raftpb::message &m, entry_formatter_func f) {
  return describe_message_with_indent("", m, f);
}

std::string describe_ready(const ready &rd, entry_formatter_func f) {
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
    auto entries_str = describe_entries(rd.entries, f);
    fmt::format_to(std::back_inserter(buf), "Entries:\n{}", entries_str);
  }

  // Handle snapshot
  if (!pb::is_empty_snap(rd.snapshot)) {
    auto snapshot_str = describe_snapshot(rd.snapshot);
    fmt::format_to(std::back_inserter(buf), "Snapshot {}\n", snapshot_str);
  }

  // Handle committed_entries
  if (!rd.committed_entries.empty()) {
    auto committed_entries_str = describe_entries(rd.committed_entries, f);
    fmt::format_to(std::back_inserter(buf), "CommittedEntries:\n{}", committed_entries_str);
  }

  // Handle messages
  if (!rd.messages.empty()) {
    fmt::format_to(std::back_inserter(buf), "Messages:\n");
    for (const auto &msg : rd.messages) {
      auto msg_str = describe_message(msg, f);
      fmt::format_to(std::back_inserter(buf), "{}\n", msg_str);
    }
  }

  // Final assembly
  if (buf.size() > 0) {
    return fmt::format("Ready MustSync={}:\n{}", rd.must_sync, std::string_view(buf.data(), buf.size()));
  }
  return "<empty Ready>";
}

}  // namespace lepton::core
