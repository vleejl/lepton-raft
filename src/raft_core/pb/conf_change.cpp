#include "raft_core/pb/conf_change.h"

#include <absl/strings/str_join.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <raft.pb.h>

#include <cassert>
#include <string>
#include <system_error>

#include "basic/enum_name.h"
#include "basic/log.h"
#include "error/error.h"
#include "error/leaf.h"
#include "error/logic_error.h"
namespace lepton::core {

namespace pb {
template <class>
constexpr bool always_false = false;

leaf::result<raftpb::ConfChange> conf_change_var_as_v1(conf_change_var&& cc) {
  return std::visit(
      [](auto&& c) -> leaf::result<raftpb::ConfChange> {
        using T = std::decay_t<decltype(c)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          // 处理 Go 的 nil 情况
          assert(false);
          return raftpb::ConfChange{};
        } else if constexpr (std::is_same_v<T, raftpb::ConfChange>) {
          return std::move(c);
        } else if constexpr (std::is_same_v<T, raftpb::ConfChangeV2>) {
          return new_error(logic_error::INVALID_PARAM);
        } else {
          static_assert(always_false<T>, "非穷尽类型检查");
        }
      },
      cc);
}

raftpb::ConfChangeV2 conf_change_var_as_v2(conf_change_var&& cc) {
  return std::visit(
      [](auto&& c) -> raftpb::ConfChangeV2 {
        using T = std::decay_t<decltype(c)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          // 处理 Go 的 nil 情况
          assert(false);
          return raftpb::ConfChangeV2{};
        } else if constexpr (std::is_same_v<T, raftpb::ConfChange>) {
          return conf_change_as_v2(std::move(c));
        } else if constexpr (std::is_same_v<T, raftpb::ConfChangeV2>) {
          return std::move(c);
        } else {
          static_assert(always_false<T>, "非穷尽类型检查");
        }
      },
      cc);
}

leaf::result<std::tuple<raftpb::EntryType, std::string>> serialize_conf_change(const conf_change_var& cc) {
  return std::visit(
      [](const auto& c) -> leaf::result<std::tuple<raftpb::EntryType, std::string>> {
        using T = std::decay_t<decltype(c)>;

        if constexpr (std::is_same_v<T, std::monostate>) {
          // 处理 Go 的 nil 情况
          return std::make_tuple(raftpb::ENTRY_CONF_CHANGE_V2, "");
        } else if constexpr (std::is_same_v<T, raftpb::ConfChange>) {
          std::string data;
          if (!c.SerializeToString(&data)) {  // 假设使用 protobuf 的序列化
            return leaf::new_error(std::errc::invalid_argument, "serialize failed");
          }
          return std::make_tuple(raftpb::ENTRY_CONF_CHANGE, data);
        } else if constexpr (std::is_same_v<T, raftpb::ConfChangeV2>) {
          std::string data;
          if (!c.SerializeToString(&data)) {
            return leaf::new_error(std::errc::invalid_argument, "serialize failed");
          }
          return std::make_tuple(raftpb::ENTRY_CONF_CHANGE_V2, data);
        } else {
          static_assert(always_false<T>, "非穷尽类型检查");
        }
      },
      cc);
}

leaf::result<raftpb::Message> conf_change_to_message(const conf_change_var& cc) {
  return leaf::try_handle_some(
      [&]() -> leaf::result<raftpb::Message> {
        BOOST_LEAF_AUTO(v, serialize_conf_change(cc));
        auto& [typ, data] = v;
        raftpb::Message m;
        m.set_type(raftpb::MessageType::MSG_PROP);
        auto entry = m.add_entries();
        entry->set_type(typ);
        entry->set_data(data);
        return m;
      },
      [&](const lepton_error& e) -> leaf::result<raftpb::Message> { return new_error(e); });
}

std::tuple<raftpb::ConfChange, bool> conf_change_as_v1(raftpb::ConfChange&& cc) { return {cc, true}; }

raftpb::ConfChangeV2 conf_change_as_v2(raftpb::ConfChange&& cc) {
  raftpb::ConfChangeV2 obj;
  auto changes = obj.add_changes();
  changes->set_type(cc.type());
  changes->set_node_id(cc.node_id());
  obj.set_allocated_context(cc.release_context());
  return obj;
}

raftpb::ConfChangeV2 conf_change_as_v2(raftpb::ConfChangeV2&& cc) { return cc; }

// first return: 若为 true，表示进入联合共识后自动退出联合状态。
// second return: 若为 true，表示该配置变更必须使用联合共识
std::tuple<bool, bool> enter_joint(const raftpb::ConfChangeV2& c) {
  // NB: in theory, more config changes could qualify for the "simple"
  // protocol but it depends on the config on top of which the changes apply.
  // For example, adding two learners is not OK if both nodes are part of the
  // base config (i.e. two voters are turned into learners in the process of
  // applying the conf change). In practice, these distinctions should not
  // matter, so we keep it simple and use Joint Consensus liberally.
  if (c.transition() != raftpb::ConfChangeTransition::CONF_CHANGE_TRANSITION_AUTO || c.changes_size() > 1) {
    // Use Joint Consensus.
    bool auto_leave = false;
    switch (c.transition()) {
      case raftpb::ConfChangeTransition::CONF_CHANGE_TRANSITION_AUTO: {
        auto_leave = true;
        break;
      }
      case raftpb::ConfChangeTransition::CONF_CHANGE_TRANSITION_JOINT_IMPLICIT: {
        auto_leave = true;
        break;
      }
      case raftpb::ConfChangeTransition::CONF_CHANGE_TRANSITION_JOINT_EXPLICIT: {
        // use auto_leave default value: false
        break;
      }
      default:
        LEPTON_CRITICAL("unknown transition: %+v", enum_name(c.transition()));
        break;
    }
    return {auto_leave, true};
  }
  return {false, false};
}

static bool equal_conf_change_single(const raftpb::ConfChangeSingle& a, const raftpb::ConfChangeSingle& b) {
  return a.type() == b.type() && a.node_id() == b.node_id();
}

static bool equal_conf_change_v2(const raftpb::ConfChangeV2& a, const raftpb::ConfChangeV2& b) {
  // 特殊处理 transition 字段：值为 0 视为未设置
  auto get_effective_transition = [](const raftpb::ConfChangeV2& msg) {
    return (msg.has_transition() && msg.transition() != 0) ? msg.transition() : 0;
  };

  // 比较 transition（处理 0 值特殊逻辑）
  if (get_effective_transition(a) != get_effective_transition(b)) {
    return false;
  }

  // Compare `changes` (repeated message, order matters)
  if (a.changes_size() != b.changes_size()) return false;
  for (int i = 0; i < a.changes_size(); ++i) {
    if (!equal_conf_change_single(a.changes(i), b.changes(i))) {
      return false;
    }
  }

  // Compare `context` (optional bytes)
  if (a.has_context() != b.has_context()) return false;
  if (a.has_context() && a.context() != b.context()) return false;

  return true;
}

bool leave_joint(raftpb::ConfChangeV2& c) {
  c.clear_context();
  static raftpb::ConfChangeV2 empty_conf_change;
  return equal_conf_change_v2(c, empty_conf_change);
}
std::string conf_changes_to_string(const repeated_conf_change& ccs) {
  fmt::memory_buffer buf;

  for (int i = 0; i < ccs.size(); ++i) {
    const auto& cs = ccs.Get(i);

    // 添加分隔符（如果不是第一个元素）
    if (i > 0) {
      buf.push_back(' ');
    }

    // 根据变更类型添加前缀
    switch (cs.type()) {
      case raftpb::CONF_CHANGE_ADD_NODE:
        buf.push_back('v');
        break;
      case raftpb::CONF_CHANGE_REMOVE_NODE:
        buf.push_back('r');
        break;
      case raftpb::CONF_CHANGE_UPDATE_NODE:
        buf.push_back('u');
        break;
      case raftpb::CONF_CHANGE_ADD_LEARNER_NODE:
        buf.push_back('l');
        break;
      default:
        fmt::format_to(std::back_inserter(buf), "unknown");
        break;
    }

    // 添加节点ID
    fmt::format_to(std::back_inserter(buf), "{}", cs.node_id());
  }

  return fmt::to_string(buf);
}

leaf::result<repeated_conf_change> conf_changes_from_string(std::string_view s) {
  repeated_conf_change ccs;

  size_t start = 0;
  size_t end = s.find(' ');

  while (start < s.size()) {
    // 模拟 stream >> tok 的行为：跳过前导空格并截取单词
    if (end == std::string_view::npos) end = s.size();
    std::string_view tok = s.substr(start, end - start);

    if (tok.empty()) {  // 处理连续空格
      start = end + 1;
      end = s.find(' ', start);
      continue;
    }

    raftpb::ConfChangeSingle cc;
    switch (tok[0]) {
      case 'v':
        cc.set_type(raftpb::CONF_CHANGE_ADD_NODE);
        break;
      case 'l':
        cc.set_type(raftpb::CONF_CHANGE_ADD_LEARNER_NODE);
        break;
      case 'r':
        cc.set_type(raftpb::CONF_CHANGE_REMOVE_NODE);
        break;
      case 'u':
        cc.set_type(raftpb::CONF_CHANGE_UPDATE_NODE);
        break;
      default:
        return new_error(logic_error::INVALID_PARAM, fmt::format("unknown input: {}", tok));
    }

    // --- 修复点：更兼容的数字解析 ---
    std::string_view id_part = tok.substr(1);
    if (id_part.empty()) {
      return new_error(logic_error::INVALID_PARAM, "missing node id");
    }

    // 使用 strtoull，它不需要 null-terminated 字符串
    // 注意：strtoull 需要 char*，如果 id_part 后面有空格，它会自动停止
    char* endptr = nullptr;
    const char* begin = id_part.data();
    unsigned long long value = std::strtoull(begin, &endptr, 10);

    // 验证转换是否完整覆盖了该单词
    if (endptr != begin + id_part.size()) {
      return new_error(logic_error::INVALID_PARAM, fmt::format("invalid node id: {}", id_part));
    }

    cc.set_node_id(static_cast<uint64_t>(value));
    ccs.Add(std::move(cc));

    // 移动到下一个单词
    start = end + 1;
    end = s.find(' ', start);
  }
  return ccs;
}

std::string describe_conf_change_v2(const raftpb::ConfChangeV2& cc) {
  auto result = cc.ShortDebugString();
  fmt::memory_buffer buf;
  buf.push_back('[');
  if (cc.has_transition()) {
    fmt::format_to(std::back_inserter(buf), "Transition: {} ", magic_enum::enum_name(cc.transition()));
  }
  if (cc.changes_size() > 0) {
    fmt::format_to(std::back_inserter(buf), "Changes: {} ", conf_changes_to_string(cc.changes()));
  }
  if (cc.has_context()) {
    fmt::format_to(std::back_inserter(buf), "Context: {} ", cc.context());
  }
  buf.push_back(']');
  return std::string(buf.data(), buf.size());
}

}  // namespace pb

}  // namespace lepton::core
