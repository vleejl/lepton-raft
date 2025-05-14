#include "conf_change.h"

#include <google/protobuf/util/message_differencer.h>

#include "error.h"
#include "fmt/format.h"
#include "leaf.hpp"
#include "magic_enum.hpp"
#include "raft.pb.h"
using google::protobuf::util::MessageDifferencer;
namespace lepton {

namespace pb {

template <class>
constexpr bool always_false = false;  // 自行实现

leaf::result<std::tuple<raftpb::entry_type, std::string>> serialize_conf_change(const conf_change_var& cc) {
  return std::visit(
      [](const auto& c) -> leaf::result<std::tuple<raftpb::entry_type, std::string>> {
        using T = std::decay_t<decltype(c)>;

        if constexpr (std::is_same_v<T, std::monostate>) {
          // 处理 Go 的 nil 情况
          return std::make_tuple(raftpb::ENTRY_CONF_CHANGE_V2, "");
        } else if constexpr (std::is_same_v<T, raftpb::conf_change>) {
          std::string data;
          if (!c.SerializeToString(&data)) {  // 假设使用 protobuf 的序列化
            return leaf::new_error(std::errc::invalid_argument, "serialize failed");
          }
          return std::make_tuple(raftpb::ENTRY_CONF_CHANGE, data);
        } else if constexpr (std::is_same_v<T, raftpb::conf_change_v2>) {
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

leaf::result<raftpb::message> conf_change_to_message(const conf_change_var& cc) {
  return leaf::try_handle_some(
      [&]() -> leaf::result<raftpb::message> {
        BOOST_LEAF_AUTO(v, serialize_conf_change(cc));
        auto& [typ, data] = v;
        raftpb::message m;
        m.set_type(raftpb::message_type::MSG_PROP);
        auto entry = m.add_entries();
        entry->set_type(typ);
        entry->set_data(data);
        return m;
      },
      [&](const lepton_error& e) -> leaf::result<raftpb::message> { return new_error(e); });
}

std::tuple<raftpb::conf_change, bool> conf_change_as_v1(raftpb::conf_change&& cc) { return {cc, true}; }

raftpb::conf_change_v2 conf_change_as_v2(raftpb::conf_change&& cc) {
  raftpb::conf_change_v2 obj;
  auto changes = obj.add_changes();
  changes->set_type(cc.type());
  changes->set_node_id(cc.node_id());
  obj.set_allocated_context(cc.release_context());
  return obj;
}

std::tuple<raftpb::conf_change, bool> conf_change_as_v1(raftpb::conf_change_v2&& _) {
  return {raftpb::conf_change{}, false};
}

raftpb::conf_change_v2 conf_change_as_v2(raftpb::conf_change_v2&& cc) { return cc; }

// first return: 若为 true，表示进入联合共识后自动退出联合状态。
// second return: 若为 true，表示该配置变更必须使用联合共识
std::tuple<bool, bool> enter_joint(raftpb::conf_change_v2 c) {
  // NB: in theory, more config changes could qualify for the "simple"
  // protocol but it depends on the config on top of which the changes apply.
  // For example, adding two learners is not OK if both nodes are part of the
  // base config (i.e. two voters are turned into learners in the process of
  // applying the conf change). In practice, these distinctions should not
  // matter, so we keep it simple and use Joint Consensus liberally.
  if (c.transition() != raftpb::conf_change_transition::CONF_CHANGE_TRANSITION_AUTO || c.changes_size() > 1) {
    // Use Joint Consensus.
    bool auto_leave = false;
    switch (c.transition()) {
      case raftpb::conf_change_transition::CONF_CHANGE_TRANSITION_AUTO: {
        auto_leave = true;
        break;
      }
      case raftpb::conf_change_transition::CONF_CHANGE_TRANSITION_JOINT_IMPLICIT: {
        auto_leave = true;
        break;
      }
      case raftpb::conf_change_transition::CONF_CHANGE_TRANSITION_JOINT_EXPLICIT: {
        // use auto_leave default value: false
        break;
      }
      default:
        LEPTON_CRITICAL("unknown transition: %+v", magic_enum::enum_name(c.transition()));
        break;
    }
    return {auto_leave, true};
  }
  return {false, false};
}

bool leave_joint(raftpb::conf_change_v2 c) {
  c.clear_context();
  return MessageDifferencer::Equals(c, raftpb::conf_change_v2{});
}
}  // namespace pb

}  // namespace lepton
