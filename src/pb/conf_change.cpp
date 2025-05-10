#include "conf_change.h"

#include "leaf.hpp"
#include "raft.pb.h"

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
}  // namespace pb

}  // namespace lepton
