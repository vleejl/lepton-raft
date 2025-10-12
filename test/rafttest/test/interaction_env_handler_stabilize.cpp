#include <cstdint>
#include <tuple>

#include "interaction_env.h"
#include "leaf.h"
#include "raft.pb.h"

namespace interaction {

// Don't drop local messages, which require reliable delivery.
static bool is_local_msg(const raftpb::message &msg) {
  return msg.from() == msg.to() || lepton::pb::is_local_msg_target(msg.to()) ||
         lepton::pb::is_local_msg_target(msg.from());
}

// splitMsgs extracts messages for the given recipient of the given type (-1 for
// all types) from msgs, and returns them along with the remainder of msgs.
std::tuple<lepton::pb::repeated_message, lepton::pb::repeated_message> split_msgs(
    const lepton::pb::repeated_message &msgs, std::uint64_t to, int raftpb_message_type, bool drop) {
  lepton::pb::repeated_message to_msgs;
  lepton::pb::repeated_message rmdr;
  for (const auto &m : msgs) {
    if ((m.to() == to) && !(drop && is_local_msg(m)) &&
        (raftpb_message_type == -1 || static_cast<int>(m.type()) == raftpb_message_type)) {
      to_msgs.Add()->CopyFrom(m);
    } else {
      rmdr.Add()->CopyFrom(m);
    }
  }
  return {std::move(to_msgs), std::move(rmdr)};
}

lepton::leaf::result<void> interaction_env::handle_stabilize(const datadriven::test_data &test_data) {
  std::vector<defer> restorers;

  auto idxs = node_idxs(test_data);
  for (const auto &arg : test_data.cmd_args) {
    for (std::size_t j = 0; j < arg.vals_.size(); ++j) {
      if (arg.key_ == "log-level") {
        auto current_level = output->get_level();
        // 创建新的 defer 并存储
        restorers.emplace_back([this, current_level] { output->set_level(current_level); });

        std::string level;
        auto err = arg.scan_err(j, level);
        assert(err);
        LEPTON_LEAF_CHECK(log_level(level));
      }
    }
  }
  return stabilize(idxs);
}

lepton::leaf::result<void> interaction_env::stabilize(const std::vector<std::size_t> &idxs) {}
}  // namespace interaction