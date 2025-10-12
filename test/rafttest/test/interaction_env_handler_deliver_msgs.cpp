#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "describe.h"
#include "interaction_env.h"
#include "leaf.h"
#include "log.h"
#include "logic_error.h"
#include "raft.pb.h"
#include "test_raft_protobuf.h"
#include "test_utility_data.h"

namespace interaction {

lepton::leaf::result<void> interaction_env::handle_deliver_msgs(const datadriven::test_data &test_data) {
  // raftpb::message_type
  int raftpb_message_type = -1;
  std::vector<recipient> rs;
  for (const auto &arg : test_data.cmd_args) {
    if (arg.vals_.empty()) {
      std::uint64_t id = 0;
      auto result = safe_stoull(arg.key_);
      assert(result);
      rs.emplace_back(recipient{*result, false});
    }
    for (std::size_t j = 0; j < arg.vals_.size(); ++j) {
      if (arg.key_ == "drop") {
        std::uint64_t id = 0;
        auto err = arg.scan_err(j, id);
        assert(err);
        auto found = false;
        for (auto &r : rs) {
          if (r.id == id) {
            found = true;
            break;
          }
        }
        if (found) {
          LEPTON_CRITICAL("can't both deliver and drop msgs to {}", id);
          assert(false);
        }
        rs.emplace_back(recipient{id, true});
      } else if (arg.key_ == "type") {
        std::string msg_type_val;
        auto err = arg.scan_err(j, msg_type_val);
        assert(err);
        if (message_str2enum_map.find(msg_type_val) == message_str2enum_map.end()) {
          LEPTON_CRITICAL("unknown message type {}", msg_type_val);
          assert(false);
        }
        raftpb_message_type = static_cast<int>(message_str2enum_map[msg_type_val]);
      }
    }
  }
  if (auto n = deliver_msgs(raftpb_message_type, rs); n == 0) {
    output->write_string("no messages\n");
  }
  return {};
}

int interaction_env::deliver_msgs(int raftpb_message_type, const std::vector<recipient> &rs) {
  int n = 0;
  for (const auto &r : rs) {
    auto [msgs, env_msgs] = split_msgs(messages, r.id, raftpb_message_type, r.drop);
    messages = std::move(env_msgs);
    n += static_cast<int>(msgs.size());
    for (const auto &m : msgs) {
      if (r.drop) {
        output->write_string("dropped: ");
      }
      output->write_string(lepton::describe_message(m));
      if (r.drop) {
        // NB: it's allowed to drop messages to nodes that haven't been instantiated yet,
        // we haven't used msg.To yet.
        continue;
      }
      assert(m.to() > 0);
      auto node_idx = static_cast<std::size_t>(m.to() - 1);
      assert(node_idx < nodes.size());
      auto &n = nodes[node_idx];
      auto _ = boost::leaf::try_handle_some(
          [&]() -> lepton::leaf::result<void> {
            LEPTON_LEAF_CHECK(n.raw_node.step(raftpb::message{m}));
            return {};
          },
          [&](const lepton::lepton_error &e) -> lepton::leaf::result<void> {
            output->write_string(e.message);
            return {};
          });
    }
  }
  return n;
}
}  // namespace interaction