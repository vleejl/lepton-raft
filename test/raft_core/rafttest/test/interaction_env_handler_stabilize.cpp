#include <cassert>
#include <cstdint>
#include <functional>
#include <tuple>
#include <vector>

#include "error/leaf.h"
#include "fmt/format.h"
#include "interaction_env.h"
#include "raft.pb.h"
#include "spdlog/spdlog.h"

namespace interaction {

// Don't drop local messages, which require reliable delivery.
static bool is_local_msg(const raftpb::Message &msg) {
  return msg.from() == msg.to() || lepton::core::pb::is_local_msg_target(msg.to()) ||
         lepton::core::pb::is_local_msg_target(msg.from());
}

// splitMsgs extracts messages for the given recipient of the given type (-1 for
// all types) from msgs, and returns them along with the remainder of msgs.
std::tuple<lepton::core::pb::repeated_message, lepton::core::pb::repeated_message> split_msgs(
    const lepton::core::pb::repeated_message &msgs, std::uint64_t to, int raftpb_message_type, bool drop) {
  lepton::core::pb::repeated_message to_msgs;
  lepton::core::pb::repeated_message rmdr;
  for (const auto &m : msgs) {
    if ((m.to() == to) && !(drop && is_local_msg(m)) &&
        (raftpb_message_type < 0 || static_cast<int>(m.type()) == raftpb_message_type)) {
      // LOG_INFO("split_msgs {}", m.DebugString());
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

lepton::leaf::result<void> interaction_env::stabilize(const std::vector<std::size_t> &idxs) {
  std::vector<std::reference_wrapper<node>> nodes;
  if (!idxs.empty()) {
    for (const auto &idx : idxs) {
      assert(idx < this->nodes.size());
      nodes.push_back(std::ref(this->nodes[idx]));
    }
  } else {
    for (auto &n : this->nodes) {
      nodes.push_back(std::ref(n));
    }
  }
  while (true) {
    auto done = true;
    for (auto &n_ref : nodes) {
      auto &n = n_ref.get();
      auto &rn = n.raw_node;
      if (rn.has_ready()) {
        auto idx = rn.status().basic_status.id - 1;
        output->write_string(fmt::format("> {} handling Ready\n", idx + 1));
        auto has_error = false;
        std::string msg;
        this->with_indent([&]() {
          auto _ = boost::leaf::try_handle_some(
              [&]() -> lepton::leaf::result<void> {
                LEPTON_LEAF_CHECK(process_ready(idx));
                return {};
              },
              [&](const lepton::lepton_error &e) -> lepton::leaf::result<void> {
                has_error = true;
                msg = e.message;
                return {};
              });
        });
        if (has_error) {
          return new_error(lepton::logic_error::INVALID_PARAM, msg);
        }
        done = false;
      }
    }

    for (auto &n_ref : nodes) {
      auto &n = n_ref.get();
      auto &rn = n.raw_node;
      auto id = rn.status().basic_status.id;
      // NB: we grab the messages just to see whether to print the header.
      // DeliverMsgs will do it again.
      auto msgs = split_msgs(messages, id, -1, false);
      if (!std::get<0>(msgs).empty()) {
        output->write_string(fmt::format("> {} receiving messages\n", id));
        this->with_indent([&]() { this->deliver_msgs(-1, {{.id = id}}); });
        done = false;
      }
    }

    for (auto &n_ref : nodes) {
      auto &n = n_ref.get();
      auto &rn = n.raw_node;
      auto idx = rn.status().basic_status.id - 1;
      if (!n.append_work.empty()) {
        auto has_error = false;
        std::string msg;
        output->write_string(fmt::format("> {} processing append thread\n", idx + 1));
        while (!n.append_work.empty()) {
          this->with_indent([&]() {
            auto _ = boost::leaf::try_handle_some(
                [&]() -> lepton::leaf::result<void> {
                  LEPTON_LEAF_CHECK(process_append_thread(idx));
                  return {};
                },
                [&](const lepton::lepton_error &e) -> lepton::leaf::result<void> {
                  has_error = true;
                  msg = e.message;
                  return {};
                });
          });
          if (has_error) {
            return new_error(lepton::logic_error::INVALID_PARAM, msg);
          }
        }
        done = false;
      }
    }

    for (auto &n_ref : nodes) {
      auto &n = n_ref.get();
      auto &rn = n.raw_node;
      auto idx = rn.status().basic_status.id - 1;
      if (n.apply_work.size() > 0) {
        auto has_error = false;
        std::string msg;
        output->write_string(fmt::format("> {} processing apply thread\n", idx + 1));
        this->with_indent([&]() {
          auto _ = boost::leaf::try_handle_some(
              [&]() -> lepton::leaf::result<void> {
                LEPTON_LEAF_CHECK(process_apply_thread(idx));
                return {};
              },
              [&](const lepton::lepton_error &e) -> lepton::leaf::result<void> {
                has_error = true;
                msg = e.message;
                return {};
              });
        });
        if (has_error) {
          return new_error(lepton::logic_error::INVALID_PARAM, msg);
        }
        done = false;
      }
    }

    if (done) {
      return {};
    }
  }
  return {};
}
}  // namespace interaction