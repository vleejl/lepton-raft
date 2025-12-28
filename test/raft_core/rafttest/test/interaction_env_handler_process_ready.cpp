#include <cassert>
#include <cstddef>
#include <utility>

#include "basic/log.h"
#include "error/leaf.h"
#include "error/logic_error.h"
#include "interaction_env.h"
#include "raft_core/describe.h"
#include "raft_core/pb/protobuf.h"
#include "spdlog/spdlog.h"

namespace interaction {
lepton::leaf::result<void> interaction_env::handle_process_ready(const datadriven::test_data &test_data) {
  auto idxs = node_idxs(test_data);
  for (auto idx : idxs) {
    if (idxs.size() > 1) {
      auto has_error = false;
      std::string msg;
      output->write_string(fmt::format("> {} handling Ready\n", idx + 1));
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
    } else {
      LEPTON_LEAF_CHECK(process_ready(idx));
    }
  }
  return {};
}

lepton::leaf::result<void> interaction_env::process_ready(std::size_t node_idx) {
  // TODO(tbg): Allow simulating crashes here.
  assert(node_idx < nodes.size());
  auto &n = nodes[node_idx];
  auto rd = n.raw_node.ready();
  output->write_string(lepton::core::describe_ready(rd, nullptr));
  if (!n.config.async_storage_writes) {
    LEPTON_LEAF_CHECK(process_append(n, std::move(rd.hard_state), std::move(rd.entries), std::move(rd.snapshot)));
    LEPTON_LEAF_CHECK(process_apply(n, rd.committed_entries));
  }
  for (auto &msg : rd.messages) {
    LOG_TRACE("process_ready msg {}", msg.DebugString());
    if (lepton::core::pb::is_local_msg_target(msg.to())) {
      if (!n.config.async_storage_writes) {
        LEPTON_CRITICAL("unexpected local msg target");
      }
      switch (msg.type()) {
        case raftpb::message_type::MSG_STORAGE_APPEND: {
          n.append_work.Add(std::move(msg));
          break;
        }
        case raftpb::message_type::MSG_STORAGE_APPLY: {
          n.apply_work.Add(std::move(msg));
          break;
        }
        default: {
          LEPTON_CRITICAL("unexpected msg type: {}", magic_enum::enum_name(msg.type()));
        }
      }
    } else {
      messages.Add(std::move(msg));
    }
  }
  if (!n.config.async_storage_writes) {
    n.raw_node.advance();
  }
  return {};
}
}  // namespace interaction