#include <cstddef>
#include <optional>

#include "conf_change.h"
#include "describe.h"
#include "interaction_env.h"
#include "leaf.h"
#include "lepton_error.h"
#include "logic_error.h"
#include "raft.pb.h"
#include "test_utility_data.h"

namespace interaction {
lepton::leaf::result<void> interaction_env::handle_process_apply_thread(const datadriven::test_data &test_data) {
  auto idxs = node_idxs(test_data);
  for (auto idx : idxs) {
    if (idxs.size() > 1) {
      auto has_error = false;
      std::string msg;
      with_indent([&]() {
        output->write_string(fmt::format("> {} processing apply thread\n", idx + 1));
        this->with_indent([&]() {
          auto err = process_apply_thread(idx);
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
      });
      if (has_error) {
        return new_error(lepton::logic_error::INVALID_PARAM, msg);
      }
    } else {
      LEPTON_LEAF_CHECK(process_apply_thread(idx));
    }
  }
  return {};
}

lepton::leaf::result<void> interaction_env::process_apply_thread(std::size_t node_idx) {
  auto &n = nodes[node_idx];
  if (n.apply_work.empty()) {
    output->write_string("no append work to perform");
    return {};
  }
  auto &m = n.apply_work[0];
  n.apply_work.erase(n.apply_work.begin());
  m.clear_responses();
  output->write_string("Processing:\n");
  output->write_string(lepton::describe_message(m) + "\n");
  LEPTON_LEAF_CHECK(process_apply(n, m.entries()));

  output->write_string("Responses:\n");
  for (const auto &resp : m.responses()) {
    output->write_string(lepton::describe_message(resp) + "\n");
  }
  messages.Add(std::move(m));
  return {};
}

lepton::leaf::result<void> process_apply(node &n, const lepton::pb::repeated_entry &ents) {
  for (const auto &ent : ents) {
    std::string update;
    std::optional<raftpb::conf_state> cs;
    switch (ent.type()) {
      case raftpb::ENTRY_CONF_CHANGE: {
        raftpb::conf_change cc;
        if (!cc.ParseFromString(ent.data())) {
          return lepton::new_error(lepton::logic_error::INVALID_PARAM, "parse failed");
        }
        update = cc.context();
        cs = n.raw_node.apply_conf_change(lepton::pb::conf_change_as_v2(std::move(cc)));
        break;
      }
      case raftpb::ENTRY_CONF_CHANGE_V2: {
        raftpb::conf_change_v2 cc;
        if (!cc.ParseFromString(ent.data())) {
          return lepton::new_error(lepton::logic_error::INVALID_PARAM, "parse failed");
        }
        update = cc.context();
        cs = n.raw_node.apply_conf_change(std::move(cc));
        break;
      }
      default: {
        update = ent.data();
        break;
      }
    }

    // Record the new state by starting with the current state and applying
    // the command.
    const auto &last_snap = n.history[n.history.size() - 1];
    raftpb::snapshot snap;
    snap.mutable_data()->append(last_snap.data().begin(), last_snap.data().end());
    // NB: this hard-codes an "appender" state machine.
    snap.mutable_data()->append(std::move(update));
    snap.mutable_metadata()->set_index(ent.index());
    snap.mutable_metadata()->set_term(ent.term());
    if (cs.has_value()) {
      cs = n.history[n.history.size() - 1].metadata().conf_state();
    }
    snap.mutable_metadata()->mutable_conf_state()->CopyFrom(cs.value());
    n.history.Add(std::move(snap));
  }
  return {};
}
}  // namespace interaction