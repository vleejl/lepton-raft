#include <raft.pb.h>

#include <cassert>
#include <cstddef>

#include "error/leaf.h"
#include "error/logic_error.h"
#include "interaction_env.h"
#include "raft_core/describe.h"
#include "raft_core/pb/types.h"

namespace interaction {
lepton::leaf::result<void> interaction_env::handle_process_append_thread(const datadriven::test_data &test_data) {
  auto idxs = node_idxs(test_data);
  for (auto idx : idxs) {
    if (idxs.size() > 1) {
      auto has_error = false;
      std::string msg;
      output->write_string(fmt::format("> {} processing append thread\n", idx + 1));
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
    } else {
      LEPTON_LEAF_CHECK(process_append_thread(idx));
    }
  }
  return {};
}

lepton::leaf::result<void> interaction_env::process_append_thread(std::size_t node_idx) {
  auto &n = nodes[node_idx];
  if (n.append_work.empty()) {
    output->write_string("no append work to perform");
    return {};
  }
  assert(!n.append_work.empty());
  auto m = std::move(n.append_work[0]);        // 移动第一个元素
  n.append_work.erase(n.append_work.begin());  // 删除空槽
  lepton::core::pb::repeated_message resps;
  m.mutable_responses()->Swap(&resps);
  m.clear_responses();
  output->write_string("Processing:\n");
  output->write_string(lepton::core::describe_message(m, nullptr) + "\n");
  raftpb::HardState hard_state;
  hard_state.set_term(m.term());
  hard_state.set_vote(m.vote());
  hard_state.set_commit(m.commit());
  raftpb::Snapshot snap;
  if (m.has_snapshot()) {
    snap.CopyFrom(m.snapshot());
  }
  lepton::core::pb::repeated_entry ents;
  ents.CopyFrom(m.entries());
  LEPTON_LEAF_CHECK(process_append(n, std::move(hard_state), std::move(ents), std::move(snap)));
  output->write_string("Responses:\n");
  for (const auto &resp : resps) {
    output->write_string(lepton::core::describe_message(resp, nullptr) + "\n");
  }
  messages.MergeFrom(resps);
  return {};
}

lepton::leaf::result<void> process_append(node &n, raftpb::HardState &&hard_state,
                                          lepton::core::pb::repeated_entry &&ents, raftpb::Snapshot &&snap) {
  // TODO(tbg): the order of operations here is not necessarily safe. See:
  // https://github.com/etcd-io/etcd/pull/10861
  auto &s = n.storage;
  if (!lepton::core::pb::is_empty_hard_state(hard_state)) {
    LEPTON_LEAF_CHECK(s->set_hard_state(std::move(hard_state)));
  }
  if (!lepton::core::pb::is_empty_snap(snap)) {
    if (!ents.empty()) {
      return lepton::new_error(lepton::logic_error::INVALID_PARAM, "can't apply snapshot and entries at the same time");
    }
    LEPTON_LEAF_CHECK(s->apply_snapshot(std::move(snap)));
    return {};
  }
  if (!ents.empty()) {
    LEPTON_LEAF_CHECK(s->append(std::move(ents)));
  }
  return {};
}

}  // namespace interaction