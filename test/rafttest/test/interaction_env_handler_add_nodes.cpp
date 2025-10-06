#include <cassert>
#include <cstddef>
#include <memory>
#include <utility>

#include "interaction_env.h"
#include "leaf.h"
#include "memory_storage.h"
#include "raft.pb.h"

namespace interaction {

using snapshot_function_ptr = lepton::leaf::result<raftpb::snapshot> (*)();

// // clang-format off
// struct storage_builer : pro::facade_builder
//   ::add_convention<lepton::storage_initial_state, lepton::leaf::result<std::tuple<raftpb::hard_state,
//   raftpb::conf_state>>()>
//   ::add_convention<lepton::storage_entries, lepton::leaf::result<lepton::pb::repeated_entry>(std::uint64_t lo,
//   std::uint64_t hi, std::uint64_t max_size) const>
//   ::add_convention<lepton::storage_term, lepton::leaf::result<std::uint64_t>(std::uint64_t i) const>
//   ::add_convention<lepton::storage_last_index, lepton::leaf::result<std::uint64_t>() const>
//   ::add_convention<lepton::storage_first_index, lepton::leaf::result<std::uint64_t>() const>
//   ::add_convention<lepton::storage_snapshot, lepton::leaf::result<raftpb::snapshot>() const>
//   ::add_convention<storage_set_hard_state, void(raftpb::hard_state&& hard_state)>
//   ::add_convention<storage_apply_snapshot, lepton::leaf::result<void>(raftpb::snapshot &&snapshot)>
//   ::add_convention<storage_compact, lepton::leaf::result<void>(std::uint64_t compact_index)>
//   ::add_convention<storage_append, lepton::leaf::result<void>(lepton::pb::repeated_entry&& entries)>
//   ::add_skill<pro::skills::as_view>
//   ::build{};
// // clang-format on

struct snap_override_storage {
  pro::proxy<storage_builer> storage;
  std::function<lepton::leaf::result<raftpb::snapshot>()> snap_override_func;

  lepton::leaf::result<std::tuple<raftpb::hard_state, raftpb::conf_state>> initial_state() const {
    return storage->initial_state();
  }

  lepton::leaf::result<lepton::pb::repeated_entry> entries(std::uint64_t lo, std::uint64_t hi,
                                                           std::uint64_t max_size) const {
    return storage->entries(lo, hi, max_size);
  }

  lepton::leaf::result<std::uint64_t> term(std::uint64_t i) const { return storage->term(i); }

  lepton::leaf::result<std::uint64_t> last_index() const { return storage->last_index(); }

  lepton::leaf::result<std::uint64_t> first_index() const { return storage->first_index(); }

  lepton::leaf::result<raftpb::snapshot> snapshot() const {
    if (snap_override_func != nullptr) {
      return snap_override_func();
    }
    return storage->snapshot();
  }

  void set_hard_state(raftpb::hard_state &&hard_state) { storage->set_hard_state(std::move(hard_state)); }

  lepton::leaf::result<void> apply_snapshot(raftpb::snapshot &&snapshot) {
    return storage->apply_snapshot(std::move(snapshot));
  }

  lepton::leaf::result<void> compact(std::uint64_t compact_index) { return storage->compact(compact_index); }

  lepton::leaf::result<void> append(lepton::pb::repeated_entry &&entries) {
    return storage->append(std::move(entries));
  }
};

lepton::leaf::result<void> interaction_env::add_nodes(std::size_t n, const lepton::config &config,
                                                      raftpb::snapshot &snap) {
  auto bootstrap = lepton::pb::is_empty_snap(snap);
  for (std::size_t i = 0; i < n; ++i) {
    auto id = static_cast<std::uint64_t>(nodes.size() + 1);
    auto storage_ptr =
        std::make_unique<snap_override_storage>(pro::make_proxy<storage_builer, lepton::memory_storage>(),
                                                // When you ask for a snapshot, you get the most recent snapshot.
                                                //
                                                // TODO(tbg): this is sort of clunky, but MemoryStorage itself will
                                                // give you some fixed snapshot and also the snapshot changes
                                                // whenever you compact the logs and vice versa, so it's all a bit
                                                // awkward to use.
                                                [&]() -> lepton::leaf::result<raftpb::snapshot> const {
                                                  auto &history = this->nodes[id - 1].history;
                                                  return history.at(history.size() - 1);
                                                });
    auto &s = *storage_ptr;
    if (bootstrap) {
      // NB: we could make this work with 1, but MemoryStorage just
      // doesn't play well with that and it's not a loss of generality.
      if (snap.metadata().index() <= 1) {
        return lepton::new_error(lepton::logic_error::INVALID_PARAM, "index must be specified as > 1 due to bootstrap");
      }
      snap.mutable_metadata()->set_term(1);
      if (auto ret = s.storage->apply_snapshot(std::move(snap)); !ret) {
        return ret;
      }
      auto first_index = s.storage->first_index();
      if (!first_index) {
        return first_index.error();
      }
      auto fi = *first_index;
      // At the time of writing and for *MemoryStorage, applying a
      // snapshot also truncates appropriately, but this would change with
      // other storage engines potentially.
      if (fi != snap.metadata().index() + 1) {
        return lepton::new_error(
            lepton::logic_error::INVALID_PARAM,
            fmt::format("first index {} should be snap index + 1 {}", fi, snap.metadata().index() + 1));
      }
    }
    // fork the config stub
    auto copy_cfg = config.clone();
    copy_cfg.id = id;
    pro::proxy<lepton::storage_builer> storage_proxy = storage_ptr.get();
    copy_cfg.storage = std::move(storage_proxy);
    if (this->options.on_confg) {
      this->options.on_confg(copy_cfg);
      if (copy_cfg.id != id) {
        // This could be supported but then we need to do more work
        // translating back and forth -- not worth it.
        return lepton::new_error(lepton::logic_error::INVALID_PARAM, "OnConfig must not change the ID");
      }
    }
  }
}

lepton::leaf::result<void> interaction_env::handle_add_nodes(const datadriven::test_data &test_data) {
  auto n = first_as_int(test_data);
  raftpb::snapshot snap;
  auto cfg = raft_config_stub();
  assert(test_data.cmd_args.size() >= 1);
  for (std::size_t i = 1; i <= test_data.cmd_args.size(); ++i) {
    const auto &arg = test_data.cmd_args[i];
    for (std::size_t j = 0; j < arg.vals_.size(); ++j) {
      if (arg.key_ == "voters") {
        std::uint64_t id = 0;
        auto err = arg.scan_err(j, id);
        assert(err);
        snap.mutable_metadata()->mutable_conf_state()->add_voters(id);
      } else if (arg.key_ == "learners") {
        std::uint64_t id = 0;
        auto err = arg.scan_err(j, id);
        assert(err);
        snap.mutable_metadata()->mutable_conf_state()->add_learners(id);
      } else if (arg.key_ == "inflight") {
        std::size_t inflight = 0;
        auto err = arg.scan_err(j, inflight);
        assert(err);
        cfg.max_inflight_msgs = inflight;
      } else if (arg.key_ == "index") {
        std::uint64_t index = 0;
        auto err = arg.scan_err(j, index);
        assert(err);
        snap.mutable_metadata()->set_index(index);
      } else if (arg.key_ == "content") {
        std::string content;
        auto err = arg.scan_err(j, content);
        assert(err);
        snap.mutable_data()->swap(content);
      } else if (arg.key_ == "async-storage-writes") {
        bool async_storage_writes = false;
        auto err = arg.scan_err(j, async_storage_writes);
        assert(err);
        cfg.async_storage_writes = async_storage_writes;
      } else if (arg.key_ == "prevote") {
        bool pre_vote = false;
        auto err = arg.scan_err(j, pre_vote);
        assert(err);
        cfg.pre_vote = pre_vote;
      } else if (arg.key_ == "checkquorum") {
        bool check_quorum = false;
        auto err = arg.scan_err(j, check_quorum);
        assert(err);
        cfg.check_quorum = check_quorum;
      } else if (arg.key_ == "max-committed-size-per-ready") {
        std::size_t max_committed_size_per_ready = 0;
        auto err = arg.scan_err(j, max_committed_size_per_ready);
        assert(err);
        cfg.max_committed_size_per_ready = static_cast<std::uint64_t>(max_committed_size_per_ready);
      } else if (arg.key_ == "disable-conf-change-validation") {
        bool disable_conf_change_validation = false;
        auto err = arg.scan_err(j, disable_conf_change_validation);
        assert(err);
        cfg.disable_conf_change_validation = disable_conf_change_validation;
      } else if (arg.key_ == "read_only") {
        std::string read_only_opt;
        auto err = arg.scan_err(j, read_only_opt);
        assert(err);
        if (read_only_opt == "safe") {
          cfg.read_only_opt = lepton::read_only_option::READ_ONLY_SAFE;
        } else if (read_only_opt == "lease") {
          cfg.read_only_opt = lepton::read_only_option::READ_ONLY_LEASE_BASED;
        } else {
          assert(false);
        }
      } else {
        assert(false);
        return lepton::new_error(lepton::logic_error::INVALID_PARAM, fmt::format("unknown key {}", arg.key_));
      }
    }
    return add_nodes(static_cast<std::size_t>(n), cfg, snap);
  }
}
}  // namespace interaction