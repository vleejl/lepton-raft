#include "test_raft_utils.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "error.h"
#include "log.h"
#include "memory_storage.h"
#include "progress.h"
#include "proxy.h"
#include "raft.h"
#include "raft.pb.h"
#include "storage.h"
#include "test_utility_data.h"
#include "tracker.h"

static test_memory_storage_options with_peers(lepton::pb::repeated_uint64 &&peers) {
  // / 将右值 peers 移动构造到堆内存，并用 shared_ptr 管理 auto data =
  auto data = std::make_shared<lepton::pb::repeated_uint64>(std::move(peers));

  // 返回的 lambda 按值捕获 shared_ptr（安全）
  return [data](lepton::memory_storage &ms) {
    auto *conf_state = ms.snapshot_ref().mutable_metadata()->mutable_conf_state();

    // 安全操作：data 的生命周期与 lambda 绑定
    conf_state->mutable_voters()->Swap(data.get());
  };
}

test_memory_storage_options with_peers(std::vector<std::uint64_t> &&peers) {
  lepton::pb::repeated_uint64 repeated_uint64;
  for (auto id : peers) {
    repeated_uint64.Add(id);
  }
  return with_peers(std::move(repeated_uint64));
}

static test_memory_storage_options with_learners(lepton::pb::repeated_uint64 &&learners) {
  // / 将右值 peers 移动构造到堆内存，并用 shared_ptr 管理 auto data =
  auto data = std::make_shared<lepton::pb::repeated_uint64>(std::move(learners));

  // 返回的 lambda 按值捕获 shared_ptr（安全）
  return [data](lepton::memory_storage &ms) {
    auto *conf_state = ms.snapshot_ref().mutable_metadata()->mutable_conf_state();

    // 安全操作：data 的生命周期与 lambda 绑定
    conf_state->mutable_learners()->Swap(data.get());
  };
}

test_memory_storage_options with_learners(std::vector<std::uint64_t> &&learners) {
  lepton::pb::repeated_uint64 repeated_learners;
  for (auto id : learners) {
    repeated_learners.Add(id);
  }
  return with_learners(std::move(repeated_learners));
}

lepton::config new_test_config(std::uint64_t id, int election_tick, int heartbeat_tick,
                               pro::proxy<lepton::storage_builer> &&storage) {
  return lepton::config{id, election_tick, heartbeat_tick, std::move(storage), lepton::NO_LIMIT, 256};
}

std::unique_ptr<lepton::memory_storage> new_test_memory_storage(std::vector<test_memory_storage_options> &&options) {
  auto ms_ptr = std::make_unique<lepton::memory_storage>();
  auto &ms = *ms_ptr;
  for (auto &option : options) {
    option(ms);
  }
  return ms_ptr;
}

void set_randomized_election_timeout(lepton::raft &r, int election_timeout) {
  r.randomized_election_timeout_ = election_timeout;
}

static std::vector<std::uint64_t> ids_by_size(std::size_t size) {
  std::vector<std::uint64_t> ids(size);
  for (std::size_t i = 0; i < size; ++i) {
    ids[i] = 1 + static_cast<std::uint64_t>(i);
  }
  return ids;
}

network new_network_with_config(std::function<void(lepton::config &)> config_func,
                                std::vector<state_machine_builer_pair> &&peers) {
  auto size = peers.size();
  auto peer_addrs = ids_by_size(size);

  std::map<std::uint64_t, state_machine_builer_pair> npeers;
  std::map<std::uint64_t, std::unique_ptr<lepton::memory_storage>> nstorage;
  for (std::size_t i = 0; i < size; ++i) {
    auto id = peer_addrs[i];
    auto &pair_item = peers[i];
    if (pair_item.builder_view) {
      auto view_type_name = std::string(proxy_typeid(*pair_item.builder_view).name());
      if (view_type_name.find("raft") != std::string::npos) {
        if (pair_item.raft_handle == nullptr) {
          assert(pair_item.raft_handle != nullptr);
        }
        auto &raft_handle = *pair_item.raft_handle;
        // TODO(tbg): this is all pretty confused. Clean this up.
        std::map<std::uint64_t, bool> learners;
        auto &learners_view = raft_handle.trk_.config_view().learners;
        if (learners_view) {
          for (auto id : *learners_view) {
            learners[id] = true;
          }
        }
        const_cast<std::uint64_t &>(raft_handle.id_) = id;
        raft_handle.trk_ =
            lepton::tracker::progress_tracker(raft_handle.trk_.max_inflight(), raft_handle.trk_.max_inflight_bytes());
        if (!learners.empty()) {
          raft_handle.trk_.mutable_config_view().learners.reset();
        }
        for (std::size_t j = 0; j < size; ++j) {
          auto peer_id = peer_addrs[j];
          lepton::tracker::progress pr;
          if (learners.find(peer_id) != learners.end()) {
            pr.set_learner(true);
            raft_handle.trk_.mutable_config_view().add_leaner_node(peer_id);
          } else {
            raft_handle.trk_.mutable_config_view().voters.insert_node_into_primary_config(peer_id);
          }
          raft_handle.trk_.progress_map_mutable_view().mutable_view()[peer_id] = std::move(pr);
        }
        raft_handle.reset(raft_handle.term_);
        npeers.try_emplace(id, std::move(pair_item));
      } else if (view_type_name.find("black_hole") != std::string::npos) {
        npeers.try_emplace(id, std::move(pair_item));
      } else {
        assert(false);
      }
    } else {  // empty proxy view
      assert(!nstorage.contains(id));
      nstorage.insert({id, new_test_memory_storage({with_peers(ids_by_size(size))})});
      pro::proxy<lepton::storage_builer> storage = nstorage[id].get();
      auto cfg = new_test_config(id, 10, 1, std::move(storage));
      if (config_func != nullptr) {
        config_func(cfg);
      }

      auto r = new_raft(std::move(cfg));
      assert(r);
      auto raft_handle = std::make_unique<lepton::raft>(std::move(r.value()));
      state_machine_builer_pair pair{std::move(raft_handle)};
      npeers.try_emplace(id, std::move(pair));
    }
  }
  return {std::move(npeers), std::move(nstorage), {}, {}, nullptr};
}

network new_network(std::vector<state_machine_builer_pair> &&peers) {
  return new_network_with_config(nullptr, std::move(peers));
}

void network::send(std::vector<raftpb::message> &&msgs) {
  while (!msgs.empty()) {
    auto &msg = *msgs.begin();
    auto &p = peers.at(msg.to());
    p.builder_view->step(std::move(msg));
    p.builder_view->advance_messages_after_append();
    auto mm = filter(p.builder_view->read_messages());
    msgs.erase(msgs.begin());
    msgs.insert(msgs.end(), mm.begin(), mm.end());
  }
}

std::vector<raftpb::message> network::filter(const lepton::pb::repeated_message &msgs) {
  std::vector<raftpb::message> mm;
  for (auto &msg : msgs) {
    if (ignorem.contains(msg.type())) {
      continue;
    }
    switch (msg.type()) {
      case raftpb::message_type::MSG_HUP: {
        // hups never go over the network, so don't drop them but panic
        LEPTON_CRITICAL("unexpected msgHup");
        break;
      }
      default: {
        auto perc_iter = dropm.find({msg.from(), msg.to()});
        auto perc = perc_iter != dropm.end() ? perc_iter->second : 0.0;
        if (auto n = rand_float64(); n < perc) {
          continue;
        }
      }
    }
    if (msg_hook != nullptr) {
      if (!msg_hook(msg)) {
        continue;
      }
    }
    // SPDLOG_INFO(msg.DebugString());
    mm.push_back(raftpb::message{msg});
  }
  return mm;
}