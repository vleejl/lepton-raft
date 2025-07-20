#include "test_raft_utils.h"

#include <gtest/gtest.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "lepton_error.h"
#include "log.h"
#include "memory_storage.h"
#include "progress.h"
#include "proxy.h"
#include "raft.h"
#include "raft.pb.h"
#include "storage.h"
#include "test_utility_data.h"
#include "tracker.h"

using namespace lepton;

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

test_memory_storage_options with_learners(lepton::pb::repeated_uint64 &&learners) {
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

std::unique_ptr<lepton::memory_storage> new_test_memory_storage_ptr(
    std::vector<test_memory_storage_options> &&options) {
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

std::vector<std::uint64_t> ids_by_size(std::size_t size) {
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
      nstorage.insert({id, new_test_memory_storage_ptr({with_peers(ids_by_size(size))})});
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
    auto msg_to = msg.to();
    assert(peers.contains(msg_to));
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

memory_storage new_test_memory_storage(std::vector<test_memory_storage_options> &&options) {
  memory_storage ms;
  for (auto &option : options) {
    option(ms);
  }
  return ms;
}

void check_raft_node_after_send_msg(const std::vector<test_expected_raft_status> &tests, std::source_location loc) {
  int test_case_idx = -1;
  for (const auto &iter : tests) {
    test_case_idx++;
    ASSERT_EQ(magic_enum::enum_name(iter.expected_state), magic_enum::enum_name(iter.raft_handle->state_type_))
        << fmt::format("#{} [{}:{}][{}]\n", test_case_idx, loc.file_name(), loc.line(), loc.function_name());
    ASSERT_EQ(iter.expected_term, iter.raft_handle->term_)
        << fmt::format("#{} [{}:{}][{}]\n", test_case_idx, loc.file_name(), loc.line(), loc.function_name());
    ASSERT_EQ(iter.last_index, iter.raft_handle->raft_log_handle_.last_index())
        << fmt::format("#{} [{}:{}][{}]\n", test_case_idx, loc.file_name(), loc.line(), loc.function_name());
  }
}

// nextEnts returns the appliable entries and updates the applied index.
lepton::pb::repeated_entry next_ents(raft &r, memory_storage &s) {
  lepton::pb::repeated_entry ents;
  auto next_unstable_ents = r.raft_log_handle_.next_unstable_ents();
  for (const auto &entry : next_unstable_ents) {
    ents.Add()->CopyFrom(*entry);
  }
  // Append unstable entries.
  s.append(std::move(ents));
  r.raft_log_handle_.stable_to(r.raft_log_handle_.last_entry_id());

  // Run post-append steps.
  r.advance_messages_after_append();

  // Return committed entries.
  auto next_committed_ents = r.raft_log_handle_.next_committed_ents(true);
  r.raft_log_handle_.applied_to(r.raft_log_handle_.committed(), 0);
  return next_committed_ents;
}

void must_append_entry(raft &r, lepton::pb::repeated_entry &&ents) {
  if (!r.append_entry(std::move(ents))) {
    LEPTON_CRITICAL("entry unexpectedly dropped");
  }
}

lepton::raft new_test_raft(lepton::config &&config) {
  auto r = new_raft(std::move(config));
  assert(r);
  return std::move(r.value());
}

lepton::raft new_test_raft(std::uint64_t id, int election_tick, int heartbeat_tick,
                           pro::proxy<storage_builer> &&storage) {
  auto r = new_raft(new_test_config(id, election_tick, heartbeat_tick, std::move(storage)));
  assert(r);
  return std::move(r.value());
}

lepton::raft new_test_learner_raft(std::uint64_t id, int election_tick, int heartbeat_tick,
                                   pro::proxy<storage_builer> &&storage) {
  return new_test_raft(id, election_tick, heartbeat_tick, std::move(storage));
}

raftpb::message new_pb_message(std::uint64_t from, std::uint64_t to, raftpb::message_type type) {
  raftpb::message msg;
  msg.set_from(from);
  msg.set_to(to);
  msg.set_type(type);
  return msg;
}

raftpb::message new_pb_message(std::uint64_t from, std::uint64_t to, std::uint64_t term, raftpb::message_type type) {
  raftpb::message msg;
  msg.set_from(from);
  msg.set_to(to);
  msg.set_term(term);
  msg.set_type(type);
  return msg;
}

raftpb::message new_pb_message(std::uint64_t from, std::uint64_t to, raftpb::message_type type, std::string data) {
  raftpb::message msg;
  msg.set_from(from);
  msg.set_to(to);
  msg.set_type(type);
  auto entry = msg.add_entries();
  entry->set_data(data);
  return msg;
}

raftpb::message new_pb_message(std::uint64_t from, std::uint64_t to, raftpb::message_type type,
                               lepton::pb::repeated_entry &&entries) {
  raftpb::message msg;
  msg.set_from(from);
  msg.set_to(to);
  msg.set_type(type);
  msg.mutable_entries()->Swap(&entries);
  return msg;
}

state_machine_builer_pair ents_with_config(std::function<void(lepton::config &)> config_func,
                                           std::vector<std::uint64_t> &&term) {
  memory_storage ms;
  lepton::pb::repeated_entry entries;
  for (std::size_t i = 0; i < term.size(); ++i) {
    auto entry = entries.Add();
    entry->set_index(i + 1);
    entry->set_term(term[i]);
  }
  assert(ms.append(std::move(entries)));

  auto storage = pro::make_proxy<storage_builer, memory_storage>(std::move(ms));
  auto cfg = new_test_config(1, 5, 1, std::move(storage));
  if (config_func != nullptr) {
    config_func(cfg);
  }
  auto r = new_raft(std::move(cfg));
  assert(r);
  r->reset(term.back());
  auto raft_handle = std::make_unique<lepton::raft>(std::move(r.value()));
  return state_machine_builer_pair{std::move(raft_handle)};
}

// votedWithConfig creates a raft state machine with Vote and Term set
// to the given value but no log entries (indicating that it voted in
// the given term but has not received any logs).
state_machine_builer_pair voted_with_config(std::function<void(lepton::config &)> config_func, std::uint64_t vote,
                                            std::uint64_t term) {
  memory_storage ms;
  raftpb::hard_state hard_state;
  hard_state.set_vote(vote);
  hard_state.set_term(term);
  ms.set_hard_state(std::move(hard_state));

  auto storage = pro::make_proxy<storage_builer, memory_storage>(std::move(ms));
  auto cfg = new_test_config(1, 5, 1, std::move(storage));
  if (config_func != nullptr) {
    config_func(cfg);
  }
  auto r = new_raft(std::move(cfg));
  assert(r);
  r->reset(term);
  auto raft_handle = std::make_unique<lepton::raft>(std::move(r.value()));
  return state_machine_builer_pair{std::move(raft_handle)};
}

void raft_config_quorum_hook(lepton::config &cfg) { cfg.check_quorum = true; }

void raft_config_pre_vote(lepton::config &cfg) { cfg.pre_vote = true; }

void raft_config_read_only_lease_based(lepton::config &cfg) {
  cfg.read_only_opt = lepton::read_only_option::READ_ONLY_LEASE_BASED;
}

void raft_quorum_hook(lepton::raft &sm) { ASSERT_TRUE(sm.check_quorum_); }

void raft_pre_vote_hook(lepton::raft &sm) { ASSERT_TRUE(sm.pre_vote_); }

void raft_read_only_lease_based_hook(lepton::raft &sm) {
  ASSERT_EQ(lepton::read_only_option::READ_ONLY_LEASE_BASED, sm.read_only_.read_only_opt());
}

void raft_become_follower_hook(lepton::raft &sm) { sm.become_follower(1, NONE); }

network init_network(std::vector<std::uint64_t> &&ids,
                     std::vector<std::function<void(lepton::config &cfg)>> raft_config_hook,
                     std::vector<std::function<void(lepton::raft &sm)>> raft_hook) {
  std::vector<state_machine_builer_pair> peers;
  auto append_raft_node_func = [&](std::uint64_t id, std::vector<std::uint64_t> peer_ds) {
    auto sm_cfg = new_test_config(
        id, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers(std::move(peer_ds))}})));
    for (auto func : raft_config_hook) {
      func(sm_cfg);
    }
    auto sm = new_test_raft(std::move(sm_cfg));
    for (auto func : raft_hook) {
      func(sm);
    }
    peers.emplace_back(state_machine_builer_pair{std::make_unique<lepton::raft>(std::move(sm))});
  };
  for (auto id : ids) {
    append_raft_node_func(id, ids);
  }
  return new_network(std::move(peers));
}

network init_empty_network(std::vector<std::uint64_t> &&ids) {
  std::vector<state_machine_builer_pair> peers;
  for (auto _ : ids) {
    peers.emplace_back(state_machine_builer_pair{});
  }
  return new_network(std::move(peers));
}

void emplace_nil_peer(std::vector<state_machine_builer_pair> &peers) {
  peers.emplace_back(state_machine_builer_pair{});
}

void emplace_nop_stepper(std::vector<state_machine_builer_pair> &peers) {
  emplace_nil_peer(peers);
  peers.back().init_black_hole_builder(pro::make_proxy<state_machine_builer, black_hole>());
}

auto ignore_size_hint_mem_storage_entries(lepton::memory_storage &m, std::uint64_t lo, std::uint64_t hi,
                                          std::uint64_t _) {
  return m.entries(lo, hi, lepton::NO_LIMIT);
}