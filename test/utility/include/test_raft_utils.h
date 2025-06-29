#ifndef _LEPTON_TEST_RAFT_NETWORKING_H_
#define _LEPTON_TEST_RAFT_NETWORKING_H_
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "config.h"
#include "memory_storage.h"
#include "proxy.h"
#include "raft.h"
#include "raft.pb.h"
#include "test_raft_state_machine.h"
#include "types.h"
#include "utility_macros.h"

using test_memory_storage_options = std::function<void(lepton::memory_storage &)>;

test_memory_storage_options with_peers(std::vector<std::uint64_t> &&peers);
test_memory_storage_options with_learners(std::vector<std::uint64_t> &&learners);
std::unique_ptr<lepton::memory_storage> new_test_memory_storage_ptr(std::vector<test_memory_storage_options> &&options);
lepton::config new_test_config(std::uint64_t id, int election_tick, int heartbeat_tick,
                               pro::proxy<lepton::storage_builer> &&storage);
// setRandomizedElectionTimeout set up the value by caller instead of choosing
// by system, in some test scenario we need to fill in some expected value to
// ensure the certainty
void set_randomized_election_timeout(lepton::raft &r, int election_timeout);
std::vector<std::uint64_t> ids_by_size(std::size_t size);
struct black_hole {
  lepton::leaf::result<void> step(raftpb::message &&) { return {}; }

  lepton::pb::repeated_message read_messages() { return {}; }

  void advance_messages_after_append() {}

  black_hole() = default;
};

struct connem {
  std::uint64_t from;
  std::uint64_t to;
  // 严格弱序比较
  bool operator<(const connem &other) const { return (from < other.from) || (from == other.from && to < other.to); }

  // 可选：等价性比较
  bool operator==(const connem &other) const { return (from == other.from) && (to == other.to); }
};

struct state_machine_builer_pair {
  NOT_COPYABLE(state_machine_builer_pair)
  state_machine_builer_pair() = default;
  // state_machine_builer_pair(pro::proxy<state_machine_builer> &&builder_param) : builder(std::move(builder_param)) {
  //   // builder_view = builder_param;
  // }
  // state_machine_builer_pair(pro::proxy_view<state_machine_builer> builder_param)
  //     : builder_view(builder_param), raft_handle(nullptr) {}  // 从 pro::proxy 获取视图
  state_machine_builer_pair(std::unique_ptr<lepton::raft> &&raft_handle_param)
      : raft_handle_ptr(std::move(raft_handle_param)) {
    builder_view = raft_handle_ptr.get();
    raft_handle = raft_handle_ptr.get();
  }

  state_machine_builer_pair(lepton::raft &raft_ref) {
    builder_view = pro::proxy_view<state_machine_builer>(&raft_ref);
    raft_handle = &raft_ref;
  }

  void init_black_hole_builder(pro::proxy<state_machine_builer> &&builder_param) {
    black_hole_builder = std::move(builder_param);
    builder_view = black_hole_builder;
  }
  state_machine_builer_pair(state_machine_builer_pair &&other) = default;
  pro::proxy<state_machine_builer> black_hole_builder;
  pro::proxy_view<state_machine_builer> builder_view;
  std::unique_ptr<lepton::raft> raft_handle_ptr;
  lepton::raft *raft_handle;
};

struct network {
  NOT_COPYABLE(network)
  network(std::map<std::uint64_t, state_machine_builer_pair> &&peers,
          std::map<std::uint64_t, std::unique_ptr<lepton::memory_storage>> &&storage, std::map<connem, double> &&dropm,
          std::map<raftpb::message_type, bool> &&ignorem, std::function<bool(const raftpb::message &)> msg_hook)
      : peers(std::move(peers)),
        storage(std::move(storage)),
        dropm(std::move(dropm)),
        ignorem(std::move(ignorem)),
        msg_hook(std::move(msg_hook)) {}
  network(network &&) = default;
  network &operator=(network &&) = default;

  std::map<std::uint64_t, state_machine_builer_pair> peers;
  std::map<std::uint64_t, std::unique_ptr<lepton::memory_storage>> storage;
  std::map<connem, double> dropm;
  std::map<raftpb::message_type, bool> ignorem;

  // msgHook is called for each message sent. It may inspect the
  // message and return true to send it or false to drop it.
  std::function<bool(const raftpb::message &)> msg_hook;

  void send(std::vector<raftpb::message> &&msgs);

  void drop(std::uint64_t from, std::uint64_t to, double perc) { dropm[connem{.from = from, .to = to}] = perc; }

  void cut(std::uint64_t from, std::uint64_t to) {
    drop(from, to, 2.0);  // always drop
    drop(to, from, 2.0);  // always drop
  }

  void isolate(std::uint64_t id) {
    for (std::size_t i = 0; i < peers.size(); ++i) {
      auto nid = static_cast<std::uint64_t>(i) + 1;
      if (nid != id) {
        drop(id, nid, 1.0);
        drop(nid, id, 1.0);
      }
    }
  }

  void ignore(raftpb::message_type t) { ignorem.insert({t, true}); }

  void recover() {
    dropm.clear();
    ignorem.clear();
  }

  std::vector<raftpb::message> filter(const lepton::pb::repeated_message &msgs);
};

// newNetworkWithConfig is like newNetwork but calls the given func to
// modify the configuration of any state machines it creates.
network new_network_with_config(std::function<void(lepton::config &)> config_func,
                                std::vector<state_machine_builer_pair> &&peers);

// newNetwork initializes a network from peers.
// A nil node will be replaced with a new *stateMachine.
// A *stateMachine will get its k, id.
// When using stateMachine, the address list is always [1, n].
network new_network(std::vector<state_machine_builer_pair> &&peers);

// 内存存储相关
lepton::memory_storage new_test_memory_storage(std::vector<std::function<void(lepton::memory_storage &)>> &&options);
std::function<void(lepton::memory_storage &)> with_learners(lepton::pb::repeated_uint64 &&learners);

// 状态验证
struct test_expected_raft_status {
  lepton::raft *raft_handle;
  lepton::state_type expected_state;
  std::uint64_t expected_term;
  std::uint64_t last_index;
};

void check_raft_node_after_send_msg(const std::vector<test_expected_raft_status> &tests,
                                    std::source_location loc = std::source_location::current());

// 日志操作
lepton::pb::repeated_entry next_ents(lepton::raft &r, lepton::memory_storage &s);
void must_append_entry(lepton::raft &r, lepton::pb::repeated_entry &&ents);

// Raft 实例创建
lepton::raft new_test_raft(lepton::config &&config);
lepton::raft new_test_raft(std::uint64_t id, int election_tick, int heartbeat_tick,
                           pro::proxy<lepton::storage_builer> &&storage);
lepton::raft new_test_learner_raft(std::uint64_t id, int election_tick, int heartbeat_tick,
                                   pro::proxy<lepton::storage_builer> &&storage);

// 消息构造
raftpb::message new_pb_message(std::uint64_t from, std::uint64_t to, raftpb::message_type type);
raftpb::message new_pb_message(std::uint64_t from, std::uint64_t to, std::uint64_t term, raftpb::message_type type);
raftpb::message new_pb_message(std::uint64_t from, std::uint64_t to, raftpb::message_type type, std::string data);
raftpb::message new_pb_message(std::uint64_t from, std::uint64_t to, raftpb::message_type type,
                               lepton::pb::repeated_entry &&entries);

// 特殊状态构造
using config_hook = std::function<void(lepton::config &)>;
state_machine_builer_pair ents_with_config(config_hook config_func, std::vector<std::uint64_t> &&term);
state_machine_builer_pair voted_with_config(config_hook config_func, std::uint64_t vote, std::uint64_t term);

// 配置钩子
void raft_config_quorum_hook(lepton::config &cfg);
void raft_config_pre_vote(lepton::config &cfg);
void raft_config_read_only_lease_based(lepton::config &cfg);

// Raft 钩子
void raft_quorum_hook(lepton::raft &sm);
void raft_pre_vote_hook(lepton::raft &sm);
void raft_read_only_lease_based_hook(lepton::raft &sm);
void raft_become_follower_hook(lepton::raft &sm);

// 网络初始化
network init_network(std::vector<std::uint64_t> &&ids, std::vector<config_hook> raft_config_hook = {},
                     std::vector<std::function<void(lepton::raft &)>> raft_hook = {});
network init_empty_network(std::vector<std::uint64_t> &&ids);

void emplace_nil_peer(std::vector<state_machine_builer_pair> &peers);
void emplace_nop_stepper(std::vector<state_machine_builer_pair> &peers);
#endif  // _LEPTON_TEST_RAFT_NETWORKING_H_
