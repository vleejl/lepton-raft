#ifndef _LEPTON_TEST_RAFT_NETWORKING_H_
#define _LEPTON_TEST_RAFT_NETWORKING_H_
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
lepton::config new_test_config(std::uint64_t id, int election_tick, int heartbeat_tick,
                               pro::proxy<lepton::storage_builer> &&storage);
std::unique_ptr<lepton::memory_storage> new_test_memory_storage(std::vector<test_memory_storage_options> &&options);
// setRandomizedElectionTimeout set up the value by caller instead of choosing
// by system, in some test scenario we need to fill in some expected value to
// ensure the certainty
void set_randomized_election_timeout(lepton::raft &r, int election_timeout);

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
#endif  // _LEPTON_TEST_RAFT_NETWORKING_H_
