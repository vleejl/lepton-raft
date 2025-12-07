#include <gtest/gtest.h>
#include <proxy.h>
#include <raft.pb.h>
#include <spdlog/spdlog.h>

#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "asio/awaitable.hpp"
#include "asio/detached.hpp"
#include "asio/error_code.hpp"
#include "asio/use_awaitable.hpp"
#include "asio/use_future.hpp"
#include "channel.h"
#include "conf_change.h"
#include "config.h"
#include "describe.h"
#include "enum_name.h"
#include "fmt/format.h"
#include "joint.h"
#include "leaf.h"
#include "lepton_error.h"
#include "log.h"
#include "majority.h"
#include "memory_storage.h"
#include "node.h"
#include "node_adapter.h"
#include "node_interface.h"
#include "protobuf.h"
#include "raft.h"
#include "raft_error.h"
#include "raft_network.h"
#include "raw_node.h"
#include "read_only.h"
#include "ready.h"
#include "state.h"
#include "storage.h"
#include "test_diff.h"
#include "test_raft_protobuf.h"
#include "test_raft_utils.h"
#include "test_utility_data.h"
#include "tracker.h"
#include "types.h"
using namespace lepton;
using namespace lepton::core;
using namespace asio::experimental::awaitable_operators;
using asio::steady_timer;

class node_adapter_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

static asio::awaitable<int> wait_leader(std::vector<std::unique_ptr<rafttest::node_adapter>> &nodes) {
  int index = -1;
  auto loop_times = 0;
  while (true) {
    loop_times++;
    if (loop_times > 9451) {
      assert(false);
    }
    // SPDLOG_INFO("loop_times:{}, waiting for leader to be elected", loop_times);
    std::set<std::uint64_t> l;
    for (std::size_t i = 0; i < nodes.size(); ++i) {
      auto status = co_await nodes[i]->status();
      auto lead = status->basic_status.soft_state.leader_id;
      if (lead != 0) {
        l.insert(lead);
        if (nodes[i]->id_ == lead) {
          // SPDLOG_INFO("leader elected: {}, loop_times: {}", nodes[i]->id_, loop_times);
          index = static_cast<int>(i);
        }
      }
    }

    if ((l.size() == 1) && (index != -1)) {
      co_return index;
    }
  }
}

static asio::awaitable<void> print_node_status(std::string hint, std::unique_ptr<rafttest::node_adapter> &node) {
  auto status = co_await node->status();
  EXPECT_TRUE(status);
  auto term = status->basic_status.hard_state.term();
  auto commit = status->basic_status.hard_state.commit();
  SPDLOG_INFO("[{}]node {}, term {}, commit index {}", hint, node->id_, term, commit);
  co_return;
}

static asio::awaitable<bool> wait_commit_converge(asio::any_io_executor executor,
                                                  std::vector<std::unique_ptr<rafttest::node_adapter>> &nodes,
                                                  std::uint64_t target) {
  auto wait_loop_times = target;
  for (std::size_t i = 0; i < wait_loop_times; ++i) {
    std::set<std::uint64_t> c;
    std::size_t good = 0;
    for (auto &node : nodes) {
      auto status = co_await node->status();
      EXPECT_TRUE(status);
      auto commit = status->basic_status.hard_state.commit();
      c.insert(commit);
      if (commit > target) {
        good++;
      }
    }
    if ((c.size() == 1) && (good == nodes.size())) {
      co_return true;
    }
    asio::steady_timer timer(executor, std::chrono::milliseconds(100));
    co_await timer.async_wait(asio::use_awaitable);
  }
  for (auto &node : nodes) {
    co_await print_node_status("final status", node);
  }
  co_return false;
}

TEST_F(node_adapter_test_suit, test_network_delay) {
  constexpr std::size_t node_count = 5;
  asio::io_context io_context;
  std::vector<std::uint64_t> node_ids;
  std::vector<lepton::core::peer> peers;
  for (std::uint64_t i = 1; i <= node_count; ++i) {
    node_ids.push_back(i);
    peers.push_back(lepton::core::peer{.ID = i, .context = std::string{}});
  }
  rafttest::raft_network nt{io_context.get_executor(), node_ids};
  std::vector<std::unique_ptr<rafttest::node_adapter>> nodes;
  for (std::uint64_t i = 1; i <= node_count; ++i) {
    nodes.emplace_back(rafttest::start_node(io_context.get_executor(), i, std::vector<lepton::core::peer>(peers),
                                            std::make_unique<rafttest::node_network>(i, &nt)));
  }

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        co_await wait_leader(nodes);

        std::size_t propose_num = 100;
        for (std::size_t i = 0; i < propose_num; ++i) {
          co_await nodes[0]->propose(io_context.get_executor(), "index " + std::to_string(i) + "  somedata");
        }

        auto result = co_await wait_commit_converge(io_context.get_executor(), nodes, propose_num);
        EXPECT_TRUE(result);

        for (auto &node : nodes) {
          co_await node->stop();
          SPDLOG_INFO("node {} stopped", node->id_);
        }
        co_return;
      },
      asio::detached);

  io_context.run();
}

TEST_F(node_adapter_test_suit, test_restart_simplify) {
  constexpr std::size_t node_count = 1;
  asio::io_context io_context;
  std::vector<std::uint64_t> node_ids;
  std::vector<lepton::core::peer> peers;
  for (std::uint64_t i = 1; i <= node_count; ++i) {
    node_ids.push_back(i);
    peers.push_back(lepton::core::peer{.ID = i, .context = std::string{}});
  }
  rafttest::raft_network nt{io_context.get_executor(), node_ids};
  std::vector<std::unique_ptr<rafttest::node_adapter>> nodes;
  for (std::uint64_t i = 1; i <= node_count; ++i) {
    nodes.emplace_back(rafttest::start_node(io_context.get_executor(), i, std::vector<lepton::core::peer>(peers),
                                            std::make_unique<rafttest::node_network>(i, &nt)));
  }

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        co_await wait_leader(nodes);

        // for (std::size_t i = 0; i < 30; ++i) {
        //   co_await nodes[0]->propose(io_context.get_executor(), "index " + std::to_string(i) + "  somedata");
        // }

        co_await nodes[0]->stop();

        nodes[0] = co_await rafttest::node_adapter::restart_node(std::move(nodes[0]), &nt);
        // for (std::size_t i = 0; i < 30; ++i) {
        //   co_await nodes[0]->propose(io_context.get_executor(), "index " + std::to_string(i) + "  somedata");
        // }

        // auto result = co_await wait_commit_converge(io_context.get_executor(), nodes, 60);
        // EXPECT_TRUE(result);

        for (auto &node : nodes) {
          co_await node->stop();
          SPDLOG_INFO("node {} stopped", node->id_);
        }
        co_return;
      },
      asio::detached);

  io_context.run();
}

TEST_F(node_adapter_test_suit, test_restart) {
  constexpr std::size_t node_count = 5;
  asio::io_context io_context;
  std::vector<std::uint64_t> node_ids;
  std::vector<lepton::core::peer> peers;
  for (std::uint64_t i = 1; i <= node_count; ++i) {
    node_ids.push_back(i);
    peers.push_back(lepton::core::peer{.ID = i, .context = std::string{}});
  }
  rafttest::raft_network nt{io_context.get_executor(), node_ids};
  std::vector<std::unique_ptr<rafttest::node_adapter>> nodes;
  for (std::uint64_t i = 1; i <= node_count; ++i) {
    nodes.emplace_back(rafttest::start_node(io_context.get_executor(), i, std::vector<lepton::core::peer>(peers),
                                            std::make_unique<rafttest::node_network>(i, &nt)));
  }

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto l_result = co_await wait_leader(nodes);
        EXPECT_GE(l_result, 0);
        auto l = static_cast<std::size_t>(l_result);

        auto k1 = static_cast<std::size_t>((l + 1) % 5);
        auto k2 = (l + 2) % 5;

        for (std::size_t i = 0; i < 30; ++i) {
          co_await nodes[l]->propose(io_context.get_executor(), "index " + std::to_string(i) + "  somedata");
        }
        co_await nodes[k1]->stop();
        for (std::size_t i = 0; i < 30; ++i) {
          co_await nodes[(l + 3) % 5]->propose(io_context.get_executor(), "index " + std::to_string(i) + "  somedata");
        }

        co_await nodes[k2]->stop();
        for (std::size_t i = 0; i < 30; ++i) {
          co_await nodes[(l + 4) % 5]->propose(io_context.get_executor(), "index " + std::to_string(i) + "  somedata");
        }
        nodes[k2] = co_await rafttest::node_adapter::restart_node(std::move(nodes[k2]), &nt);
        for (std::size_t i = 0; i < 30; ++i) {
          co_await nodes[l]->propose(io_context.get_executor(), "index " + std::to_string(i) + "  somedata");
        }
        nodes[k1] = co_await rafttest::node_adapter::restart_node(std::move(nodes[k1]), &nt);

        auto result = co_await wait_commit_converge(io_context.get_executor(), nodes, 120);
        EXPECT_TRUE(result);
        SPDLOG_INFO("[final status]node:{}, node:{} restarted", nodes[k1]->id_, nodes[k2]->id_);
        for (auto &node : nodes) {
          co_await node->stop();
          SPDLOG_INFO("node {} stopped", node->id_);
        }
        co_return;
      },
      asio::detached);

  io_context.run();
}

TEST_F(node_adapter_test_suit, test_simple_pause) {
  constexpr std::size_t node_count = 1;
  asio::io_context io_context;
  std::vector<std::uint64_t> node_ids;
  std::vector<lepton::core::peer> peers;
  for (std::uint64_t i = 1; i <= node_count; ++i) {
    node_ids.push_back(i);
    peers.push_back(lepton::core::peer{.ID = i, .context = std::string{}});
  }
  rafttest::raft_network nt{io_context.get_executor(), node_ids};
  std::vector<std::unique_ptr<rafttest::node_adapter>> nodes;
  for (std::uint64_t i = 1; i <= node_count; ++i) {
    nodes.emplace_back(rafttest::start_node(io_context.get_executor(), i, std::vector<lepton::core::peer>(peers),
                                            std::make_unique<rafttest::node_network>(i, &nt)));
  }

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto l_result = co_await wait_leader(nodes);
        EXPECT_GE(l_result, 0);
        auto l = static_cast<std::size_t>(l_result);

        std::size_t i = 0;
        for (; i < 30; ++i) {
          co_await nodes[l]->propose(io_context.get_executor(), "index " + std::to_string(i) + "  somedata");
        }
        {
          auto result = co_await wait_commit_converge(io_context.get_executor(), nodes, 30);
          EXPECT_TRUE(result);
        }

        co_await nodes[l]->pause();
        for (; i < 60; ++i) {
          co_await nodes[l]->propose(io_context.get_executor(), "index " + std::to_string(i) + "  somedata");
        }

        co_await nodes[l]->resume();
        co_await print_node_status("resume status", nodes[l]);
        {
          auto result = co_await wait_commit_converge(io_context.get_executor(), nodes, 60);
          EXPECT_TRUE(result);
        }

        for (; i < 90; ++i) {
          co_await nodes[l]->propose(io_context.get_executor(), "index " + std::to_string(i) + "  somedata");
        }
        auto result = co_await wait_commit_converge(io_context.get_executor(), nodes, 90);
        EXPECT_TRUE(result);

        for (auto &node : nodes) {
          co_await node->stop();
          SPDLOG_INFO("node {} stopped", node->id_);
        }
        co_return;
      },
      asio::detached);

  io_context.run();
}

TEST_F(node_adapter_test_suit, test_pause) {
  constexpr std::size_t node_count = 5;
  asio::io_context io_context;
  std::vector<std::uint64_t> node_ids;
  std::vector<lepton::core::peer> peers;
  for (std::uint64_t i = 1; i <= node_count; ++i) {
    node_ids.push_back(i);
    peers.push_back(lepton::core::peer{.ID = i, .context = std::string{}});
  }
  rafttest::raft_network nt{io_context.get_executor(), node_ids};
  std::vector<std::unique_ptr<rafttest::node_adapter>> nodes;
  for (std::uint64_t i = 1; i <= node_count; ++i) {
    nodes.emplace_back(rafttest::start_node(io_context.get_executor(), i, std::vector<lepton::core::peer>(peers),
                                            std::make_unique<rafttest::node_network>(i, &nt)));
  }

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto l_result = co_await wait_leader(nodes);
        EXPECT_GE(l_result, 0);
        auto l = static_cast<std::size_t>(l_result);

        auto k1 = static_cast<std::size_t>((l + 1) % 5);
        auto k2 = (l + 2) % 5;

        for (std::size_t i = 0; i < 30; ++i) {
          co_await nodes[l]->propose(io_context.get_executor(), "index " + std::to_string(i) + "  somedata");
        }

        co_await nodes[k1]->pause();
        co_await print_node_status("pause status", nodes[k1]);
        for (std::size_t i = 0; i < 30; ++i) {
          co_await nodes[(l + 3) % 5]->propose(io_context.get_executor(), "index " + std::to_string(i) + "  somedata");
        }

        co_await nodes[k2]->pause();
        co_await print_node_status("pause status", nodes[k2]);
        for (std::size_t i = 0; i < 30; ++i) {
          co_await nodes[(l + 4) % 5]->propose(io_context.get_executor(), "index " + std::to_string(i) + "  somedata");
        }

        co_await nodes[k2]->resume();
        co_await print_node_status("resume status", nodes[k2]);
        for (std::size_t i = 0; i < 30; ++i) {
          co_await nodes[l]->propose(io_context.get_executor(), "index " + std::to_string(i) + "  somedata");
        }
        co_await nodes[k1]->resume();
        co_await print_node_status("resume status", nodes[k1]);

        auto result = co_await wait_commit_converge(io_context.get_executor(), nodes, 120);
        EXPECT_TRUE(result);

        SPDLOG_INFO("[final status]leader:{}, node:{}, node:{} pause and resume", nodes[l]->id_, nodes[k1]->id_,
                    nodes[k2]->id_);
        for (auto &node : nodes) {
          co_await node->stop();
          SPDLOG_INFO("node {} stopped", node->id_);
        }
        co_return;
      },
      asio::detached);

  io_context.run();
}