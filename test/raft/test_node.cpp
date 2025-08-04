#include <gtest/gtest.h>
#include <proxy.h>
#include <raft.pb.h>
#include <spdlog/spdlog.h>

#include <cmath>
#include <cstddef>
#include <cstdio>
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
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "joint.h"
#include "leaf.hpp"
#include "lepton_error.h"
#include "log.h"
#include "magic_enum.hpp"
#include "majority.h"
#include "memory_storage.h"
#include "node.h"
#include "protobuf.h"
#include "raft.h"
#include "raft_error.h"
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
using asio::steady_timer;

class node_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

static node_handle new_node_test_harness(asio::any_io_executor executor, lepton::config&& config,
                                         std::vector<peer>&& peers) {
  node_handle n;
  if (!peers.empty()) {
    n = setup_node(executor, std::move(config), std::move(peers));
  } else {
    auto raw_node_result = leaf::try_handle_some(
        [&]() -> leaf::result<lepton::raw_node> {
          BOOST_LEAF_AUTO(v, new_raw_node(std::move(config)));
          return v;
        },
        [&](const lepton_error& e) -> leaf::result<lepton::raw_node> {
          LEPTON_CRITICAL(e.message);
          return new_error(e);
        });
    assert(raw_node_result);
    n = std::make_unique<node>(executor, std::move(*raw_node_result));
  }
  n->start_run();
  return n;
}

// readyWithTimeout selects from n.Ready() with a 1-second timeout. It
// panics on timeout, which is better than the indefinite wait that
// would occur if this channel were read without being wrapped in a
// select.
static asio::awaitable<lepton::ready> ready_with_timeout(asio::any_io_executor executor, lepton::node& n) {
  asio::steady_timer timeout_timer(executor, std::chrono::seconds(1));
  auto [order, msg_ec, msg_result, timer_ec] = co_await asio::experimental::make_parallel_group(
                                                   // 1. 从 channel 接收消息
                                                   [&](auto token) { return n.ready_handle().async_receive(token); },
                                                   [&](auto token) { return timeout_timer.async_wait(token); })
                                                   .async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);
  switch (order[0]) {
    case 0: {
      co_return msg_result;
    }
    case 1: {
      LEPTON_CRITICAL("timed out waiting for ready");
    }
  }
  co_return ready{};
}

// TestNodeStep ensures that node.Step sends msgProp to propc chan
// and other kinds of messages to recvc chan.
TEST_F(node_test_suit, test_node_step) {
  for (std::size_t i = 0; i < all_raftpb_message_types.size(); ++i) {
    asio::io_context io;
    auto msg_type = all_raftpb_message_types[i];
    raftpb::message msg;
    msg.set_type(msg_type);

    auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
    // auto& mm_storage = *mm_storage_ptr;
    pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
    auto raw_node_result = lepton::new_raw_node(new_test_config(1, 3, 1, std::move(storage_proxy)));
    ASSERT_TRUE(raw_node_result);
    auto& raw_node = *raw_node_result;
    lepton::node n(io.get_executor(), std::move(raw_node));
    // Proposal goes to proc chan. Others go to recvc chan.
    auto fut = asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
          auto result = co_await n.step(std::move(msg));
          EXPECT_TRUE(result) << "msg type: " << magic_enum::enum_name(msg_type);
          std::cout << "step finished, " << "msg type: " << magic_enum::enum_name(msg_type) << std::endl;
          co_return;
        },
        asio::use_future);

    auto expected_fut = asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
          if (msg_type == raftpb::message_type::MSG_PROP) {
            // Propose message should be sent to propc chan
            auto msg_result = n.prop_chan_.try_receive([&](asio::error_code _, msg_with_result msg_result) {
              EXPECT_EQ(msg_type, msg_result.msg.type()) << "msg type: " << magic_enum::enum_name(msg_type);
            });
            EXPECT_TRUE(msg_result) << "msg type: " << magic_enum::enum_name(msg_type);
          } else {
            auto msg_result = n.recv_chan_.try_receive([&](asio::error_code _, raftpb::message msg) {
              EXPECT_EQ(msg_type, msg.type()) << "msg type: " << magic_enum::enum_name(msg_type);
            });
            if (lepton::pb::is_local_msg(msg_type)) {
              EXPECT_FALSE(msg_result) << "msg type: " << magic_enum::enum_name(msg_type);
            } else {
              EXPECT_TRUE(msg_result) << "msg type: " << magic_enum::enum_name(msg_type);  // 非本地消息应该收到
            }
          }
          co_return;
        },
        asio::use_future);
    io.run();
    fut.get();
    expected_fut.get();
  }
}

// TestNodeStepUnblock should Cancel and Stop should unblock Step()
TEST_F(node_test_suit, test_node_step_unblock) {
  struct test_case {
    std::function<void(lepton::node& n, asio::io_context& io)> unblock;
    std::error_code expected_error;
  };

  // C++ 中没有context概念，所以不验证该测试case
  std::vector<test_case> tests;
  tests.push_back(test_case{[&](lepton::node& n, asio::io_context& io) {  // 用一个协程作为 "run 已启动" 的标记
                              asio::co_spawn(
                                  io,
                                  [&]() -> asio::awaitable<void> {
                                    SPDLOG_INFO("event loop has started");
                                    co_return;
                                  },
                                  asio::detached);

                              // 在下一轮事件循环中 stop()（此时 step() 应已挂起）
                              asio::post(io, [&]() {
                                SPDLOG_INFO("close done channel");
                                n.done_chan_.close();
                              });
                            },
                            lepton::make_error_code(raft_error::STOPPED)});

  for (size_t i = 0; i < tests.size(); ++i) {
    asio::io_context io;

    auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
    pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
    auto raw_node_result = lepton::new_raw_node(new_test_config(1, 3, 1, std::move(storage_proxy)));
    ASSERT_TRUE(raw_node_result);
    auto& raw_node = *raw_node_result;

    // 创建 node（未运行主 loop）
    lepton::node n(io.get_executor(), std::move(raw_node));

    const auto& tt = tests[i];

    channel<expected<void>> err_chan(io.get_executor());

    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
          raftpb::message msg;
          msg.set_type(raftpb::message_type::MSG_PROP);
          auto step_result = co_await n.step(std::move(msg));
          SPDLOG_INFO("step result has received");
          EXPECT_FALSE(step_result);
          EXPECT_EQ(tt.expected_error, step_result.error());
          co_await err_chan.async_send(asio::error_code{}, step_result, asio::use_awaitable);
          SPDLOG_INFO("finish async send error code info");
          co_return;
        },
        asio::detached);

    // 触发 unblock 操作
    tt.unblock(n, io);

    auto expected_fut = asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
          auto step_result = co_await err_chan.async_receive(asio::use_awaitable);
          EXPECT_EQ(tt.expected_error, step_result.error());
          n.stop_chan_.try_receive([&](asio::error_code ec) { EXPECT_EQ(asio::error_code{}, ec); });
          co_return;
        },
        asio::use_future);

    io.run();
    expected_fut.get();
  }
}

// TestNodePropose ensures that node.Propose sends the given proposal to the underlying raft.
TEST_F(node_test_suit, test_node_propose) {
  lepton::pb::repeated_message msgs;

  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();

  // 创建配置和节点
  auto raw_node_result = lepton::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto raw_node = *std::move(raw_node_result);

  asio::io_context io_context;
  lepton::node n(io_context.get_executor(), std::move(raw_node));

  // 启动节点运行协程
  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        SPDLOG_INFO("ready to node run.......");
        n.start_run();
        SPDLOG_INFO("node run finshed.......");
        co_return;
      },
      asio::detached);

  // 启动主测试逻辑协程
  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        // 发起竞选
        co_await n.campaign();

        // 等待成为leader
        while (true) {
          SPDLOG_INFO("main loop ......................");
          auto rd = co_await n.ready_handle().async_receive();
          if (rd.entries.size() > 0) {
            SPDLOG_INFO("Appending entries to storage");
            EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
          }

          // 检查是否成为leader
          if (rd.soft_state && rd.soft_state->leader_id == n.raw_node_.raft_.id()) {
            SPDLOG_INFO("Node {} became leader", n.raw_node_.raft_.id());
            n.raw_node_.raft_.step_func_ = [&](lepton::raft& _, raftpb::message&& msg) -> lepton::leaf::result<void> {
              SPDLOG_INFO(lepton::describe_message(msg));
              if (msg.type() == raftpb::message_type::MSG_APP_RESP) {
                return {};  // injected by (*raft).advance
              }
              msgs.Add(std::move(msg));
              return {};
            };
            co_await n.advance();
            break;
          } else {
            co_await n.advance();
          }
        }

        // 提出提案
        co_await n.propose(io_context.get_executor(), "somedata");

        // 短暂延迟确保消息处理
        asio::steady_timer timer(io_context, std::chrono::milliseconds(50));
        co_await timer.async_wait(asio::use_awaitable);

        // 停止节点
        co_await n.stop();
        SPDLOG_INFO("stop node successful.......");
        co_return;
      },
      asio::detached);

  io_context.run();

  // 验证结果
  ASSERT_EQ(1, msgs.size());
  ASSERT_EQ(magic_enum::enum_name(raftpb::message_type::MSG_PROP), magic_enum::enum_name(msgs[0].type()));
  ASSERT_EQ("somedata", msgs[0].entries(0).data());
}

// TestDisableProposalForwarding ensures that proposals are not forwarded to
// the leader when DisableProposalForwarding is true.
TEST_F(node_test_suit, test_disable_proposal_forwarding) {
  auto r1 =
      new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  auto r2 =
      new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  auto r3_cfg =
      new_test_config(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  r3_cfg.disable_proposal_forwarding = true;
  auto r3 = new_test_raft(std::move(r3_cfg));
  std::vector<state_machine_builer_pair> peers;
  peers.emplace_back(state_machine_builer_pair{r1});
  peers.emplace_back(state_machine_builer_pair{r2});
  peers.emplace_back(state_machine_builer_pair{r3});
  auto nt = new_network(std::move(peers));

  // elect r1 as leader
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // send proposal to r2(follower) where DisableProposalForwarding is false
  ASSERT_TRUE(r2.step(new_pb_message(2, 2, raftpb::message_type::MSG_PROP, "testdata")));

  // verify r2(follower) does forward the proposal when DisableProposalForwarding is false
  ASSERT_EQ(1, r2.msgs_.size());

  // send proposal to r3(follower) where DisableProposalForwarding is true
  ASSERT_FALSE(r3.step(new_pb_message(3, 3, raftpb::message_type::MSG_PROP, "testdata")));

  // verify r3(follower) does not forward the proposal when DisableProposalForwarding is true
  ASSERT_EQ(0, r3.msgs_.size());
}

// TestNodeReadIndexToOldLeader ensures that raftpb.MsgReadIndex to old leader
// gets forwarded to the new leader and 'send' method does not attach its term.
TEST_F(node_test_suit, test_node_read_index_to_old_leader) {
  auto r1 =
      new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  auto r2 =
      new_test_raft(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  auto r3_cfg =
      new_test_config(1, 10, 1, pro::make_proxy<storage_builer>(new_test_memory_storage({{with_peers({1, 2, 3})}})));
  auto r3 = new_test_raft(std::move(r3_cfg));
  std::vector<state_machine_builer_pair> peers;
  peers.emplace_back(state_machine_builer_pair{r1});
  peers.emplace_back(state_machine_builer_pair{r2});
  peers.emplace_back(state_machine_builer_pair{r3});
  auto nt = new_network(std::move(peers));

  // elect r1 as leader
  nt.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::state_type::FOLLOWER, 1, 1},
    };
    check_raft_node_after_send_msg(tests);
  }

  // send readindex request to r2(follower)
  ASSERT_TRUE(r2.step(new_pb_message(2, 2, raftpb::message_type::MSG_PROP, "testdata")));

  // verify r2(follower) does forward the proposal when DisableProposalForwarding is false
  ASSERT_EQ(1, r2.msgs_.size());
  auto read_indx_msg1 = new_pb_message(2, 1, raftpb::message_type::MSG_PROP, "testdata");
  ASSERT_EQ(read_indx_msg1.DebugString(), r2.msgs_[0].DebugString());

  // send readindex request to r3(follower)
  ASSERT_TRUE(r3.step(new_pb_message(3, 3, raftpb::message_type::MSG_PROP, "testdata")));

  // verify r3(follower) forwards this message to r1(leader) with term not set as well.
  ASSERT_EQ(1, r3.msgs_.size());
  auto read_indx_msg2 = new_pb_message(3, 1, raftpb::message_type::MSG_PROP, "testdata");
  ASSERT_EQ(read_indx_msg2.DebugString(), r3.msgs_[0].DebugString());

  // now elect r3 as leader
  nt.send({new_pb_message(3, 3, raftpb::message_type::MSG_HUP)});
  {
    std::vector<test_expected_raft_status> tests{
        {nt.peers.at(1).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::state_type::LEADER, 2, 2},
    };
    check_raft_node_after_send_msg(tests);
  }

  // let r1 steps the two messages previously we got from r2, r3
  ASSERT_TRUE(r1.step(raftpb::message{read_indx_msg1}));
  ASSERT_TRUE(r1.step(raftpb::message{read_indx_msg2}));

  // verify r1(follower) forwards these messages again to r3(new leader)
  ASSERT_EQ(2, r1.msgs_.size());
  auto read_indx_msg3 = new_pb_message(2, 3, raftpb::message_type::MSG_PROP, "testdata");
  ASSERT_EQ(read_indx_msg3.DebugString(), r1.msgs_[0].DebugString());
  read_indx_msg3 = new_pb_message(3, 3, raftpb::message_type::MSG_PROP, "testdata");
  ASSERT_EQ(read_indx_msg3.DebugString(), r1.msgs_[1].DebugString());
}

// TestNodeProposeConfig ensures that node.ProposeConfChange sends the given configuration proposal
// to the underlying raft.
TEST_F(node_test_suit, test_node_propose_config) {
  lepton::pb::repeated_message msgs;

  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();

  // 创建配置和节点
  auto raw_node_result = lepton::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto raw_node = *std::move(raw_node_result);

  asio::io_context io_context;
  lepton::node n(io_context.get_executor(), std::move(raw_node));

  // 启动节点运行协程
  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        SPDLOG_INFO("ready to node run.......");
        n.start_run();
        SPDLOG_INFO("node run finshed.......");
        co_return;
      },
      asio::detached);

  std::string msg_data;

  // 启动主测试逻辑协程
  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        // 发起竞选
        co_await n.campaign();

        // 等待成为leader
        while (true) {
          SPDLOG_INFO("main loop ......................");
          auto rd = co_await n.ready_handle().async_receive();
          if (rd.entries.size() > 0) {
            SPDLOG_INFO("Appending entries to storage");
            EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
          }

          // 检查是否成为leader
          if (rd.soft_state && rd.soft_state->leader_id == n.raw_node_.raft_.id()) {
            SPDLOG_INFO("Node {} became leader", n.raw_node_.raft_.id());
            n.raw_node_.raft_.step_func_ = [&](lepton::raft& _, raftpb::message&& msg) -> lepton::leaf::result<void> {
              SPDLOG_INFO(lepton::describe_message(msg));
              if (msg.type() == raftpb::message_type::MSG_APP_RESP) {
                return {};  // injected by (*raft).advance
              }
              msgs.Add(std::move(msg));
              return {};
            };
            co_await n.advance();
            break;
          } else {
            co_await n.advance();
          }
        }

        // 提出提案
        auto cc =
            lepton::pb::conf_change_as_v2(create_conf_change_v1(1, raftpb::conf_change_type::CONF_CHANGE_ADD_NODE));
        msg_data = cc.SerializeAsString();
        co_await n.propose_conf_change(cc);

        // 短暂延迟确保消息处理
        asio::steady_timer timer(io_context, std::chrono::milliseconds(50));
        co_await timer.async_wait(asio::use_awaitable);

        // 停止节点
        co_await n.stop();
        SPDLOG_INFO("stop node successful.......");
        co_return;
      },
      asio::detached);

  io_context.run();

  ASSERT_EQ(1, msgs.size());
  ASSERT_EQ(magic_enum::enum_name(raftpb::message_type::MSG_PROP), magic_enum::enum_name(msgs[0].type()));
  ASSERT_EQ(msg_data, msgs[0].entries(0).data());
}

// TestNodeProposeAddDuplicateNode ensures that two proposes to add the same node should
// not affect the later propose to add new node.
TEST_F(node_test_suit, test_node_propose_add_duplicate_node) {
  asio::io_context io_context;

  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();

  // 创建配置和节点
  auto cfg = new_test_config(1, 10, 1, std::move(storage_proxy));
  auto node_handle = new_node_test_harness(io_context.get_executor(), std::move(cfg), {});

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        // 发起竞选
        co_await node_handle->campaign();
        lepton::pb::repeated_entry all_committed_entries;
        asio::steady_timer timer(io_context.get_executor());
        timer.expires_after(std::chrono::milliseconds(100));
        lepton::signal_channel goroutine_stopped_chan(io_context.get_executor());
        lepton::signal_channel apply_conf_chan(io_context.get_executor());
        lepton::signal_channel cancel_chan(io_context.get_executor());

        auto rd = co_await ready_with_timeout(io_context.get_executor(), *node_handle);
        EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
        co_await node_handle->advance();

        auto running_loop = true;
        auto sub_loop = [&]() -> asio::awaitable<void> {
          while (running_loop) {
            auto [order, msg_ec, msg_result, cancel_ec, timer_ec] =
                co_await asio::experimental::make_parallel_group(
                    // 1. 从 channel 接收消息
                    [&](auto token) { return node_handle->ready_handle().async_receive(token); },
                    [&](auto token) { return cancel_chan.async_receive(token); },
                    [&](auto token) { return timer.async_wait(token); })
                    .async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);
            switch (order[0]) {
              case 0: {
                SPDLOG_INFO("receive new ready");
                SPDLOG_INFO(lepton::describe_ready(msg_result));
                EXPECT_TRUE(mm_storage.append(std::move(msg_result.entries)));
                auto applied = false;
                for (auto& entry : msg_result.committed_entries) {
                  auto entry_type = entry.type();
                  all_committed_entries.Add(raftpb::entry{entry});
                  switch (entry_type) {
                    case raftpb::ENTRY_CONF_CHANGE: {
                      raftpb::conf_change cc;
                      EXPECT_TRUE(cc.ParseFromString(entry.data()));
                      co_await node_handle->apply_conf_change(lepton::pb::conf_change_var_as_v2(cc));
                      applied = true;
                      break;
                    }
                    default:
                      break;
                  }
                }
                co_await node_handle->advance();
                if (applied) {
                  co_await apply_conf_chan.async_send(asio::error_code{});
                }
                break;
              }
              case 1: {
                running_loop = false;
                break;
              }
              case 2: {
                SPDLOG_INFO("node is ticking");
                node_handle->tick();
                timer.expires_after(std::chrono::milliseconds(100));
                break;
              }
            }
          }
          co_await node_handle->stop();
          co_await goroutine_stopped_chan.async_send(asio::error_code{});
        };
        SPDLOG_INFO("[main logic]ready to start sub loop");
        asio::co_spawn(
            io_context,
            [&]() -> asio::awaitable<void> {
              co_await sub_loop();
              co_return;
            },
            asio::detached);
        SPDLOG_INFO("[main logic]start sub loop successful");

        raftpb::conf_change cc1;
        cc1.set_type(::raftpb::conf_change_type::CONF_CHANGE_ADD_NODE);
        cc1.set_node_id(1);
        auto ccdata1 = cc1.SerializeAsString();
        SPDLOG_INFO("[main logic]first times ready to propose conf change");
        co_await node_handle->propose_conf_change(cc1);
        SPDLOG_INFO("[main logic]first times propose conf change success");
        co_await apply_conf_chan.async_receive();
        SPDLOG_INFO("[main logic]first times apply conf change success");

        // try add the same node again
        co_await node_handle->propose_conf_change(cc1);
        co_await apply_conf_chan.async_receive();
        SPDLOG_INFO("[main logic]first times try add the same node again...");

        // the new node join should be ok
        raftpb::conf_change cc2;
        cc2.set_type(::raftpb::conf_change_type::CONF_CHANGE_ADD_NODE);
        cc2.set_node_id(2);
        auto ccdata2 = cc2.SerializeAsString();
        co_await node_handle->propose_conf_change(cc2);
        co_await apply_conf_chan.async_receive();
        SPDLOG_INFO("[main logic]second times propose conf change success...");

        co_await cancel_chan.async_send(asio::error_code{});
        co_await node_handle->stop();
        co_await goroutine_stopped_chan.async_receive();
        SPDLOG_INFO("[main logic]sub loop has finished");

        EXPECT_EQ(4, all_committed_entries.size());
        EXPECT_EQ(ccdata1, all_committed_entries[1].data());
        EXPECT_EQ(ccdata2, all_committed_entries[3].data());
        co_return;
      },
      asio::detached);
  io_context.run();
}

// TestBlockProposal ensures that node will block proposal when it does not
// know who is the current leader; node will accept proposal when it knows
// who is the current leader.
TEST_F(node_test_suit, test_block_proposal) {
  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();

  // 创建配置和节点
  auto raw_node_result = lepton::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto raw_node = *std::move(raw_node_result);

  asio::io_context io_context;
  lepton::node n(io_context.get_executor(), std::move(raw_node));

  n.start_run();
  lepton::channel<std::error_code> err_chan(io_context.get_executor());
  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto result = co_await n.propose(io_context.get_executor(), "somedata");
        if (result.has_value()) {
          co_await err_chan.async_send(asio::error_code{}, std::error_code{});
        } else {
          co_await err_chan.async_send(asio::error_code{}, result.error());
        }
        co_return;
      },
      asio::detached);

  ASSERT_FALSE(err_chan.try_receive(
      [&](asio::error_code _, std::error_code ec) { EXPECT_TRUE(ec) << "unexpected case, want blocking"; }));

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        co_await n.campaign();
        auto rd = co_await n.ready_handle().async_receive();
        EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
        co_await n.advance();
        asio::steady_timer timeout_timer(io_context.get_executor(), std::chrono::seconds(10));
        auto [order, msg_ec, msg_result, timer_ec] =
            co_await asio::experimental::make_parallel_group(
                // 1. 从 channel 接收消息
                [&](auto token) { return err_chan.async_receive(token); },
                [&](auto token) { return timeout_timer.async_wait(token); })
                .async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);
        switch (order[0]) {
          case 0: {
            EXPECT_EQ(std::error_code{}, msg_result);
            break;
          }
          case 1: {
            EXPECT_FALSE(true) << "blocking proposal, want unblocking";
            break;
          }
        }
        co_await n.stop();
        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(node_test_suit, test_node_propose_wait_dropped) {
  lepton::pb::repeated_message msgs;

  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();

  // 创建配置和节点
  auto raw_node_result = lepton::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto raw_node = *std::move(raw_node_result);

  asio::io_context io_context;
  lepton::node n(io_context.get_executor(), std::move(raw_node));

  // 启动节点运行协程
  n.start_run();

  // 启动主测试逻辑协程
  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        // 发起竞选
        co_await n.campaign();

        // 等待成为leader
        while (true) {
          SPDLOG_INFO("main loop ......................");
          auto rd = co_await n.ready_handle().async_receive();
          if (rd.entries.size() > 0) {
            SPDLOG_INFO("Appending entries to storage");
            EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
          }

          // 检查是否成为leader
          if (rd.soft_state && rd.soft_state->leader_id == n.raw_node_.raft_.id()) {
            SPDLOG_INFO("Node {} became leader", n.raw_node_.raft_.id());
            n.raw_node_.raft_.step_func_ = [&](lepton::raft& _, raftpb::message&& msg) -> lepton::leaf::result<void> {
              SPDLOG_INFO(lepton::describe_message(msg));
              if ((msg.type() == raftpb::message_type::MSG_PROP) &&
                  (msg.DebugString().find("test_dropping")) != std::string::npos) {
                return new_error(lepton::raft_error::PROPOSAL_DROPPED);
              }
              if (msg.type() == raftpb::message_type::MSG_APP_RESP) {
                // This is produced by raft internally, see (*raft).advance.
                return {};
              }
              msgs.Add(std::move(msg));
              return {};
            };
            co_await n.advance();
            break;
          } else {
            co_await n.advance();
          }
        }

        // 提出提案
        auto propose_result = co_await n.propose(io_context.get_executor(), "test_dropping");
        EXPECT_FALSE(propose_result.has_value());
        EXPECT_EQ(lepton::make_error_code(lepton::raft_error::PROPOSAL_DROPPED), propose_result.error());

        // 停止节点
        co_await n.stop();
        SPDLOG_INFO("stop node successful.......");
        co_return;
      },
      asio::detached);

  io_context.run();

  // 验证结果
  ASSERT_EQ(0, msgs.size());
}

// TestNodeTick ensures that node.Tick() will increase the
// elapsed of the underlying raft state machine.
TEST_F(node_test_suit, test_node_tick) {
  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();

  // 创建配置和节点
  auto raw_node_result = lepton::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto raw_node = *std::move(raw_node_result);

  asio::io_context io_context;
  lepton::node n(io_context.get_executor(), std::move(raw_node));

  // 启动节点运行协程
  n.start_run();
  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        n.tick();
        const auto elapsed = n.raw_node_.raft_.election_elapsed_;
        asio::steady_timer timer(io_context, std::chrono::milliseconds(100));
        while (elapsed + 1 != n.raw_node_.raft_.election_elapsed_) {
          SPDLOG_INFO("current elapsed:{}, election_elapsed_:{}", elapsed, n.raw_node_.raft_.election_elapsed_);
          co_await timer.async_wait(asio::use_awaitable);
          timer.expires_after(std::chrono::milliseconds(100));
        }
        co_await n.stop();
        co_return;
      },
      asio::use_future);

  io_context.run();
}