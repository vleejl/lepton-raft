#include <gtest/gtest.h>
#include <proxy.h>
#include <raft.pb.h>
#include <spdlog/spdlog.h>

#include <chrono>
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
#include "basic/enum_name.h"
#include "basic/log.h"
#include "coroutine/channel.h"
#include "error/coro_error.h"
#include "error/leaf.h"
#include "error/lepton_error.h"
#include "error/raft_error.h"
#include "fmt/format.h"
#include "raft_core/config.h"
#include "raft_core/describe.h"
#include "raft_core/memory_storage.h"
#include "raft_core/node.h"
#include "raft_core/node_interface.h"
#include "raft_core/pb/conf_change.h"
#include "raft_core/pb/protobuf.h"
#include "raft_core/pb/types.h"
#include "raft_core/quorum/joint.h"
#include "raft_core/quorum/majority.h"
#include "raft_core/raft.h"
#include "raft_core/raw_node.h"
#include "raft_core/read_only.h"
#include "raft_core/ready.h"
#include "raft_core/storage.h"
#include "raft_core/tracker/state.h"
#include "raft_core/tracker/tracker.h"
#include "test_diff.h"
#include "test_raft_protobuf.h"
#include "test_raft_utils.h"
#include "test_utility_data.h"
#include "v4/proxy.h"
using namespace lepton;
using namespace lepton::core;
using namespace asio::experimental::awaitable_operators;
using asio::steady_timer;

class node_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

static node_handle new_node_test_harness(asio::any_io_executor executor, lepton::core::config&& config,
                                         std::vector<peer>&& peers) {
  node_handle n;
  if (!peers.empty()) {
    n = setup_node(executor, std::move(config), std::move(peers));
  } else {
    auto raw_node_result = leaf::try_handle_some(
        [&]() -> leaf::result<lepton::core::raw_node> {
          BOOST_LEAF_AUTO(v, new_raw_node(std::move(config)));
          return v;
        },
        [&](const lepton_error& e) -> leaf::result<lepton::core::raw_node> {
          LEPTON_CRITICAL(e.message);
          return new_error(e);
        });
    assert(raw_node_result);
    n = std::make_unique<node>(executor, std::move(*raw_node_result));
  }
  n->start_run();
  return n;
}

static asio::awaitable<lepton::core::ready_handle> ready_with_timeout(asio::any_io_executor executor,
                                                                      lepton::core::node& n,
                                                                      std::chrono::nanoseconds timeout) {
  asio::steady_timer timeout_timer(executor, timeout);
  auto result = co_await (n.wait_ready(executor) || timeout_timer.async_wait(asio::as_tuple(asio::use_awaitable)));
  switch (result.index()) {
    case 0: {
      auto recv_result = std::get<0>(result);
      co_return recv_result.value();
    }
    case 1: {
      LEPTON_CRITICAL("timed out waiting for ready");
    }
  }
  co_return nullptr;
}

static asio::awaitable<lepton::core::ready_handle> ready_with_timeout(asio::any_io_executor executor,
                                                                      lepton::core::node& n) {
  auto result = co_await ready_with_timeout(executor, n, std::chrono::seconds(1));
  co_return result;
}

asio::awaitable<void> expect_wait_timeout_async_receive_ready(asio::any_io_executor executor,
                                                              pro::proxy_view<node_builder> n) {
  asio::steady_timer timeout_timer(executor, std::chrono::milliseconds(1));
  auto has_wait_timeout = false;
  auto result = co_await (n->wait_ready(executor) || timeout_timer.async_wait(asio::as_tuple(asio::use_awaitable)));
  switch (result.index()) {
    case 0: {
      EXPECT_FALSE(true);
      break;
    }
    case 1: {
      has_wait_timeout = true;
      SPDLOG_INFO("timed out waiting for ready");
      break;
    }
  }
  EXPECT_TRUE(has_wait_timeout);
  co_return;
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
    auto raw_node_result = lepton::core::new_raw_node(new_test_config(1, 3, 1, std::move(storage_proxy)));
    ASSERT_TRUE(raw_node_result);
    auto& raw_node = *raw_node_result;
    lepton::core::node n(io.get_executor(), std::move(raw_node));
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
            auto msg_result =
                n.prop_chan_.raw_channel().try_receive([&](asio::error_code _, msg_with_result msg_result) {
                  EXPECT_EQ(msg_type, msg_result.msg.type()) << "msg type: " << magic_enum::enum_name(msg_type);
                });
            EXPECT_TRUE(msg_result) << "msg type: " << magic_enum::enum_name(msg_type);
          } else {
            auto msg_result = n.recv_chan_.raw_channel().try_receive([&](asio::error_code _, raftpb::message msg) {
              EXPECT_EQ(msg_type, msg.type()) << "msg type: " << magic_enum::enum_name(msg_type);
            });
            if (lepton::core::pb::is_local_msg(msg_type)) {
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
    std::function<void(lepton::core::node& n, asio::io_context& io)> unblock;
    std::error_code expected_error;
  };

  // C++ 中没有context概念，所以不验证该测试case
  std::vector<test_case> tests;
  tests.push_back(test_case{[&](lepton::core::node& n, asio::io_context& io) {  // 用一个协程作为 "run 已启动" 的标记
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
                                n.prop_chan_.close();
                              });
                            },
                            lepton::make_error_code(coro_error::STOPPED)});

  for (size_t i = 0; i < tests.size(); ++i) {
    asio::io_context io;

    auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
    pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
    auto raw_node_result = lepton::core::new_raw_node(new_test_config(1, 3, 1, std::move(storage_proxy)));
    ASSERT_TRUE(raw_node_result);
    auto& raw_node = *raw_node_result;

    // 创建 node（未运行主 loop）
    lepton::core::node n(io.get_executor(), std::move(raw_node));

    const auto& tt = tests[i];

    lepton::coro::channel<expected<void>> err_chan(io.get_executor());

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
  lepton::core::pb::repeated_message msgs;

  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();

  // 创建配置和节点
  auto raw_node_result = lepton::core::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto raw_node = *std::move(raw_node_result);

  asio::io_context io_context;
  lepton::core::node n(io_context.get_executor(), std::move(raw_node));

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
          SPDLOG_INFO("waiting become leader loop ......................");
          auto rd_handle_result = co_await n.wait_ready(io_context.get_executor());
          EXPECT_TRUE(rd_handle_result);
          auto rd_handle = rd_handle_result.value();
          auto& rd = *rd_handle.get();

          SPDLOG_INFO("receive raft raedy and ready to apply, {}", describe_ready(rd, nullptr));
          if (rd.entries.size() > 0) {
            SPDLOG_INFO("Appending entries to storage");
            EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
          }

          // 检查是否成为leader
          if (rd.soft_state && rd.soft_state->leader_id == n.raw_node_.raft_.id()) {
            SPDLOG_INFO("Node {} became leader", n.raw_node_.raft_.id());
            n.raw_node_.raft_.step_func_ = [&](lepton::core::raft& _,
                                               raftpb::message&& msg) -> lepton::leaf::result<void> {
              SPDLOG_INFO(lepton::core::describe_message(msg, nullptr));
              if (msg.type() == raftpb::message_type::MSG_APP_RESP) {
                return {};  // injected by (*raft).advance
              }
              msgs.Add(std::move(msg));
              return {};
            };
            co_await n.advance();
            break;
          } else {
            SPDLOG_INFO("Node {} is not leader, continue wait...", n.raw_node_.raft_.id());
            co_await n.advance();
          }
        }

        // 提出提案
        co_await n.propose(io_context.get_executor(), "somedata");
        SPDLOG_INFO("propose some data successful");

        // 短暂延迟确保消息处理
        asio::steady_timer timer(io_context, std::chrono::milliseconds(50));
        co_await timer.async_wait(asio::use_awaitable);
        SPDLOG_INFO("wait timeout and ready to stop node");
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
        {nt.peers.at(1).raft_handle, lepton::core::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::core::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::core::state_type::FOLLOWER, 1, 1},
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
        {nt.peers.at(1).raft_handle, lepton::core::state_type::LEADER, 1, 1},
        {nt.peers.at(2).raft_handle, lepton::core::state_type::FOLLOWER, 1, 1},
        {nt.peers.at(3).raft_handle, lepton::core::state_type::FOLLOWER, 1, 1},
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
        {nt.peers.at(1).raft_handle, lepton::core::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(2).raft_handle, lepton::core::state_type::FOLLOWER, 2, 2},
        {nt.peers.at(3).raft_handle, lepton::core::state_type::LEADER, 2, 2},
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
  lepton::core::pb::repeated_message msgs;

  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();

  // 创建配置和节点
  auto raw_node_result = lepton::core::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto raw_node = *std::move(raw_node_result);

  asio::io_context io_context;
  lepton::core::node n(io_context.get_executor(), std::move(raw_node));

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
          auto rd_handle_result = co_await n.wait_ready(io_context.get_executor());
          EXPECT_TRUE(rd_handle_result);
          auto rd_handle = rd_handle_result.value();
          auto& rd = *rd_handle.get();
          if (rd.entries.size() > 0) {
            SPDLOG_INFO("Appending entries to storage");
            EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
          }

          // 检查是否成为leader
          if (rd.soft_state && rd.soft_state->leader_id == n.raw_node_.raft_.id()) {
            SPDLOG_INFO("Node {} became leader", n.raw_node_.raft_.id());
            n.raw_node_.raft_.step_func_ = [&](lepton::core::raft& _,
                                               raftpb::message&& msg) -> lepton::leaf::result<void> {
              SPDLOG_INFO(lepton::core::describe_message(msg, nullptr));
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
        auto cc = lepton::core::pb::conf_change_as_v2(
            create_conf_change_v1(1, raftpb::conf_change_type::CONF_CHANGE_ADD_NODE));
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
        lepton::core::pb::repeated_entry all_committed_entries;
        asio::steady_timer timer(io_context.get_executor());
        timer.expires_after(std::chrono::milliseconds(100));
        lepton::coro::signal_channel goroutine_stopped_chan(io_context.get_executor());
        lepton::coro::signal_channel apply_conf_chan(io_context.get_executor());
        lepton::coro::signal_channel cancel_chan(io_context.get_executor());

        auto rd_result = co_await ready_with_timeout(io_context.get_executor(), *node_handle);
        auto& rd = *rd_result;
        SPDLOG_INFO("mm_storage first index:{}, last index:{}, entry size:{}", mm_storage.first_index().value(),
                    mm_storage.last_index().value(), rd.entries.size());
        EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
        SPDLOG_INFO("mm_storage first index:{}, last index:{}", mm_storage.first_index().value(),
                    mm_storage.last_index().value());
        co_await node_handle->advance();

        auto running_loop = true;
        auto sub_loop = [&]() -> asio::awaitable<void> {
          while (running_loop) {
            auto result = co_await (cancel_chan.async_receive(asio::as_tuple(asio::use_awaitable)) ||
                                    timer.async_wait(asio::as_tuple(asio::use_awaitable)) ||
                                    node_handle->wait_ready(io_context.get_executor()));
            switch (result.index()) {
              case 0: {
                SPDLOG_INFO("receive cancel siganl and ready to stop running loop");
                running_loop = false;
                break;
              }
              case 1: {
                SPDLOG_INFO("node is ticking");
                node_handle->tick();
                timer.expires_after(std::chrono::milliseconds(100));
                break;
              }
              case 2: {
                SPDLOG_INFO("receive new ready");
                auto msg_result = std::get<2>(result);
                auto& rd = *msg_result.value();
                SPDLOG_INFO(lepton::core::describe_ready(rd, nullptr));
                SPDLOG_INFO("mm_storage first index:{}, last index:{}, entry size:{}", mm_storage.first_index().value(),
                            mm_storage.last_index().value(), rd.entries.size());
                EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
                SPDLOG_INFO("mm_storage first index:{}, last index:{}", mm_storage.first_index().value(),
                            mm_storage.last_index().value());
                auto applied = false;
                for (auto& entry : rd.committed_entries) {
                  auto entry_type = entry.type();
                  all_committed_entries.Add(raftpb::entry{entry});
                  switch (entry_type) {
                    case raftpb::ENTRY_CONF_CHANGE: {
                      raftpb::conf_change cc;
                      EXPECT_TRUE(cc.ParseFromString(entry.data()));
                      co_await node_handle->apply_conf_change(lepton::core::pb::conf_change_var_as_v2(cc));
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
            }
          }
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
        SPDLOG_INFO("[main logic]cancel_chan send cancel signal success...");
        co_await goroutine_stopped_chan.async_receive();
        co_await node_handle->stop();
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
  auto raw_node_result = lepton::core::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto raw_node = *std::move(raw_node_result);

  asio::io_context io_context;
  lepton::core::node n(io_context.get_executor(), std::move(raw_node));

  n.start_run();
  lepton::coro::channel<std::error_code> err_chan(io_context.get_executor());
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
        auto rd_handle_result = co_await n.wait_ready(io_context.get_executor());
        EXPECT_TRUE(rd_handle_result);
        auto rd_handle = rd_handle_result.value();
        auto& rd = *rd_handle.get();
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
  lepton::core::pb::repeated_message msgs;

  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();

  // 创建配置和节点
  auto raw_node_result = lepton::core::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto raw_node = *std::move(raw_node_result);

  asio::io_context io_context;
  lepton::core::node n(io_context.get_executor(), std::move(raw_node));

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
          auto rd_handle_result = co_await n.wait_ready(io_context.get_executor());
          EXPECT_TRUE(rd_handle_result);
          auto rd_handle = rd_handle_result.value();
          auto& rd = *rd_handle.get();
          if (rd.entries.size() > 0) {
            SPDLOG_INFO("Appending entries to storage");
            EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
          }

          // 检查是否成为leader
          if (rd.soft_state && rd.soft_state->leader_id == n.raw_node_.raft_.id()) {
            SPDLOG_INFO("Node {} became leader", n.raw_node_.raft_.id());
            n.raw_node_.raft_.step_func_ = [&](lepton::core::raft& _,
                                               raftpb::message&& msg) -> lepton::leaf::result<void> {
              SPDLOG_INFO(lepton::core::describe_message(msg, nullptr));
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
  auto raw_node_result = lepton::core::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto raw_node = *std::move(raw_node_result);

  asio::io_context io_context;
  lepton::core::node n(io_context.get_executor(), std::move(raw_node));

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

TEST_F(node_test_suit, test_node_simple_stop) {
  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();

  // 创建配置和节点
  auto raw_node_result = lepton::core::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto raw_node = *std::move(raw_node_result);

  asio::io_context io_context;
  lepton::core::node n(io_context.get_executor(), std::move(raw_node));

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        SPDLOG_INFO("ready to listen stop node.......");
        co_await n.listen_stop();
        SPDLOG_INFO("node has stopped.......");
        co_return;
      },
      asio::detached);

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        SPDLOG_INFO("ready to stop node.......");
        co_await n.stop();
        SPDLOG_INFO("stop node successfully.......");
        co_return;
      },
      asio::use_future);
  io_context.run();
}

// TestNodeStop ensures that node.Stop() blocks until the node has stopped
// processing, and that it is idempotent
TEST_F(node_test_suit, test_node_stop) {
  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();

  // 创建配置和节点
  auto raw_node_result = lepton::core::new_raw_node(new_test_config(1, 10, 1, std::move(storage_proxy)));
  ASSERT_TRUE(raw_node_result);
  auto raw_node = *std::move(raw_node_result);

  asio::io_context io_context;
  lepton::core::node n(io_context.get_executor(), std::move(raw_node));

  lepton::coro::signal_channel done_chann{io_context.get_executor()};

  // 启动节点运行协程
  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        SPDLOG_INFO("ready to node run.......");
        co_await n.run();
        co_await done_chann.async_send(asio::error_code{});
        SPDLOG_INFO("node run finshed.......");
        co_return;
      },
      asio::detached);

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto running_status_result = co_await n.status();
        EXPECT_TRUE(running_status_result);
        auto& running_status = *running_status_result;
        SPDLOG_INFO("ready to stop node.......");
        co_await n.stop();
        SPDLOG_INFO("stop node successfully.......");

        asio::steady_timer timeout_timer(io_context.get_executor(), std::chrono::seconds(1));
        auto [order, msg_ec, timer_ec] = co_await asio::experimental::make_parallel_group(
                                             // 1. 从 channel 接收消息
                                             [&](auto token) { return done_chann.async_receive(token); },
                                             [&](auto token) { return timeout_timer.async_wait(token); })
                                             .async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);
        switch (order[0]) {
          case 0: {
            break;
          }
          case 1: {
            EXPECT_FALSE(true) << "timed out waiting for node to stop!";
            break;
          }
        }
        lepton::core::status empty_status{};
        EXPECT_FALSE(compare_status(empty_status, running_status));
        // Further status should return be empty, the node is stopped.
        auto stop_status_result = co_await n.status();
        EXPECT_FALSE(stop_status_result);

        // Subsequent Stops should have no effect.
        co_await n.stop();
        co_return;
      },
      asio::use_future);
  io_context.run();
}

// TestNodeStart ensures that a node can be started correctly. The node should
// start with correct configuration change entries, and can accept and commit
// proposals.
TEST_F(node_test_suit, test_node_start) {
  auto cc = create_conf_change_v1(1, raftpb::conf_change_type::CONF_CHANGE_ADD_NODE);
  auto ccdata = cc.SerializeAsString();
  std::vector<lepton::core::ready> wants;
  {
    lepton::core::ready ready;
    ready.hard_state.set_term(1);
    ready.hard_state.set_commit(1);
    raftpb::entry entry;
    entry.set_type(::raftpb::entry_type::ENTRY_CONF_CHANGE);
    entry.set_term(1);
    entry.set_index(1);
    entry.set_data(cc.SerializeAsString());
    ready.entries.Add(raftpb::entry{entry});
    ready.committed_entries.Add(raftpb::entry{entry});
    ready.must_sync = true;
    wants.push_back(std::move(ready));
  }
  {
    // 第二个 Ready
    lepton::core::ready ready;
    ready.hard_state.set_term(2);
    ready.hard_state.set_commit(2);
    ready.hard_state.set_vote(1);

    // Entries (index=3)
    raftpb::entry new_entry;
    new_entry.set_term(2);
    new_entry.set_index(3);
    new_entry.set_data("foo");  // 直接设置二进制数据

    // CommittedEntries (index=2)
    raftpb::entry committed_entry;
    committed_entry.set_term(2);
    committed_entry.set_index(2);
    // 默认类型为 ENTRY_NORMAL, data 为空

    ready.entries.Add(std::move(new_entry));
    ready.committed_entries.Add(std::move(committed_entry));
    ready.must_sync = true;
    wants.push_back(std::move(ready));
  }
  {
    // 第三个 Ready
    lepton::core::ready ready;
    ready.hard_state.set_term(2);
    ready.hard_state.set_commit(3);
    ready.hard_state.set_vote(1);

    // CommittedEntries (index=3)
    raftpb::entry committed_entry;
    committed_entry.set_term(2);
    committed_entry.set_index(3);
    committed_entry.set_data("foo");

    ready.committed_entries.Add(std::move(committed_entry));
    ready.must_sync = false;
    wants.push_back(std::move(ready));
  }

  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();

  // 创建配置和节点
  auto config = new_test_config(1, 10, 1, std::move(storage_proxy));
  asio::io_context io_context;
  auto n = lepton::core::start_node(io_context.get_executor(), std::move(config), {lepton::core::peer{.ID = 1}});
  // auto& r = n->raw_node_.raft_;

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        {
          auto rd_handle_result = co_await n->wait_ready(io_context.get_executor());
          EXPECT_TRUE(rd_handle_result);
          auto rd_handle = rd_handle_result.value();
          auto& rd = *rd_handle.get();
          SPDLOG_INFO("receive raft raedy and ready to apply, {}", describe_ready(rd, nullptr));
          SPDLOG_INFO("mm_storage first index:{}, last index:{}, entry size:{}", mm_storage.first_index().value(),
                      mm_storage.last_index().value(), rd.entries.size());
          EXPECT_TRUE(compare_ready(wants[0], rd));
          EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
          SPDLOG_INFO("mm_storage first index:{}, last index:{}", mm_storage.first_index().value(),
                      mm_storage.last_index().value());
          co_await n->advance();
          // SPDLOG_INFO("[raft status]state:{}, committed: {}, term:{}", magic_enum::enum_name(r.state_type_),
          //             r.raft_log_handle().committed(), r.term());
          // EXPECT_EQ(lepton::core::state_type::FOLLOWER, r.state_type_);
          // EXPECT_EQ(1, r.raft_log_handle().committed());
          // EXPECT_EQ(1, r.term());
        }

        SPDLOG_INFO("finish step 1.............");

        {
          auto result = co_await n->campaign();
          EXPECT_TRUE(result);
          SPDLOG_INFO("node send campaign msg successful");
          // EXPECT_EQ(lepton::core::state_type::CANDIDATE, r.state_type_);
          // EXPECT_EQ(1, r.raft_log_handle().committed());
          // EXPECT_EQ(2, r.term());
          // SPDLOG_INFO("[raft status]state:{}, committed: {}, term:{}", magic_enum::enum_name(r.state_type_),
          //             r.raft_log_handle().committed(), r.term());
        }

        // Persist vote.
        {  // sfot state change: follower -> candidate
          SPDLOG_INFO("try to async_receive from ready handle");
          auto rd_handle_result = co_await n->wait_ready(io_context.get_executor());
          EXPECT_TRUE(rd_handle_result);
          auto rd_handle = rd_handle_result.value();
          auto& rd = *rd_handle.get();
          SPDLOG_INFO("receive raft raedy and ready to apply, {}", describe_ready(rd, nullptr));
          SPDLOG_INFO("mm_storage first index:{}, last index:{}, entry size:{}", mm_storage.first_index().value(),
                      mm_storage.last_index().value(), rd.entries.size());
          EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
          SPDLOG_INFO("mm_storage first index:{}, last index:{}", mm_storage.first_index().value(),
                      mm_storage.last_index().value());
          co_await n->advance();
          // SPDLOG_INFO("[raft status]state:{}, committed: {}, term:{}", magic_enum::enum_name(r.state_type_),
          //             r.raft_log_handle().committed(), r.term());
          // EXPECT_EQ(lepton::core::state_type::CANDIDATE, r.state_type_);
          // EXPECT_EQ(1, r.raft_log_handle().committed());
          // EXPECT_EQ(2, r.term());
        }
        {  // sfot state change: candidate -> leader
          // Append empty entry.
          SPDLOG_INFO("try to async_receive from ready handle again");
          auto rd_handle_result = co_await n->wait_ready(io_context.get_executor());
          EXPECT_TRUE(rd_handle_result);
          auto rd_handle = rd_handle_result.value();
          auto& rd = *rd_handle.get();
          SPDLOG_INFO("receive raft raedy and ready to apply, {}", describe_ready(rd, nullptr));
          SPDLOG_INFO("mm_storage first index:{}, last index:{}, entry size:{}", mm_storage.first_index().value(),
                      mm_storage.last_index().value(), rd.entries.size());
          EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
          SPDLOG_INFO("mm_storage first index:{}, last index:{}", mm_storage.first_index().value(),
                      mm_storage.last_index().value());
          co_await n->advance();
          // SPDLOG_INFO("[raft status]state:{}, committed: {}, term:{}", magic_enum::enum_name(r.state_type_),
          //             r.raft_log_handle().committed(), r.term());
          // EXPECT_EQ(lepton::core::state_type::LEADER, r.state_type_);
          // EXPECT_EQ(1, r.raft_log_handle().committed());
          // EXPECT_EQ(2, r.term());
        }

        SPDLOG_INFO("finish step 2.............");

        co_await n->propose("foo");

        {
          SPDLOG_INFO("try to async_receive from ready handle");
          auto rd_handle_result = co_await n->wait_ready(io_context.get_executor());
          EXPECT_TRUE(rd_handle_result);
          auto rd_handle = rd_handle_result.value();
          auto& rd = *rd_handle.get();
          SPDLOG_INFO("receive raft raedy and ready to apply, {}", describe_ready(rd, nullptr));
          SPDLOG_INFO("mm_storage first index:{}, last index:{}, entry size:{}", mm_storage.first_index().value(),
                      mm_storage.last_index().value(), rd.entries.size());
          EXPECT_TRUE(compare_ready(wants[1], rd)) << describe_ready(wants[1], nullptr) << "\n"
                                                   << describe_ready(rd, nullptr);
          EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
          SPDLOG_INFO("mm_storage first index:{}, last index:{}", mm_storage.first_index().value(),
                      mm_storage.last_index().value());
          co_await n->advance();
        }

        SPDLOG_INFO("finish step 3.............");

        {
          auto rd_handle_result = co_await n->wait_ready(io_context.get_executor());
          EXPECT_TRUE(rd_handle_result);
          auto rd_handle = rd_handle_result.value();
          auto& rd = *rd_handle.get();
          SPDLOG_INFO("receive raft raedy and ready to apply, content {}", describe_ready(rd, nullptr));
          SPDLOG_INFO("mm_storage first index:{}, last index:{}, entry size:{}", mm_storage.first_index().value(),
                      mm_storage.last_index().value(), rd.entries.size());
          EXPECT_TRUE(compare_ready(wants[2], rd));
          EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
          SPDLOG_INFO("mm_storage first index:{}, last index:{}", mm_storage.first_index().value(),
                      mm_storage.last_index().value());
          co_await n->advance();
        }

        SPDLOG_INFO("finish step 4.............");

        co_await expect_wait_timeout_async_receive_ready(io_context.get_executor(), n);
        co_await n->stop();

        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(node_test_suit, test_node_restart) {
  lepton::core::pb::repeated_entry entries;
  {
    auto entry = entries.Add();
    entry->set_term(1);
    entry->set_index(1);
  }
  {
    auto entry = entries.Add();
    entry->set_term(1);
    entry->set_index(2);
    entry->set_data("foo");
  }
  raftpb::hard_state hs;
  hs.set_term(1);
  hs.set_commit(1);

  ready want_rd;
  // No HardState is emitted because there was no change.
  want_rd.hard_state = raftpb::hard_state{};
  // commit up to index commit index in st
  want_rd.committed_entries =
      lepton::core::pb::repeated_entry{entries.begin(), entries.begin() + static_cast<int>(hs.commit())};
  ASSERT_EQ(1, want_rd.committed_entries.size());
  // MustSync is false because no HardState or new entries are provided.
  want_rd.must_sync = false;

  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  mm_storage.set_hard_state(std::move(hs));
  ASSERT_TRUE(mm_storage.append(std::move(entries)));

  asio::io_context io_context;
  auto n = lepton::core::restart_node(io_context.get_executor(), new_test_config(1, 10, 1, std::move(storage_proxy)));

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto rd_result = co_await n->wait_ready(io_context.get_executor());
        EXPECT_TRUE(rd_result);
        auto& rd = *rd_result.value();
        EXPECT_TRUE(compare_ready(want_rd, rd));
        co_await n->advance();

        co_await expect_wait_timeout_async_receive_ready(io_context.get_executor(), n);
        co_await n->stop();
        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(node_test_suit, test_node_restart_from_snapshot) {
  auto snap = create_snapshot(2, 1, {1, 2});

  lepton::core::pb::repeated_entry entries;
  auto& entry = *entries.Add();
  entry.set_term(1);
  entry.set_index(3);
  entry.set_data("foo");

  raftpb::hard_state hs;
  hs.set_term(1);
  hs.set_commit(3);

  ready want_rd;
  // No HardState is emitted because nothing changed relative to what is
  // already persisted.
  want_rd.hard_state = raftpb::hard_state{};
  // commit up to index commit index in st
  want_rd.committed_entries = entries;
  // MustSync is only true when there is a new HardState or new entries;
  // neither is the case here.
  want_rd.must_sync = false;

  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();
  mm_storage.set_hard_state(std::move(hs));
  ASSERT_TRUE(mm_storage.apply_snapshot(std::move(snap)));
  ASSERT_TRUE(mm_storage.append(std::move(entries)));

  asio::io_context io_context;
  auto n = lepton::core::restart_node(io_context.get_executor(), new_test_config(1, 10, 1, std::move(storage_proxy)));

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        auto rd_result = co_await n->wait_ready(io_context.get_executor());
        EXPECT_TRUE(rd_result);
        auto& rd = *rd_result.value();
        EXPECT_TRUE(compare_ready(want_rd, rd));
        co_await n->advance();

        co_await expect_wait_timeout_async_receive_ready(io_context.get_executor(), n);
        co_await n->stop();
        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(node_test_suit, test_node_advance) {
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
        {
          // Persist vote.
          auto rd_result = co_await ready_with_timeout(io_context.get_executor(), *node_handle);
          auto& rd = *rd_result;
          SPDLOG_INFO("mm_storage first index:{}, last index:{}, entry size:{}", mm_storage.first_index().value(),
                      mm_storage.last_index().value(), rd.entries.size());
          EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
          SPDLOG_INFO("mm_storage first index:{}, last index:{}", mm_storage.first_index().value(),
                      mm_storage.last_index().value());
          co_await node_handle->advance();
        }

        {
          // Append empty entry.
          auto rd_result = co_await ready_with_timeout(io_context.get_executor(), *node_handle);
          auto& rd = *rd_result;
          SPDLOG_INFO("mm_storage first index:{}, last index:{}, entry size:{}", mm_storage.first_index().value(),
                      mm_storage.last_index().value(), rd.entries.size());
          EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
          SPDLOG_INFO("mm_storage first index:{}, last index:{}", mm_storage.first_index().value(),
                      mm_storage.last_index().value());
          co_await node_handle->advance();
        }

        {
          SPDLOG_INFO("try to propose foo");
          co_await node_handle->propose("foo");
          SPDLOG_INFO("propose foo successful and wait for ready");
          auto rd_result = co_await ready_with_timeout(io_context.get_executor(), *node_handle);
          auto& rd = *rd_result;
          SPDLOG_INFO("mm_storage first index:{}, last index:{}, entry size:{}", mm_storage.first_index().value(),
                      mm_storage.last_index().value(), rd.entries.size());
          EXPECT_TRUE(mm_storage.append(std::move(rd.entries)));
          SPDLOG_INFO("mm_storage first index:{}, last index:{}", mm_storage.first_index().value(),
                      mm_storage.last_index().value());
          co_await node_handle->advance();
        }

        co_await ready_with_timeout(io_context.get_executor(), *node_handle, std::chrono::milliseconds(100));
        co_await node_handle->stop();
        co_return;
      },
      asio::use_future);
  io_context.run();
}

TEST_F(node_test_suit, test_soft_state_equal) {
  struct test_case {
    lepton::core::soft_state st;
    bool wt;
  };
  std::vector<test_case> tests = {
      {.st = lepton::core::soft_state{}, .wt = true},
      {.st = lepton::core::soft_state{.leader_id = 1}, .wt = false},
      {.st = lepton::core::soft_state{.raft_state = lepton::core::state_type::LEADER}, .wt = false},
  };
  for (const auto& iter : tests) {
    ASSERT_EQ(iter.wt, iter.st == lepton::core::soft_state{});
  }
}

TEST_F(node_test_suit, test_is_hard_state_equal) {
  struct test_case {
    raftpb::hard_state hs;
    bool wt;
  };

  std::vector<test_case> tests;
  tests.emplace_back(test_case{.wt = true});
  {
    raftpb::hard_state hs;
    hs.set_vote(1);
    tests.emplace_back(test_case{.hs = hs, .wt = false});
  }
  {
    raftpb::hard_state hs;
    hs.set_commit(1);
    tests.emplace_back(test_case{.hs = hs, .wt = false});
  }
  {
    raftpb::hard_state hs;
    hs.set_term(1);
    tests.emplace_back(test_case{.hs = hs, .wt = false});
  }
  for (const auto& iter : tests) {
    ASSERT_EQ(iter.wt, lepton::core::pb::is_empty_hard_state(iter.hs));
  }
}

TEST_F(node_test_suit, test_node_propose_add_learner_node) {
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
        asio::steady_timer timer(io_context.get_executor());
        timer.expires_after(std::chrono::milliseconds(100));
        lepton::coro::signal_channel goroutine_stopped_chan(io_context.get_executor());
        lepton::coro::signal_channel apply_conf_chan(io_context.get_executor());
        lepton::coro::signal_channel cancel_chan(io_context.get_executor());

        auto running_loop = true;
        auto sub_loop = [&]() -> asio::awaitable<void> {
          while (running_loop) {
            auto result = co_await (cancel_chan.async_receive(asio::as_tuple(asio::use_awaitable)) ||
                                    timer.async_wait(asio::as_tuple(asio::use_awaitable)) ||
                                    node_handle->wait_ready(io_context.get_executor()));
            switch (result.index()) {
              case 0: {
                SPDLOG_INFO("receive cancel siganl and ready to stop running loop");
                running_loop = false;
                break;
              }
              case 1: {
                SPDLOG_INFO("node is ticking");
                node_handle->tick();
                timer.expires_after(std::chrono::milliseconds(100));
                break;
              }
              case 2: {
                auto msg_result = std::get<2>(result);
                auto& rd = *msg_result.value();
                SPDLOG_INFO("receive new ready:\n{}", lepton::core::describe_ready(rd, nullptr));

                EXPECT_TRUE(mm_storage.append(lepton::core::pb::repeated_entry{rd.entries}));

                for (auto& entry : rd.entries) {
                  auto entry_type = entry.type();
                  switch (entry_type) {
                    case raftpb::ENTRY_CONF_CHANGE: {
                      raftpb::conf_change cc;
                      EXPECT_TRUE(cc.ParseFromString(entry.data()));
                      co_await node_handle->apply_conf_change(lepton::core::pb::conf_change_var_as_v2(cc));
                      co_await apply_conf_chan.async_send(asio::error_code{});
                      break;
                    }
                    default:
                      break;
                  }
                }
                co_await node_handle->advance();
                break;
              }
            }
          }
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
        cc1.set_type(::raftpb::conf_change_type::CONF_CHANGE_ADD_LEARNER_NODE);
        cc1.set_node_id(2);
        SPDLOG_INFO("[main logic]first times ready to propose conf change");
        co_await node_handle->propose_conf_change(cc1);
        SPDLOG_INFO("[main logic]first times propose conf change success");
        co_await apply_conf_chan.async_receive();
        SPDLOG_INFO("[main logic]first times apply conf change success");

        co_await cancel_chan.async_send(asio::error_code{});
        SPDLOG_INFO("[main logic]cancel_chan send cancel signal success...");
        co_await goroutine_stopped_chan.async_receive();
        co_await node_handle->stop();
        SPDLOG_INFO("[main logic]sub loop has finished");
        co_return;
      },
      asio::detached);
  io_context.run();
}

TEST_F(node_test_suit, test_append_pagination) {
  constexpr auto max_size_per_msg = 2048;

  std::vector<state_machine_builer_pair> peers;
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  emplace_nil_peer(peers);
  auto n =
      new_network_with_config([](lepton::core::config& c) { c.max_size_per_msg = max_size_per_msg; }, std::move(peers));

  auto seen_full_message = false;

  // Inspect all messages to see that we never exceed the limit, but
  // we do see messages of larger than half the limit.
  n.msg_hook = [&](const raftpb::message& m) -> bool {
    if (m.type() == raftpb::message_type::MSG_APP) {
      std::size_t size = 0;
      for (const auto& entry : m.entries()) {
        size += entry.data().size();
      }
      EXPECT_LE(size, max_size_per_msg) << "sent MsgApp that is too large";
      if (size > max_size_per_msg / 2) {
        seen_full_message = true;
      }
    }
    return true;
  };

  n.send({new_pb_message(1, 1, raftpb::message_type::MSG_HUP)});

  // Partition the network while we make our proposals. This forces
  // the entries to be batched into larger messages.
  n.isolate(1);
  for (auto i = 0; i < 5; ++i) {
    n.send({new_pb_message(1, 1, raftpb::message_type::MSG_PROP, std::string(1000, 'a'))});
  }
  n.recover();

  // After the partition recovers, tick the clock to wake everything
  // back up and send the messages.
  n.send({new_pb_message(1, 1, raftpb::message_type::MSG_BEAT)});
  ASSERT_TRUE(seen_full_message)
      << "didn't see any messages more than half the max size; something is wrong with this test";
}

TEST_F(node_test_suit, test_commit_pagination) {
  asio::io_context io_context;

  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();

  // 创建配置和节点
  auto cfg = new_test_config(1, 10, 1, std::move(storage_proxy));
  // 设置每条消息的最大大小为 2048 字节
  cfg.max_committed_size_per_ready = 2048;
  auto node_handle = new_node_test_harness(io_context.get_executor(), std::move(cfg), {});

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        // 发起竞选
        co_await node_handle->campaign();

        // Persist vote.
        auto rd = co_await ready_with_timeout(io_context.get_executor(), *node_handle);
        SPDLOG_INFO("mm_storage first index:{}, last index:{}, entry size:{}", mm_storage.first_index().value(),
                    mm_storage.last_index().value(), (*rd.get()).entries.size());
        EXPECT_TRUE(mm_storage.append(std::move((*rd.get()).entries)));
        SPDLOG_INFO("mm_storage first index:{}, last index:{}", mm_storage.first_index().value(),
                    mm_storage.last_index().value());
        co_await node_handle->advance();

        // Append empty entry.
        rd = co_await ready_with_timeout(io_context.get_executor(), *node_handle);
        EXPECT_TRUE(mm_storage.append(std::move((*rd.get()).entries)));
        co_await node_handle->advance();

        // Apply empty entry.
        rd = co_await ready_with_timeout(io_context.get_executor(), *node_handle);
        EXPECT_EQ(1, (*rd.get()).committed_entries.size());
        EXPECT_TRUE(mm_storage.append(std::move((*rd.get()).entries)));
        co_await node_handle->advance();

        // =========================== 完成leader 的选举 ===============================

        std::string str(1000, 'a');
        for (std::size_t i = 0; i < 3; ++i) {
          co_await node_handle->propose(std::string{str});
        }

        // First the three proposals have to be appended.
        {
          rd = co_await ready_with_timeout(io_context.get_executor(), *node_handle);
          EXPECT_EQ(3, (*rd.get()).entries.size());
          EXPECT_TRUE(mm_storage.append(std::move((*rd.get()).entries)));
          co_await node_handle->advance();
        }

        // The 3 proposals will commit in two batches.
        rd = co_await ready_with_timeout(io_context.get_executor(), *node_handle);
        EXPECT_EQ(2, (*rd.get()).committed_entries.size());
        EXPECT_TRUE(mm_storage.append(std::move((*rd.get()).entries)));
        co_await node_handle->advance();

        rd = co_await ready_with_timeout(io_context.get_executor(), *node_handle);
        EXPECT_EQ(1, (*rd.get()).committed_entries.size());
        EXPECT_TRUE(mm_storage.append(std::move((*rd.get()).entries)));
        co_await node_handle->advance();

        co_await node_handle->stop();
        co_return;
      },
      asio::detached);
  io_context.run();
}

TEST_F(node_test_suit, test_commit_pagination_with_async_storage_writes) {
  asio::io_context io_context;

  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();

  // 创建配置和节点
  auto cfg = new_test_config(1, 10, 1, std::move(storage_proxy));
  cfg.max_committed_size_per_ready = 2048;
  cfg.async_storage_writes = true;

  auto node_handle = new_node_test_harness(io_context.get_executor(), std::move(cfg), {});
  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        // 发起竞选
        co_await node_handle->campaign();

        {
          // Persist vote.
          auto rd_handle_result = co_await node_handle->wait_ready(io_context.get_executor());
          EXPECT_TRUE(rd_handle_result);
          auto rd_handle = rd_handle_result.value();
          auto& rd = *rd_handle.get();
          SPDLOG_INFO("[receive ready] ready content:\n{}", lepton::core::describe_ready(rd, nullptr));
          EXPECT_EQ(1, rd.messages.size());
          auto& m = rd.messages[0];
          EXPECT_EQ(raftpb::message_type::MSG_STORAGE_APPEND, m.type());
          EXPECT_TRUE(mm_storage.append(std::move(*m.mutable_entries())));
          for (auto& resp : *m.mutable_responses()) {
            auto resp_msg = lepton::core::describe_message(resp, nullptr);
            SPDLOG_INFO("[step 1] ready step message:\n{}", resp_msg);
            auto step_result = co_await node_handle->step(std::move(resp));
            SPDLOG_INFO("[step 1] step message successfult:\n{}", resp_msg);
            EXPECT_TRUE(step_result);
          }
        }

        SPDLOG_INFO("[step 1] finish step......");

        {
          // Append empty entry.
          auto rd_handle_result = co_await node_handle->wait_ready(io_context.get_executor());
          EXPECT_TRUE(rd_handle_result);
          auto rd_handle = rd_handle_result.value();
          auto& rd = *rd_handle.get();
          SPDLOG_INFO("[receive ready] ready content:\n{}", lepton::core::describe_ready(rd, nullptr));
          EXPECT_EQ(1, rd.messages.size());
          auto& m = rd.messages[0];
          EXPECT_EQ(raftpb::message_type::MSG_STORAGE_APPEND, m.type());
          EXPECT_TRUE(mm_storage.append(std::move(*m.mutable_entries())));
          for (auto& resp : *m.mutable_responses()) {
            auto resp_msg = lepton::core::describe_message(resp, nullptr);
            SPDLOG_INFO("[step 2] ready step message:\n{}", resp_msg);
            auto step_result = co_await node_handle->step(std::move(resp));
            SPDLOG_INFO("[step 2] step message successfult:\n{}", resp_msg);
            EXPECT_TRUE(step_result);
          }
        }

        SPDLOG_INFO("[step 2] finish step......");

        {
          // Apply empty entry.
          auto rd_handle_result = co_await node_handle->wait_ready(io_context.get_executor());
          EXPECT_TRUE(rd_handle_result);
          auto rd_handle = rd_handle_result.value();
          auto& rd = *rd_handle.get();
          SPDLOG_INFO("[receive ready] ready content:\n{}", lepton::core::describe_ready(rd, nullptr));
          for (auto& m : rd.messages) {
            SPDLOG_INFO("[Apply empty entry] message content:\n{}", lepton::core::describe_message(m, nullptr));
          }
          EXPECT_EQ(2, rd.messages.size());
          for (auto& m : rd.messages) {
            if (m.type() == raftpb::message_type::MSG_STORAGE_APPEND) {
              EXPECT_TRUE(mm_storage.append(std::move(*m.mutable_entries())));
              for (auto& resp : *m.mutable_responses()) {
                auto step_result = co_await node_handle->step(std::move(resp));
                EXPECT_TRUE(step_result);
              }
            } else if (m.type() == raftpb::message_type::MSG_STORAGE_APPLY) {
              EXPECT_EQ(1, m.entries_size());
              EXPECT_EQ(1, m.responses_size());
              auto step_result = co_await node_handle->step(std::move(*m.mutable_responses(0)));
              EXPECT_TRUE(step_result);
            } else {
              assert(false);
            }
          }
        }

        SPDLOG_INFO("[step 3] finish step......");

        // Propose first entry.
        std::string blob(1024, 'a');
        co_await node_handle->propose(std::string{blob});

        {
          // Append first entry.
          auto rd_handle = co_await ready_with_timeout(io_context.get_executor(), *node_handle);
          auto& rd = *rd_handle.get();
          EXPECT_EQ(1, rd.messages.size());
          auto& m = rd.messages[0];
          EXPECT_EQ(raftpb::message_type::MSG_STORAGE_APPEND, m.type());
          EXPECT_EQ(1, m.entries_size());
          EXPECT_TRUE(mm_storage.append(std::move(*m.mutable_entries())));
          for (auto& resp : *m.mutable_responses()) {
            auto step_result = co_await node_handle->step(std::move(resp));
            EXPECT_TRUE(step_result);
          }
        }

        // Propose second entry.
        co_await node_handle->propose(std::string{blob});

        lepton::core::pb::repeated_message apply_resps;
        {
          // Append second entry. Don't apply first entry yet.
          auto rd_handle = co_await ready_with_timeout(io_context.get_executor(), *node_handle);
          auto& rd = *rd_handle.get();
          EXPECT_EQ(2, rd.messages.size());
          for (auto& m : rd.messages) {
            if (m.type() == raftpb::message_type::MSG_STORAGE_APPEND) {
              EXPECT_TRUE(mm_storage.append(std::move(*m.mutable_entries())));
              for (auto& resp : *m.mutable_responses()) {
                auto step_result = co_await node_handle->step(std::move(resp));
                EXPECT_TRUE(step_result);
              }
            } else if (m.type() == raftpb::message_type::MSG_STORAGE_APPLY) {
              EXPECT_EQ(1, m.entries_size());
              EXPECT_EQ(1, m.responses_size());
              apply_resps.Add({raftpb::message{m.responses(0)}});
            } else {
              assert(false);
            }
          }
        }

        // Propose third entry.
        co_await node_handle->propose(std::string{blob});

        {
          // Append third entry. Don't apply second entry yet.
          auto rd_handle = co_await ready_with_timeout(io_context.get_executor(), *node_handle);
          auto& rd = *rd_handle.get();
          EXPECT_EQ(2, rd.messages.size());
          for (auto& m : rd.messages) {
            if (m.type() == raftpb::message_type::MSG_STORAGE_APPEND) {
              EXPECT_TRUE(mm_storage.append(std::move(*m.mutable_entries())));
              for (auto& resp : *m.mutable_responses()) {
                auto step_result = co_await node_handle->step(std::move(resp));
                EXPECT_TRUE(step_result);
              }
            } else if (m.type() == raftpb::message_type::MSG_STORAGE_APPLY) {
              EXPECT_EQ(1, m.entries_size());
              EXPECT_EQ(1, m.responses_size());
              apply_resps.Add({raftpb::message{m.responses(0)}});
            } else {
              assert(false);
            }
          }
        }

        // Third entry should not be returned to be applied until first entry's
        // application is acknowledged.
        auto drain = true;
        while (drain) {
          asio::steady_timer timeout_timer(io_context.get_executor(), std::chrono::milliseconds(10));
          auto result = co_await (node_handle->wait_ready(io_context.get_executor()) ||
                                  timeout_timer.async_wait(asio::as_tuple(asio::use_awaitable)));
          switch (result.index()) {
            case 0: {
              auto recv_result = std::get<0>(result);
              auto& rd = *recv_result;
              for (const auto& m : rd->messages) {
                EXPECT_NE(raftpb::message_type::MSG_STORAGE_APPLY, m.type())
                    << "unexpected message: " << m.DebugString();
              }
              break;
            }
            case 1: {
              drain = false;
              break;
            }
          }
        }

        // Acknowledged first entry application.
        auto step_result = co_await node_handle->step(std::move(apply_resps[0]));
        EXPECT_TRUE(step_result);
        apply_resps.erase(apply_resps.begin());

        // Third entry now returned for application.
        {
          auto rd_handle = co_await ready_with_timeout(io_context.get_executor(), *node_handle);
          auto& rd = *rd_handle.get();
          EXPECT_EQ(1, rd.messages.size());
          auto& m = rd.messages[0];
          EXPECT_EQ(raftpb::message_type::MSG_STORAGE_APPLY, m.type());
          EXPECT_EQ(1, m.entries_size());
          apply_resps.Add(raftpb::message{m.responses(0)});
          // Acknowledged second and third entry application.
          for (auto& resp : apply_resps) {
            auto step_result = co_await node_handle->step(std::move(resp));
            EXPECT_TRUE(step_result);
          }
        }
        co_await node_handle->stop();
        co_return;
      },
      asio::use_future);
  io_context.run();
}

// TestNodeCommitPaginationAfterRestart regression tests a scenario in which the
// Storage's Entries size limitation is slightly more permissive than Raft's
// internal one. The original bug was the following:
//
//   - node learns that index 11 (or 100, doesn't matter) is committed
//   - nextCommittedEnts returns index 1..10 in CommittedEntries due to size limiting.
//     However, index 10 already exceeds maxBytes, due to a user-provided impl of Entries.
//   - Commit index gets bumped to 10
//   - the node persists the HardState, but crashes before applying the entries
//   - upon restart, the storage returns the same entries, but `slice` takes a different code path
//     (since it is now called with an upper bound of 10) and removes the last entry.
//   - Raft emits a HardState with a regressing commit index.
//
// A simpler version of this test would have the storage return a lot less entries than dictated
// by maxSize (for example, exactly one entry) after the restart, resulting in a larger regression.
// This wouldn't need to exploit anything about Raft-internal code paths to fail.
// 当 Raft 节点在持久化提交索引后崩溃重启时，如何处理存储层（Storage）和 Raft 内部对日志条目大小限制的不一致问题。
TEST_F(node_test_suit, test_node_commit_pagination_after_restart) {
  auto mm_storage_ptr = new_test_memory_storage_ptr({with_peers({1})});
  auto& mm_storage = *mm_storage_ptr;
  pro::proxy<storage_builer> storage_proxy = mm_storage_ptr.get();

  raftpb::hard_state persisted_hard_state;
  persisted_hard_state.set_term(1);
  persisted_hard_state.set_commit(1);
  persisted_hard_state.set_commit(10);
  mm_storage.set_hard_state(raftpb::hard_state{persisted_hard_state});

  auto& ents = mm_storage.mutable_ents();
  std::uint64_t size = 0;
  for (std::uint64_t i = 0; i < 10; ++i) {
    auto entry = ents.Add();
    entry->set_term(1);
    entry->set_index(i + 1);
    entry->set_type(raftpb::ENTRY_NORMAL);
    entry->set_data("a");
    size += entry->ByteSizeLong();
  }

  auto cfg = new_test_config(1, 10, 1, std::move(storage_proxy));
  // Set a MaxSizePerMsg that would suggest to Raft that the last committed entry should
  // not be included in the initial rd.CommittedEntries. However, our storage will ignore
  // this and *will* return it (which is how the Commit index ended up being 10 initially).
  cfg.max_size_per_msg = size - ents.at(ents.size() - 1).ByteSizeLong() - 1;

  auto raw_node_result = lepton::core::new_raw_node(std::move(cfg));
  ASSERT_TRUE(raw_node_result);
  auto& raw_node = *raw_node_result;

  asio::io_context io_context;
  lepton::core::node n(io_context.get_executor(), std::move(raw_node));

  asio::co_spawn(
      io_context,
      [&]() -> asio::awaitable<void> {
        n.start_run();
        auto rd_result = co_await ready_with_timeout(io_context.get_executor(), n);
        auto& rd = *rd_result;

        EXPECT_FALSE(!lepton::core::pb::is_empty_hard_state(rd.hard_state) &&
                     rd.hard_state.commit() < persisted_hard_state.commit());
        co_await n.stop();
        co_return;
      },
      asio::use_future);
  io_context.run();
}