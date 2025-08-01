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
#include "magic_enum.hpp"
#include "majority.h"
#include "memory_storage.h"
#include "node.h"
#include "protobuf.h"
#include "raft.h"
#include "raft_error.h"
#include "raw_node.h"
#include "read_only.h"
#include "state.h"
#include "storage.h"
#include "test_diff.h"
#include "test_raft_protobuf.h"
#include "test_raft_utils.h"
#include "test_utility_data.h"
#include "tracker.h"
#include "types.h"
using namespace lepton;

class node_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

using asio::steady_timer;

template <typename T>
asio::awaitable<leaf::result<T>> receive_with_timeout(channel<void(asio::error_code, T)>& ch,
                                                      std::chrono::steady_clock::duration timeout) {
  auto exec = co_await asio::this_coro::executor;
  steady_timer timer(exec);
  timer.expires_after(timeout);

  std::optional<T> result;
  asio::error_code final_ec;
  bool done = false;

  co_await (ch.async_receive(
                [&](asio::error_code ec, T value) {
                  if (!done) {
                    final_ec = ec;
                    if (!ec) {
                      result = std::move(value);
                    }
                    done = true;
                  }
                },
                asio::use_awaitable) ||
            timer.async_wait(
                [&](asio::error_code ec) {
                  if (!done) {
                    final_ec = asio::error::timed_out;
                    done = true;
                  }
                },
                asio::use_awaitable));
  if (final_ec) {
    co_return result;
  }
  co_return leaf::new_error(final_ec);
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
        co_await n.run1();
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