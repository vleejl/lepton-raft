#ifndef _LEPTON_NODE_
#define _LEPTON_NODE_
#include <asio/any_io_executor.hpp>
#include <asio/io_context.hpp>
#include <cstdint>
#include <string>

#include "node_interface.h"
#include "raft.pb.h"
#include "raw_node.h"
#include "ready.h"
#include "spdlog/spdlog.h"
#include "utility_macros.h"

namespace lepton {
struct peer {
  std::uint64_t id;
  std::string context;
};

// node is the canonical implementation of the Node interface

class node {
  NOT_COPYABLE(node)
 public:
  node(asio::any_io_executor executor, raw_node &&raw_node)
      : executor_(executor),
        prop_chan_(executor),
        recv_chan_(executor),
        conf_chan_(executor),
        conf_state_chan_(executor),
        ready_chan_(executor),
        advance_chan_(executor),
        tick_chan_(executor, 128),
        done_chan_(executor),
        stop_chan_(executor),
        status_chan_(executor),
        raw_node_(std::move(raw_node)) {}

  void stop() {
    std::promise<void> stop_promise;
    auto stop_future = stop_promise.get_future();

    asio::post(executor_, [this, &stop_promise] {
      // 情况1：已停止
      if (!done_chan_.is_open()) {
        stop_promise.set_value();
        return;
      }

      // 情况2：成功发送停止信号
      if (stop_chan_.try_send(std::error_code{})) {
        // 启动异步等待协程
        co_spawn(
            executor_,
            [this, &stop_promise]() -> asio::awaitable<void> {  // 在协程中等待完成信号
              asio::error_code ec;
              co_await done_chan_.async_receive(asio::redirect_error(asio::use_awaitable, ec));

              // 忽略通道关闭错误
              if (ec != asio::experimental::channel_errc::channel_closed) {
                SPDLOG_ERROR("closed done channel with error: {}", ec.message());
              }
              stop_promise.set_value();
            },
            asio::detached);
      }
      // 情况3：发送失败（已停止或停止中）
      else {
        stop_promise.set_value();
      }
    });

    // 阻塞等待停止完成
    stop_future.wait();
  }

 private:
  asio::any_io_executor executor_;

  channel<raftpb::message> prop_chan_;
  channel<raftpb::message> recv_chan_;
  channel<raftpb::conf_change_v2> conf_chan_;
  channel<raftpb::conf_change_v2> conf_state_chan_;
  ready_channel ready_chan_;
  signal_channel advance_chan_;
  signal_channel tick_chan_;
  signal_channel done_chan_;
  signal_channel stop_chan_;
  status_channel status_chan_;
  raw_node raw_node_;
};
}  // namespace lepton

#endif  // _LEPTON_NODE_