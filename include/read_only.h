#ifndef _LEPTON_READ_ONLY_H_
#define _LEPTON_READ_ONLY_H_
#include <absl/types/span.h>
#include <raft.pb.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "config.h"
#include "error.h"
#include "fmt/format.h"
#include "leaf.hpp"
#include "utility_macros.h"
namespace lepton {

// ReadState provides state for read only query.
// It's caller's responsibility to call ReadIndex first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, eg. given a unique id as
// RequestCtx
struct read_state {
  std::uint64_t index;
  std::string request_ctx;
};

struct read_index_status {
  NOT_COPYABLE(read_index_status)
  read_index_status(raftpb::message&& m, std::uint64_t index) : req(std::move(m)), index(index) {}
  read_index_status(read_index_status&&) = default;

  raftpb::message req;
  std::uint64_t index;

  // NB: this never records 'false', but it's more convenient to use this
  // instead of a map[uint64]struct{} due to the API of quorum.VoteResult. If
  // this becomes performance sensitive enough (doubtful), quorum.VoteResult
  // can change to an API that is closer to that of CommittedIndex.
  std::unordered_map<uint64_t, bool> acks;
};

class read_only {
 public:
  MOVABLE_BUT_NOT_COPYABLE(read_only)
  read_only(read_only_option read_only_opt) : option_(read_only_opt) {}
  auto read_only_opt() const { return option_; }
  // addRequest adds a read only request into readonly struct.
  // `index` is the commit index of the raft state machine when it received
  // the read only request.
  // `m` is the original read only request message from the local or remote node.
  void add_request(std::uint64_t index, raftpb::message&& m) {
    assert(!m.entries().empty());
    const std::string& s = m.entries().rbegin()->data();
    pending_read_index_.try_emplace(s, std::move(m), index);
    read_index_queue_.push_back(s);
  }

  // recvAck notifies the readonly struct that the raft state machine received
  // an acknowledgment of the heartbeat that attached with the read only request
  // context.
  // recvAck 函数的作用是
  // ​​处理只读请求的确认消息​​，并跟踪这些确认的进度，以确保只读请求的线性一致性。
  // 函数功能
  // 当 Raft 集群的 ​​领导者（Leader）​​
  // 收到其他节点对只读请求的确认（通过心跳或日志提交）时，该函数会：
  // ​​更新确认状态​​：记录哪个节点（id）已确认某个只读请求（由 context 标识）。
  // ​​返回当前确认状态​​：提供当前已确认的节点列表，供后续判断是否满足
  // ​​多数派（quorum）​​ 条件。
  std::optional<std::reference_wrapper<const std::unordered_map<uint64_t, bool>>> recv_ack(std::uint64_t id,
                                                                                           const std::string& context) {
    auto iter = pending_read_index_.find(context);
    if (iter == pending_read_index_.end()) {
      return std::nullopt;  // 未找到时返回空 optional
    }
    iter->second.acks[id] = true;
    // 找到时返回包装后的引用
    return std::cref(iter->second.acks);
  }

  // advance advances the read only request queue kept by the readonly struct.
  // It dequeues the requests until it finds the read only request that has
  // the same context as the given `m`.
  // 主要用于在只读请求队列中找到特定上下文对应的请求
  std::vector<read_index_status> advance(raftpb::message&& m) {
    int i = 0;
    auto found = false;

    const std::string& ctx = m.entries().rbegin()->data();
    std::vector<std::string_view> rss_keys;
    for (const auto& okctx : read_index_queue_) {
      i++;
      auto iter = pending_read_index_.find(okctx);
      if (iter == pending_read_index_.end()) {
        panic("cannot find corresponding read state from pending map");
      }
      rss_keys.push_back(okctx);
      if (okctx == ctx) {
        found = true;
        break;
      }
    }

    if (found) {
      read_index_queue_.erase(read_index_queue_.begin(), read_index_queue_.begin() + i);
      std::vector<read_index_status> rss;
      rss.reserve(rss_keys.size());
      for (auto& iter : rss_keys) {
        // 从 unordered_map 中提取节点（键类型为 string_view）
        auto node = pending_read_index_.extract(iter);

        // 检查节点是否有效（提取成功）
        if (!node.empty()) {
          // 关键点：通过 mapped() 获取 read_index_status 的值
          // 并用 std::move 转移所有权到 vector 中
          rss.emplace_back(std::move(node.mapped()));
        } else {
          panic(fmt::format("key:{} not exist in pending_read_index", iter));
        }
      }
      return rss;
    }
    return {};
  }

  // lastPendingRequestCtx returns the context of the last pending read only
  // request in readonly struct.
  std::string last_pending_request_ctx() const {
    if (read_index_queue_.empty()) {
      return "";
    }
    return std::string{read_index_queue_.back()};
  }

 private:
  // field
  read_only_option option_;
  std::unordered_map<std::string_view, read_index_status> pending_read_index_;
  std::vector<std::string_view> read_index_queue_;
};
}  // namespace lepton

#endif  // _LEPTON_READ_ONLY_H_
