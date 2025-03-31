#ifndef _LEPTON_STATE_H_
#define _LEPTON_STATE_H_
#include <cstdint>
namespace lepton {
namespace tracker {
enum class state_type : std::uint64_t {
  // StateProbe indicates a follower whose last index isn't known. Such a
  // follower is "probed" (i.e. an append sent periodically) to narrow down
  // its last index. In the ideal (and common) case, only one round of probing
  // is necessary as the follower will react with a hint. Followers that are
  // probed over extended periods of time are often offline.
  // 含义:
  // 该状态表示一个跟随者（follower）的最后一个日志索引（即其日志进度）尚不确定，
  // 因此该跟随者需要通过定期发送 append 请求来“探测”其最新的日志位置。
  // 这个探测过程通常通过发送一个包含附加日志的请求来完成。
  // 典型场景:
  // 当一个追随者刚开始与领导者进行通信时，领导者通过探测来确定它的日志状态，通常只有一轮探测就能确定追随者的最新索引。在理想情况下，追随者会立即响应，并提供一个索引提示。如果一个追随者被持续探测且没有响应，通常表明它已经掉线。
  STATE_PROBE,
  // StateReplicate is the state steady in which a follower eagerly receives
  // log entries to append to its log.
  // 含义:
  // 该状态表示一个跟随者处于稳定状态，能够持续接收并附加日志条目。处于这个状态的跟随者已经与领导者的日志同步，且不再需要探测。
  // 典型场景:
  // 追随者与领导者之间的日志是同步的，领导者定期将日志条目发送给追随者，追随者将这些日志条目附加到自己的日志中。此时，追随者已经处于一个正常的工作状态。
  STATE_REPLICATE,
  // StateSnapshot indicates a follower that needs log entries not available
  // from the leader's Raft log. Such a follower needs a full snapshot to
  // return to StateReplicate.
  // 含义:
  // 该状态表示一个跟随者需要从领导者获取日志条目或状态快照，因为它缺少部分日志条目。通常，追随者需要通过全量快照来恢复到
  // StateReplicate 状态。
  // 典型场景:
  // 如果一个追随者在很长时间内未能接收到最新的日志条目，或者其日志与领导者的日志有很大的差异，它可能会进入
  // StateSnapshot
  // 状态。在这种情况下，追随者需要从领导者请求一个完整的快照（包括日志的历史部分），以便从一个一致的状态恢复，并重新回到
  // StateReplicate 状态。
  STATE_SNAPSHOT,
};

#ifdef LEPTON_TEST
#include <string_view>
inline std::string_view state_type2string(state_type s) {
  switch (s) {
    case state_type::STATE_PROBE: {
      return "StateProbe";
    }
    case state_type::STATE_REPLICATE: {
      return "StateReplicate";
    }
    case state_type::STATE_SNAPSHOT: {
      return "StateSnapshot";
    }
  }
  assert(false);
}
#endif
}  // namespace tracker
}  // namespace lepton

#endif  // _LEPTON_STATE_H_
