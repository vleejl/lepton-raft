#ifndef _LEPTON_QUPRUM_H_
#define _LEPTON_QUPRUM_H_

#include <fmt/core.h>
#include <proxy.h>

#include <cstdint>
#include <limits>
#include <map>

#include "fmt/format.h"
#include "lepton_error.h"
#include "utility_macros.h"
namespace lepton {
namespace quorum {
// Index is a Raft log position.
using log_index = std::uint64_t;
constexpr auto INVALID_LOG_INDEX = static_cast<log_index>(std::numeric_limits<std::uint64_t>::max());

inline std::string log_index_to_string(log_index i) {
  if (i == std::numeric_limits<uint64_t>::max()) {
    return "∞";
  }
  return std::to_string(i);
}

// VoteResult indicates the outcome of a vote.
enum class vote_result : std::uint8_t {
  // VotePending indicates that the decision of the vote depends on future
  VOTE_PENDING,
  // VoteLost indicates that the quorum has voted "no".
  VOTE_LOST,
  // VoteWon indicates that the quorum has voted "yes".
  VOTE_WON,
};

// AckedIndexer allows looking up a commit index for a given ID of a voter
// from a corresponding MajorityConfig.
PRO_DEF_MEM_DISPATCH(acked_indexer, acked_index);
// clang-format off
struct acked_indexer_builder : pro::facade_builder 
  ::add_convention<acked_indexer, leaf::result<log_index>(std::uint64_t id)>
  ::support_view
  ::build{};
// clang-format on

class map_ack_indexer {
  NONCOPYABLE_NONMOVABLE(map_ack_indexer)
  map_ack_indexer() = delete;

 public:
  map_ack_indexer(std::map<std::uint64_t, log_index>&& id_log_idx_map) : map_(id_log_idx_map) {}

  const auto& view() const { return map_; }

  leaf::result<log_index> acked_index(std::uint64_t id) {
    if (auto log_pos = map_.find(id); log_pos != map_.end()) {
      return log_pos->second;
    }
    return new_error(logic_error::KEY_NOT_FOUND, fmt::format("{} not found", id));
  }

 private:
  std::map<std::uint64_t, log_index> map_;
};
}  // namespace quorum
}  // namespace lepton

#endif  // _LEPTON_QUPRUM_H_