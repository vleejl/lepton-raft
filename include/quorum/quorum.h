#ifndef _LEPTON_QUPRUM_H_
#define _LEPTON_QUPRUM_H_

#include <cstdint>
#include <limits>
#include <map>

#include "error.h"
#include "proxy.h"
#include "utility_macros.h"
namespace lepton {
namespace quorum {
// Index is a Raft log position.
using log_index = std::uint64_t;
constexpr auto INVALID_LOG_INDEX =
    static_cast<log_index>(std::numeric_limits<std::uint64_t>::max());

// VoteResult indicates the outcome of a vote.
enum class vote_result : std::uint8_t {
  // VotePending indicates that the decision of the vote depends on future
  VOTE_PENDING,
  // VoteLost indicates that the quorum has voted "no".
  VOTE_LOST,
  // VoteWon indicates that the quorum has voted "yes".
  VOTE_MIN,
};

// AckedIndexer allows looking up a commit index for a given ID of a voter
// from a corresponding MajorityConfig.
PRO_DEF_MEM_DISPATCH(acked_indexer, acked_index);
// clang-format off
struct acked_indexer_builer : pro::facade_builder 
  ::add_convention<acked_indexer, leaf::result<log_index>(const std::uint64_t &id)>
  ::add_view<acked_indexer_builer>
  ::add_view<const acked_indexer_builer>
  ::build{};
// clang-format on

class map_ack_indexer {
  NONCOPYABLE_NONMOVABLE(map_ack_indexer)
 public:
  map_ack_indexer(std::map<std::uint64_t, log_index> &&id_log_idx_map)
      : map_(id_log_idx_map) {}
  leaf::result<log_index> acked_index(const std::uint64_t &id) {
    if (auto log_pos = map_.find(id); log_pos != map_.end()) {
      return log_pos->second;
    }
    return leaf::new_error(key_not_found_error);
  }

 private:
  std::map<std::uint64_t, log_index> map_;
};
}  // namespace quorum
}  // namespace lepton

#endif  // _LEPTON_QUPRUM_H_