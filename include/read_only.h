#ifndef _LEPTON_READ_ONLY_H_
#define _LEPTON_READ_ONLY_H_
#include <absl/types/span.h>
#include <raft.pb.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "config.h"
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
  raftpb::message req;
  std::uint64_t index;

  // NB: this never records 'false', but it's more convenient to use this
  // instead of a map[uint64]struct{} due to the API of quorum.VoteResult. If
  // this becomes performance sensitive enough (doubtful), quorum.VoteResult
  // can change to an API that is closer to that of CommittedIndex.
  std::unordered_map<uint64_t, bool> acks;
};

struct read_only {
  NOT_COPYABLE(read_only)
  read_only(read_only_option read_only_opt) : read_only_opt(read_only_opt) {}
  read_only(read_only&&) = default;

  // field
  read_only_option read_only_opt;
  std::unordered_map<std::string, read_index_status> pending_read_index;
  std::vector<std::string> read_index_queue;
};
}  // namespace lepton

#endif  // _LEPTON_READ_ONLY_H_
