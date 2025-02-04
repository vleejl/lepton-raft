#include "raft_log_unstable.h"
using namespace lepton;

unstable::unstable(std::uint64_t offset) : offset_(offset) {}

unstable::unstable(pb::snapshot_ptr snapshot,
                   std::vector<pb::entry_ptr>&& entries, std::uint64_t offset)
    : snapshot_(std::move(snapshot)),
      entries_(std::move(entries)),
      offset_(offset) {}