#ifndef _LEPTON_PB_H_
#define _LEPTON_PB_H_
#include <raft.pb.h>

#include "types.h"
namespace lepton {
namespace pb {

entry_encoding_size ent_size(const repeated_entry& entries);

entry_encoding_size ent_size(const pb::span_entry& entries);

entry_id pb_entry_id(const raftpb::entry* const entry_ptr);

bool is_empty_snap(const raftpb::snapshot& snap);

repeated_entry extract_range_without_copy(repeated_entry& src, int start, int end);

pb::span_entry limit_entry_size(pb::span_entry entries, entry_encoding_size max_size);

repeated_entry limit_entry_size(repeated_entry& entries, entry_encoding_size max_size);

// extend appends vals to the given dst slice. It differs from the standard
// slice append only in the way it allocates memory. If cap(dst) is not enough
// for appending the values, precisely size len(dst)+len(vals) is allocated.
//
// Use this instead of standard append in situations when this is the last
// append to dst, so there is no sense in allocating more than needed.
repeated_entry extend(repeated_entry& dst, pb::span_entry vals);

void assert_conf_states_equivalent(const raftpb::conf_state& lhs, const raftpb::conf_state& rhs);

bool operator==(const raftpb::hard_state& lhs, const raftpb::hard_state& rhs);

bool is_empty_hard_state(const raftpb::hard_state& hs);

}  // namespace pb

}  // namespace lepton

#endif  // _LEPTON_PB_H_
