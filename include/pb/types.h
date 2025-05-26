#ifndef _LEPTON_TYPES_H_
#define _LEPTON_TYPES_H_
#include <raft.pb.h>

#include <cstdint>

namespace lepton {
namespace pb {
// entryEncodingSize represents the protocol buffer encoding size of one or more
// entries.
using entry_encoding_size = std::uint64_t;
// entryPayloadSize represents the size of one or more entries' payloads.
// Notably, it does not depend on its Index or Term. Entries with empty
// payloads, like those proposed after a leadership change, are considered
// to be zero size.
using entry_payload_size = std::uint64_t;
using snapshot_ptr = std::unique_ptr<raftpb::snapshot>;
using entry_ptr = std::unique_ptr<raftpb::entry>;
using repeated_entry = google::protobuf::RepeatedPtrField<raftpb::entry>;
using span_entry = absl::Span<const raftpb::entry* const>;
using repeated_message = google::protobuf::RepeatedPtrField<raftpb::message>;
using repeated_conf_change = google::protobuf::RepeatedPtrField<raftpb::conf_change_single>;
using repeated_uint64 = google::protobuf::RepeatedField<std::uint64_t>;

// entryID uniquely identifies a raft log entry.
//
// Every entry is associated with a leadership term which issued this entry and
// initially appended it to the log. There can only be one leader at any term,
// and a leader never issues two entries with the same index.
struct entry_id {
  std::uint64_t term;
  std::uint64_t index;
};

// logSlice describes a correct slice of a raft log.
//
// Every log slice is considered in a context of a specific leader term. This
// term does not necessarily match entryID.term of the entries, since a leader
// log contains both entries from its own term, and some earlier terms.
//
// Two slices with a matching logSlice.term are guaranteed to be consistent,
// i.e. they never contain two different entries at the same index. The reverse
// is not true: two slices with different logSlice.term may contain both
// matching and mismatching entries. Specifically, logs at two different leader
// terms share a common prefix, after which they *permanently* diverge.
//
// A well-formed logSlice conforms to raft safety properties. It provides the
// following guarantees:
//
//  1. entries[i].Index == prev.index + 1 + i,
//  2. prev.term <= entries[0].Term,
//  3. entries[i-1].Term <= entries[i].Term,
//  4. entries[len-1].Term <= term.
//
// Property (1) means the slice is contiguous. Properties (2) and (3) mean that
// the terms of the entries in a log never regress. Property (4) means that a
// leader log at a specific term never has entries from higher terms.
//
// Users of this struct can assume the invariants hold true. Exception is the
// "gateway" code that initially constructs logSlice, such as when its content
// is sourced from a message that was received via transport, or from Storage,
// or in a test code that manually hard-codes this struct. In these cases, the
// invariants should be validated using the valid() method.
struct log_slice {
  // term is the leader term containing the given entries in its log.
  std::uint64_t term;
  // prev is the ID of the entry immediately preceding the entries.
  entry_id prev;
  // entries contains the consecutive entries representing this slice.
  repeated_entry entries;
};
}  // namespace pb
}  // namespace lepton

#endif  // _LEPTON_TYPES_H_
