#ifndef _LEPTON_PB_H_
#define _LEPTON_PB_H_
#include <raft.pb.h>

#include <cstddef>
#include <cstdint>
#include <memory>
namespace lepton {
namespace pb {
using snapshot_ptr = std::unique_ptr<raftpb::snapshot>;
using entry_ptr = std::unique_ptr<raftpb::entry>;
using repeated_entry = google::protobuf::RepeatedPtrField<raftpb::entry>;
using repeated_message = google::protobuf::RepeatedPtrField<raftpb::message>;

bool is_empty_snap(const raftpb::snapshot& snap);

repeated_entry extract_range_without_copy(repeated_entry& src, int start,
                                          int end);

repeated_entry limit_entry_size(repeated_entry& entries,
                                std::uint64_t max_size);

void assert_conf_states_equivalent(const raftpb::conf_state& lhs,
                                   const raftpb::conf_state& rhs);

bool operator==(const raftpb::hard_state& lhs, const raftpb::hard_state& rhs);

bool is_empty_hard_state(const raftpb::hard_state& hs);

}  // namespace pb

}  // namespace lepton

#endif  // _LEPTON_PB_H_
