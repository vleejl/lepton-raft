#ifndef _LEPTON_PB_H_
#define _LEPTON_PB_H_
#include <raft.pb.h>

#include <memory>
namespace lepton {
namespace pb {
using snapshot_ptr = std::unique_ptr<raftpb::snapshot>;
using entry_ptr = std::unique_ptr<raftpb::entry>;
using repeated_entry = google::protobuf::RepeatedPtrField<raftpb::entry>;

bool is_empty_snap(const raftpb::snapshot& snap);
}  // namespace pb

}  // namespace lepton

#endif  // _LEPTON_PB_H_
