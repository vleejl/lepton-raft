#ifndef _LEPTON_RAFT_PB_H_
#define _LEPTON_RAFT_PB_H_
#include <raft.pb.h>

#include <memory>
namespace lepton {
namespace pb {
using snapshot_ptr = std::unique_ptr<raftpb::snapshot>;
using entry_ptr = std::unique_ptr<raftpb::entry>;
}  // namespace pb

}  // namespace lepton

#endif  // _LEPTON_RAFT_PB_H_
