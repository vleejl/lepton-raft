#pragma once
#ifndef _LEPTON_STROAGE_PB_TYPES_H_
#define _LEPTON_STROAGE_PB_TYPES_H_
#include <raft.pb.h>
namespace lepton::storage::pb {
using snapshot = raftpb::snapshot_metadata;
}  // namespace lepton::storage::pb

#endif  // _LEPTON_STROAGE_PB_TYPES_H_
