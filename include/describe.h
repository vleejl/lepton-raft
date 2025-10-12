#ifndef _LEPTON_DESCRIBE_H_
#define _LEPTON_DESCRIBE_H_
#include "raft.h"
#include "raw_node.h"
#include "state.h"

namespace lepton {

std::string describe_soft_state(const soft_state &ss);

std::string describe_hard_state(const raftpb::hard_state &hs);

std::string describe_message(const raftpb::message &m);

std::string describe_ready(const ready &rd);

std::string describe_entries(const pb::repeated_entry &entries);

}  // namespace lepton

#endif  // _LEPTON_DESCRIBE_H_
