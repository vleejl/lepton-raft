#ifndef _LEPTON_DESCRIBE_H_
#define _LEPTON_DESCRIBE_H_
#include "raft.h"
#include "raw_node.h"
#include "state.h"

namespace lepton::core {

using entry_formatter_func = std::string(const std::string &);

std::string describe_soft_state(const soft_state &ss);

std::string describe_hard_state(const raftpb::hard_state &hs);

std::string describe_message(const raftpb::message &m, entry_formatter_func f);

std::string describe_ready(const ready &rd, entry_formatter_func f);

std::string describe_entries(const pb::repeated_entry &entries, entry_formatter_func f);

}  // namespace lepton::core

#endif  // _LEPTON_DESCRIBE_H_
