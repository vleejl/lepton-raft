#include "raft.h"

#include "leaf.hpp"
using namespace lepton;

leaf::result<std::unique_ptr<raft>> new_raft(
    std::unique_ptr<config> config_ptr) {
  if (config_ptr == nullptr) {
    return leaf::new_error("config is null ptr, can not new raft object");
  }
  BOOST_LEAF_CHECK(config_ptr->validate());
  
  return {};
}