#include "progress.h"

namespace lepton {

namespace tracker {

std::string progress_map::string(const progress_map& m) {
  std::vector<uint64_t> ids;
  for (const auto& kv : m.map_) {
    ids.push_back(kv.first);
  }

  // 排序键
  std::sort(ids.begin(), ids.end());

  // 使用 std::stringstream 来构建最终的字符串
  std::stringstream buf;
  for (const auto& id : ids) {
    buf << id << ": " << m.map_.at(id)->string() << "\n";
  }

  return buf.str();
}

leaf::result<quorum::log_index> progress_map::acked_index(std::uint64_t id) {
  if (auto log_pos = map_.find(id); log_pos != map_.end()) {
    return log_pos->second->match_;
  }
  return leaf::new_error(key_not_found_error);
}

}  // namespace tracker

}  // namespace lepton
