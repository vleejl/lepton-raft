#include "progress.h"

namespace lepton {

namespace tracker {

progress_map progress_map::clone() const {
  type data;
  for (const auto& [id, p] : map_) {
    data.emplace(id, p.clone());
  }
  return progress_map{std::move(data)};
}

std::string progress_map::string() const {
  std::vector<uint64_t> ids;
  for (const auto& kv : map_) {
    ids.push_back(kv.first);
  }

  // 排序键
  std::sort(ids.begin(), ids.end());

  // 使用 std::stringstream 来构建最终的字符串
  std::stringstream buf;
  for (const auto& id : ids) {
    buf << id << ": " << map_.at(id).string() << "\n";
  }

  return buf.str();
}

leaf::result<quorum::log_index> progress_map::acked_index(std::uint64_t id) {
  if (auto log_pos = map_.find(id); log_pos != map_.end()) {
    return log_pos->second.match_;
  }
  return new_error(error_code::KEY_NOT_FOUND, fmt::format("{} not found", id));
}

}  // namespace tracker

}  // namespace lepton
