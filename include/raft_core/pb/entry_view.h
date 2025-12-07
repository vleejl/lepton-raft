#ifndef _LEPTON_ENTRY_VIEW_H_
#define _LEPTON_ENTRY_VIEW_H_

#include <vector>

#include "types.h"
namespace lepton::core {

namespace pb {

struct entry_view {
  using span_t = span_entry;

  entry_view() = default;
  entry_view(span_t s) {
    if (!s.empty()) blocks_.push_back(s);
  }

  // 合并多个 span
  void append(span_t s) {
    if (!s.empty()) blocks_.push_back(s);
  }

  // 获取所有块的总条目数
  size_t size() const noexcept {
    size_t total = 0;
    for (auto& b : blocks_) total += b.size();
    return total;
  }

  bool empty() const noexcept { return size() == 0; }

  const std::vector<span_t>& blocks() const noexcept { return blocks_; }

  // 支持直接 for(auto&) 遍历 entries
  struct iterator {
    const entry_view* owner;
    size_t block_idx;
    size_t elem_idx;

    const raftpb::entry& operator*() const { return *owner->blocks_[block_idx][elem_idx]; }

    iterator& operator++() {
      if (++elem_idx >= owner->blocks_[block_idx].size()) {
        ++block_idx;
        elem_idx = 0;
      }
      return *this;
    }

    bool operator!=(const iterator& other) const { return block_idx != other.block_idx || elem_idx != other.elem_idx; }
  };

  iterator begin() const { return iterator{this, 0, 0}; }

  iterator end() const { return iterator{this, blocks_.size(), 0}; }

 private:
  std::vector<span_t> blocks_;
};

}  // namespace pb

}  // namespace lepton::core

#endif  // _LEPTON_ENTRY_VIEW_H_
