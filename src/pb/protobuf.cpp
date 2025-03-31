#include "protobuf.h"

#include <cassert>
#include <cstddef>

#include "conf_change.h"
#include "conf_state.h"
#include "error.h"

static raftpb::hard_state EMPTY_STATE;

namespace lepton {

namespace pb {
bool is_empty_snap(const raftpb::snapshot& snap) {
  if (!snap.has_metadata()) {
    return false;
  }
  if (!snap.metadata().has_index()) {
    return false;
  }
  return snap.metadata().index() != 0;
}

// 高性能提取范围元素（需确保使用 Arena 分配）
repeated_entry extract_range_without_copy(repeated_entry& src, int start, int end) {
  repeated_entry dst;

  // 参数校验
  const int size = src.size();
  if (start < 0 || end > size || start >= end) return dst;

  // 预分配指针数组
  const int num_elements = end - start;
  raftpb::entry** extracted = new raftpb::entry*[static_cast<std::size_t>(num_elements)];

  // 核心操作：提取元素指针
  src.UnsafeArenaExtractSubrange(start,         // 起始索引
                                 num_elements,  // 提取数量
                                 extracted      // 输出参数: 接收指针的数组
  );

  // 转移所有权到目标容器
  for (int i = 0; i < num_elements; ++i) {
    dst.AddAllocated(extracted[i]);
  }

  delete[] extracted;  // 仅删除指针数组，不删除元素本身
  return dst;
}

repeated_entry limit_entry_size(repeated_entry& entries, std::uint64_t max_size) {
  if (entries.empty()) {
    return entries;
  }
  std::size_t size = entries[0].ByteSizeLong();
  int i = 1;
  for (; i < entries.size(); ++i) {
    size += entries[i].ByteSizeLong();
    if (size > max_size) {
      break;
      ;
    }
  }
  return extract_range_without_copy(entries, 0, i);
}

void assert_conf_states_equivalent(const raftpb::conf_state& lhs, const raftpb::conf_state& rhs) {
  auto result = leaf::try_handle_some(
      [&]() -> leaf::result<void> {
        BOOST_LEAF_CHECK(conf_state_equivalent(lhs, rhs));
        return {};
      },
      [&](const lepton::lepton_error& err) -> leaf::result<void> {
        LEPTON_CRITICAL("conf states mismatch: {}", err.message);
        return new_error(err);
      });
  assert(!result);
}

bool operator==(const raftpb::hard_state& lhs, const raftpb::hard_state& rhs) {
  return lhs.term() == rhs.term() && lhs.vote() == rhs.vote() && lhs.commit() == rhs.commit();
}

bool is_empty_hard_state(const raftpb::hard_state& hs) { return hs == EMPTY_STATE; }

raftpb::conf_change_v2 convert_conf_change_v2(raftpb::conf_change&& cc) {
  raftpb::conf_change_v2 obj;
  auto changes = obj.add_changes();
  changes->set_type(cc.type());
  changes->set_node_id(cc.node_id());
  obj.set_allocated_context(cc.release_context());
  return obj;
}

}  // namespace pb

}  // namespace lepton
