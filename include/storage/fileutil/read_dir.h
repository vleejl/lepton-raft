#ifndef _LEPTON_READ_DIR_H_
#define _LEPTON_READ_DIR_H_
#include <functional>
#include <string>
#include <vector>

#include "lepton_error.h"
namespace lepton {

class read_dir_op {
 private:
  std::string ext_;
  std::string prefix_;
  bool sort_ = true;

 public:
  using option = std::function<void(read_dir_op*)>;

  read_dir_op() = default;

  void apply_opts(const std::vector<option>& opts) {
    for (const auto& opt : opts) {
      if (opt) {
        opt(this);
      }
    }
  }

  // 获取配置
  const std::string& ext() const { return ext_; }
  const std::string& prefix() const { return prefix_; }
  bool should_sort() const { return sort_; }

  static option with_ext(const std::string& ext) {
    return [ext](read_dir_op* op) { op->ext_ = ext; };
  }

  static option with_prefix(const std::string& prefix) {
    return [prefix](read_dir_op* op) { op->prefix_ = prefix; };
  }

  static option without_sort() {
    return [](read_dir_op* op) { op->sort_ = false; };
  }
};

leaf::result<std::vector<std::string>> read_dir(const std::string& directory,
                                                const std::vector<read_dir_op::option>& opts = {});

// 辅助函数：创建选项向量
template <typename... Options>
std::vector<read_dir_op::option> make_options(Options&&... options) {
  return {std::forward<Options>(options)...};
}

}  // namespace lepton

#endif  // _LEPTON_READ_DIR_H_
