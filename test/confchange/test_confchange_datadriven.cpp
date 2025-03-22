#include <absl/strings/str_split.h>
#include <fmt/core.h>
#include <gtest/gtest.h>
#include <raft.pb.h>

#include <cassert>
#include <cstdio>
#include <filesystem>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "confchange.h"
#include "data_driven.h"
#include "error.h"
#include "fmt/format.h"
#include "leaf.hpp"
#include "progress.h"
#include "tracker.h"
#include "utility_data_test.h"
using namespace lepton;

// The test files use the commands
// - simple: run a simple conf change (i.e. no joint consensus),
// - enter-joint: enter a joint config, and
// - leave-joint: leave a joint config.
// The first two take a list of config changes, which have the following
// syntax:
// - vn: make n a voter,
// - ln: make n a learner,
// - rn: remove n, and
// - un: update n.
static leaf::result<std::string> process_single_test_case(
    const std::string& cmd, const std::string& input,
    const std::map<std::string, std::vector<std::string>>& args_map,
    confchange::changer& c) {
  defer func([&c]() { c.increase_last_index(); });
  std::vector<raftpb::conf_change_single> ccs;
  std::vector<std::string> toks = absl::StrSplit(input, ' ', absl::SkipEmpty());

  for (const auto& tok : toks) {
    if (tok.size() < 2) {
      return fmt::format("unknown token {}", tok);
    }
    raftpb::conf_change_single cc;
    switch (tok[0]) {
      case 'v':
        cc.set_type(raftpb::CONF_CHANGE_ADD_NODE);
        break;
      case 'l':
        cc.set_type(raftpb::CONF_CHANGE_ADD_LEARNER_NODE);
        break;
      case 'r':
        cc.set_type(raftpb::CONF_CHANGE_REMOVE_NODE);
        break;
      case 'u':
        cc.set_type(raftpb::CONF_CHANGE_UPDATE_NODE);
        break;
      default:
        return fmt::format("unknown input: {}", tok);
    }
    std::string_view view(tok.data() + 1, tok.size() - 1);
    auto _result = safe_stoull(view);
    if (!_result.has_value()) {
      assert(_result.has_value());
    }
    cc.set_node_id(_result.value());
    ccs.push_back(cc);
  }

  tracker::config cfg;
  tracker::progress_map prs;
  if (cmd == "simple") {
    BOOST_LEAF_AUTO(v, c.simple(ccs));
    auto& [cfg, prs] = v;
    c.update_tracker_config(std::move(cfg));
    c.update_tracker_progress(std::move(prs));
  } else if (cmd == "enter-joint") {
    auto auto_leave = false;
    auto iter = args_map.find("autoleave");
    if (iter != args_map.end()) {
      assert(!iter->second.empty());
      auto_leave = string_to_bool(iter->second[0]);
    }
    BOOST_LEAF_AUTO(v, c.enter_joint(auto_leave, ccs));
    auto& [cfg, prs] = v;
    c.update_tracker_config(std::move(cfg));
    c.update_tracker_progress(std::move(prs));
  } else if (cmd == "leave-joint") {
    if (!ccs.empty()) {
      return "this command takes no input";
    } else {
      BOOST_LEAF_AUTO(v, c.leave_joint());
      auto& [cfg, prs] = v;
      c.update_tracker_config(std::move(cfg));
      c.update_tracker_progress(std::move(prs));
    }
  } else {
    return "unknown command";
  }
  return fmt::format("{}\n{}", c.config_view().string(),
                     c.progress_view().string());
}

TEST(confchange_data_driven_test_suit, test_data_driven_impl) {
  // 获取当前文件的路径
  std::filesystem::path current_file = __FILE__;

  // 获取当前文件所在目录
  std::filesystem::path current_dir = current_file.parent_path();

  // 拼接 "testdata" 目录
  std::filesystem::path project_dir = LEPTON_PROJECT_DIR;
  std::string test_dir = (current_dir / "testdata").string();

  std::vector<std::string> test_files = get_test_files(project_dir / test_dir);
  for (const auto& test_file : test_files) {
    confchange::changer c{
        tracker::progress_tracker{10},
        0  // incremented in this test with each cmd
    };
    data_driven runner{test_file};
    auto func =
        [&c](const std::string& cmd, const std::string& input,
             const std::map<std::string, std::vector<std::string>>& args_map)
        -> std::string {
      leaf::result<std::string> r = leaf::try_handle_some(
          [&]() -> leaf::result<std::string> {
            BOOST_LEAF_AUTO(v,
                            process_single_test_case(cmd, input, args_map, c));
            return v;
          },
          [](const lepton_error& e) -> leaf::result<std::string> {
            return std::string(e.message) + '\n';
          });
      if (r.has_error()) {
        auto a = 1;
      }
      assert(!r.has_error());
      return r.value();
    };
    runner.run(func);
  }
}