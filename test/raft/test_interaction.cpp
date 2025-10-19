#include <fmt/core.h>
#include <gtest/gtest.h>
#include <proxy.h>

#include <cassert>
#include <cstdio>
#include <filesystem>
#include <string>

#include "interaction_env.h"
#include "test_raft_utils.h"
using namespace lepton;

static std::string process_single_test_case(const datadriven::test_data& test_data) {
  interaction::interaction_env env{
      interaction::interaction_opts{.set_randomized_election_timeout = set_randomized_election_timeout_for_raw_node}};
  return env.handle(test_data);
}

TEST(interaction_test_suit, test_data_driven_impl) {
  // // 获取当前文件的路径
  // std::filesystem::path current_file = __FILE__;

  // // 获取当前文件所在目录
  // std::filesystem::path current_dir = current_file.parent_path();

  // // 拼接 "testdata" 目录
  // std::filesystem::path project_dir = LEPTON_PROJECT_DIR;
  // project_dir = project_dir.make_preferred();

  // // List all test files in the specified directory
  // // 先构建完整路径对象
  // std::filesystem::path full_path = project_dir / current_dir / "testdata";
  // // 再显式转换为字符串
  // std::string dir_path = full_path.string();
  // data_driven_group group{dir_path};
  // data_driven::process_func func = process_single_test_case;
  // group.run(func);
}