#include "majority.h"
#include "quorum.h"
#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <fmt/core.h>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
using namespace lepton;

// 定义测试命令结构体
struct TestData {
  std::string command;
  int arg1;
  int arg2;
  int expected_result;
};

// 解析测试用例文件，返回测试数据的向量
std::vector<TestData> parseTestData(const std::string &filename) {
  std::vector<TestData> test_data;

  std::ifstream file(filename);
  std::string line;
  TestData data;

  while (std::getline(file, line)) {
    std::istringstream iss(line);
    std::string cmd;

    if (iss >> cmd) {
      if (cmd == ">>>") {
        // 解析命令
        iss >> data.command >> data.arg1 >> data.arg2;
      } else if (cmd == "EXPECT" && data.command == "RESULT") {
        // 解析期望结果
        iss >> data.expected_result;
        test_data.push_back(data);
      }
    }
  }

  return test_data;
}

// 实际执行测试命令并验证结果
void runTest(const TestData &test) {
  int result = 0;

  if (test.command == "ADD") {
    result = test.arg1 + test.arg2;
  } else if (test.command == "SUBTRACT") {
    result = test.arg1 - test.arg2;
  } else {
    std::cerr << "Unknown command: " << test.command << std::endl;
    return;
  }

  // 检查结果是否符合预期
  if (result == test.expected_result) {
    std::cout << "PASS: " << test.command << " " << test.arg1 << " "
              << test.arg2 << " = " << result << std::endl;
  } else {
    std::cout << "FAIL: " << test.command << " " << test.arg1 << " "
              << test.arg2 << ", expected " << test.expected_result << ", got "
              << result << std::endl;
  }
}

TEST_CASE("test data driven", "[testdata]") {

  // Two majority configs. The first one is always used (though it may
  // be empty) and the second one is used iff joint is true.
  bool joint;
  std::vector<std::uint64_t> ids;
  std::vector<std::uint64_t> idsj;
  // The committed indexes for the nodes in the config in the order in
  // which they appear in (ids,idsj), without repetition. An underscore
  // denotes an omission (i.e. no information for this voter); this is
  // different from 0. For example,
  //
  // cfg=(1,2) cfgj=(2,3,4) idxs=(_,5,_,7) initializes the idx for voter
  // 2 to 5 and that for voter 4 to 7 (and no others).
  //
  // cfgj=zero is specified to instruct the test harness to treat cfgj
  // as zero instead of not specified (i.e. it will trigger a joint
  // quorum test instead of a majority quorum test for cfg only).
  std::vector<log_index> idxs;
  // Votes. These are initialized similar to idxs except the
  // only values used are 1 (voted against) and 2 (voted for).
  // This looks awkward, but is convenient because it allows
  // sharing code between the two.
  std::vector<log_index> votes;
  SECTION("testdata", "joint_commit") {
    std::vector<TestData> test_cases = parseTestData("./testdata/joint_commit");
    // 执行所有测试用例
    for (const auto &test : test_cases) {
      runTest(test);
    }
  }
}