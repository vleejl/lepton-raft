#include <gtest/gtest.h>

#include <cassert>
#include <fstream>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "cli.h"
#include "functional"

namespace datadriven {
// TestData contains information about one data-driven test case that was
// parsed from the test file.
struct test_data {
  // Cmd is the first string on the directive line (up to the first whitespace).
  std::string cmd;
  // Input is the text between the first directive line and the ---- separator.
  std::string input;
  // CmdArgs contains the k/v arguments to the command.
  std::map<std::string, std::vector<std::string>> args_map;
};
}  // namespace datadriven

enum class parse_state { INIT, PROCESS_INPUT, PROCESS_EXPECTED };

struct parse_result {
  parse_result(parse_state state) : state(state) {}

  parse_state state;
  datadriven::test_data test_data;
  // std::ostringstream test_input_stream;
  std::ostringstream test_expected_result_stream;
};

class data_driven {
 public:
  using process_func = std::function<std::string(const std::string&, const std::string&,
                                                 const std::map<std::string, std::vector<std::string>>&)>;

 private:
  void run_test_case(process_func process_test_case_func, parse_result& result, std::size_t& line_no) {
    auto test_expected_result = result.test_expected_result_stream.str();
    std::cout << "current test_file: " << test_file_ << std::endl;
    std::cout << "current process result line no: " << line_no << std::endl;
    std::cout << "expected result:\n" << test_expected_result << std::endl;
    auto test_actual_result = process_test_case_func(result.cmd, result.input, result.args_map);
    std::cout << "actual result:\n" << test_actual_result << std::endl;
    ASSERT_EQ(test_actual_result, test_expected_result);
    result = parse_result{parse_state::INIT};
  }

 public:
  data_driven(const std::string& filename) : test_file_(filename) {}

  void run(process_func process_test_case_func) {
    std::ifstream file(test_file_);
    if (!file.is_open()) {
      std::cerr << "Failed to open test file: " << test_file_ << std::endl;
      assert(false);
      return;
    }

    std::size_t line_no = 0;
    std::string line;
    parse_result result{parse_state::INIT};
    while (std::getline(file, line)) {
      ++line_no;
      if (line.empty() || line[0] == '#') {
        if (result.state == parse_state::INIT) {
          continue;
        }
        if (result.state != parse_state::PROCESS_EXPECTED) {
          assert(false);  // 状态机出现异常
          return;
        }
        run_test_case(process_test_case_func, result, line_no);
        continue;
      }

      if (line == "----") {
        result.state = parse_state::PROCESS_EXPECTED;
        continue;
      }

      if (result.state == parse_state::PROCESS_EXPECTED) {
        result.test_expected_result_stream << line << "\n";
        continue;
      }

      if (result.state == parse_state::INIT) {
        std::stringstream ss(line);
        ss >> result.cmd;
        result.args_map = parse_command_line(result.cmd, line);
        result.state = parse_state::PROCESS_INPUT;
      } else {
        result.input = line;
        result.state = parse_state::PROCESS_EXPECTED;
      }
    }
    // last test case
    run_test_case(process_test_case_func, result, line_no);
    assert(result.state == parse_state::INIT);
  }

 private:
  std::string test_file_;
};

class data_driven_group {
 public:
  data_driven_group(const std::string& dir) : dir_(dir) {}
  void run(data_driven::process_func process_test_case_func) {
    // List all test files in the specified directory
    std::vector<std::string> test_files = get_test_files(dir_);
    for (const auto& test_file : test_files) {
      data_driven runner{test_file};
      runner.run(process_test_case_func);
    }
  }

 private:
  std::string dir_;
};

class defer {
 public:
  // 构造函数接收一个可调用对象（lambda）
  defer(std::function<void()> func) : func_(std::move(func)) {}

  // 析构时调用保存的函数，模拟 Go 中的 defer
  ~defer() { func_(); }

 private:
  std::function<void()> func_;
};