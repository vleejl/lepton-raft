#pragma once
#ifndef _LEPTON_TEST_DATA_DRIVEN_H_
#define _LEPTON_TEST_DATA_DRIVEN_H_
#include <gtest/gtest.h>

#include <cassert>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "cli.h"
#include "fmt/format.h"
#include "functional"
#include "test_utility_data.h"

namespace datadriven {

// TestData contains information about one data-driven test case that was
// parsed from the test file.
struct test_data {
  // Cmd is the first string on the directive line (up to the first whitespace).
  std::string cmd;
  // Input is the text between the first directive line and the ---- separator.
  std::string input;
  // CmdArgs contains the k/v arguments to the command.
  std::vector<cmd_arg> cmd_args;
};

// ScanArgs looks up the first CmdArg matching the given key and scans it into
// the given destinations in order. If the arg does not exist, the number of
// destinations does not match that of the arguments, or a destination can not
// be populated from its matching value, a fatal error results.
// If the arg exists multiple times, the first occurrence is parsed.
//
// For example, for a TestData originating from
//
// cmd arg1=50 arg2=yoruba arg3=(50, 50, 50)
//
// the following would be valid:
//
// var i1, i2, i3, i4 int
// var s string
// td.ScanArgs(t, "arg1", &i1)
// td.ScanArgs(t, "arg2", &s)
// td.ScanArgs(t, "arg3", &i2, &i3, &i4)
inline void scan_first_args(const test_data& td, const std::string& key, std::uint64_t& dests) {
  bool found = false;
  for (const auto& arg : td.cmd_args) {
    if (arg.key_ == key) {
      found = true;
      assert(arg.vals_.size() == 1);
      auto err = arg.scan_err(0, dests);
      assert(err);
      break;
    }
  }
  assert(found);  // key not found
}

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
  using process_func = std::function<std::string(const datadriven::test_data&)>;

  using process_file_func = std::function<void(const std::string&)>;

 private:
  void run_test_case(process_func process_test_case_func, parse_result& result, std::size_t& line_no) {
    auto test_expected_result = result.test_expected_result_stream.str();
    std::cout << "current test_file: " << test_file_ << std::endl;
    std::cout << "current process result line no: " << line_no << std::endl;
    auto test_actual_result = process_test_case_func(result.test_data);
    ensure_new_line(test_actual_result);
    if (test_expected_result != test_actual_result) {
      if (test_expected_result.find('\n') != std::string::npos) {
        std::cout << "expected result:\n" << test_expected_result;
      } else {
        std::cout << "expected result:\n" << test_expected_result << std::endl;
      }
      std::cout << "actual result:\n" << test_actual_result << std::endl;
      ASSERT_EQ(test_actual_result, test_expected_result) << fmt::format("line no: {}", line_no);
    }
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
        ss >> result.test_data.cmd;
        result.test_data.cmd_args = parse_command_line(result.test_data.cmd, line);
        result.state = parse_state::PROCESS_INPUT;
      } else {
        result.test_data.input = line;
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

  void run_file(data_driven::process_file_func process_file_func) {
    std::vector<std::string> test_files = get_test_files(dir_);
    for (const auto& test_file : test_files) {
      process_file_func(test_file);
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

#endif