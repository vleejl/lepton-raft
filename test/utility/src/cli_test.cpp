#include "cli.h"

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

TEST(parse_command_line_test_suit, basic_key_value) {
  std::string line = "committed cfg=(1, 2) idx=(_,_)";
  auto cmd_args = parse_command_line("committed", line);
  std::map<std::string, std::vector<std::string>> result;
  for (const auto& arg : cmd_args) {
    result[arg.key_] = arg.vals_;
  }

  ASSERT_EQ(result.size(), 2);
  auto cfg_expected = std::vector<std::string>{"1", "2"};
  EXPECT_EQ(result["cfg"], cfg_expected);
  auto idx_expected = std::vector<std::string>{"_", "_"};
  EXPECT_EQ(result["idx"], idx_expected);
}

TEST(parse_command_line_test_suit, single_value) {
  std::string line = "committed cfg=1 idx=2";
  auto cmd_args = parse_command_line("committed", line);
  std::map<std::string, std::vector<std::string>> result;
  for (const auto& arg : cmd_args) {
    result[arg.key_] = arg.vals_;
  }

  ASSERT_EQ(result.size(), 2);

  std::vector<std::string> cfg_expected = {"1"};
  EXPECT_EQ(result["cfg"], cfg_expected);

  std::vector<std::string> idx_expected = {"2"};
  EXPECT_EQ(result["idx"], idx_expected);
}

TEST(parse_command_line_test_suit, space_in_brackets) {
  std::string line = "committed cfg=(1, 2) idx=(_, _)";
  auto cmd_args = parse_command_line("committed", line);
  std::map<std::string, std::vector<std::string>> result;
  for (const auto& arg : cmd_args) {
    result[arg.key_] = arg.vals_;
  }

  ASSERT_EQ(result.size(), 2);

  std::vector<std::string> cfg_expected = {"1", "2"};
  EXPECT_EQ(result["cfg"], cfg_expected);

  std::vector<std::string> idx_expected = {"_", "_"};
  EXPECT_EQ(result["idx"], idx_expected);
}

TEST(parse_command_line_test_suit, empty_bracket) {
  std::string line = "committed cfg=() idx=()";
  auto cmd_args = parse_command_line("committed", line);
  std::map<std::string, std::vector<std::string>> result;
  for (const auto& arg : cmd_args) {
    result[arg.key_] = arg.vals_;
  }

  ASSERT_EQ(result.size(), 2);

  std::vector<std::string> cfg_expected = {};
  EXPECT_EQ(result["cfg"], cfg_expected);

  std::vector<std::string> idx_expected = {};
  EXPECT_EQ(result["idx"], idx_expected);
}

TEST(parse_command_line_test_suit, multiple_values_with_spaces) {
  std::string line = "committed cfg=(1,  2 , 3) idx=( 4 , 5 )";
  auto cmd_args = parse_command_line("committed", line);
  std::map<std::string, std::vector<std::string>> result;
  for (const auto& arg : cmd_args) {
    result[arg.key_] = arg.vals_;
  }

  ASSERT_EQ(result.size(), 2);

  std::vector<std::string> cfg_expected = {"1", "2", "3"};
  EXPECT_EQ(result["cfg"], cfg_expected);

  std::vector<std::string> idx_expected = {"4", "5"};
  EXPECT_EQ(result["idx"], idx_expected);
}

TEST(parse_command_line_test_suit, without_cmd) {
  std::string line = "cfg=(1, 2) idx=(_,_)";
  auto cmd_args = parse_command_line("committed", line);
  std::map<std::string, std::vector<std::string>> result;
  for (const auto& arg : cmd_args) {
    result[arg.key_] = arg.vals_;
  }

  ASSERT_EQ(result.size(), 2);

  std::vector<std::string> cfg_expected = {"1", "2"};
  EXPECT_EQ(result["cfg"], cfg_expected);

  std::vector<std::string> idx_expected = {"_", "_"};
  EXPECT_EQ(result["idx"], idx_expected);
}

TEST(parse_command_line_test_suit, empty_input) {
  std::string line = "";
  auto cmd_args = parse_command_line("committed", line);
  std::map<std::string, std::vector<std::string>> result;
  for (const auto& arg : cmd_args) {
    result[arg.key_] = arg.vals_;
  }

  ASSERT_EQ(result.size(), 0);
}

TEST(parse_command_line_test_suit, only_spaces) {
  std::string line = "     ";
  auto cmd_args = parse_command_line("committed", line);
  std::map<std::string, std::vector<std::string>> result;
  for (const auto& arg : cmd_args) {
    result[arg.key_] = arg.vals_;
  }

  ASSERT_EQ(result.size(), 0);
}

TEST(parse_command_line_test_suit, only_cmd) {
  std::string line = "committed";
  auto cmd_args = parse_command_line("committed", line);
  std::map<std::string, std::vector<std::string>> result;
  for (const auto& arg : cmd_args) {
    result[arg.key_] = arg.vals_;
  }

  ASSERT_EQ(result.size(), 0);
}
