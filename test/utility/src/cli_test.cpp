#include "cli.h"

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

TEST(parse_command_line_tests, basic_key_value) {
  std::string line = "committed cfg=(1, 2) idx=(_,_)";
  std::map<std::string, std::vector<std::string>> result =
      parse_command_line("committed", line);

  ASSERT_EQ(result.size(), 2);
  auto cfg_expected = std::vector<std::string>{"1", "2"};
  EXPECT_EQ(result["cfg"], cfg_expected);
  auto idx_expected = std::vector<std::string>{"_", "_"};
  EXPECT_EQ(result["idx"], idx_expected);
}

TEST(parse_command_line_tests, single_value) {
  std::string line = "committed cfg=1 idx=2";
  std::map<std::string, std::vector<std::string>> result =
      parse_command_line("committed", line);

  ASSERT_EQ(result.size(), 2);

  std::vector<std::string> cfg_expected = {"1"};
  EXPECT_EQ(result["cfg"], cfg_expected);

  std::vector<std::string> idx_expected = {"2"};
  EXPECT_EQ(result["idx"], idx_expected);
}

TEST(parse_command_line_tests, space_in_brackets) {
  std::string line = "committed cfg=(1, 2) idx=(_, _)";
  std::map<std::string, std::vector<std::string>> result =
      parse_command_line("committed", line);

  ASSERT_EQ(result.size(), 2);

  std::vector<std::string> cfg_expected = {"1", "2"};
  EXPECT_EQ(result["cfg"], cfg_expected);

  std::vector<std::string> idx_expected = {"_", "_"};
  EXPECT_EQ(result["idx"], idx_expected);
}

TEST(parse_command_line_tests, empty_bracket) {
  std::string line = "committed cfg=() idx=()";
  std::map<std::string, std::vector<std::string>> result =
      parse_command_line("committed", line);

  ASSERT_EQ(result.size(), 2);

  std::vector<std::string> cfg_expected = {};
  EXPECT_EQ(result["cfg"], cfg_expected);

  std::vector<std::string> idx_expected = {};
  EXPECT_EQ(result["idx"], idx_expected);
}

TEST(parse_command_line_tests, multiple_values_with_spaces) {
  std::string line = "committed cfg=(1,  2 , 3) idx=( 4 , 5 )";
  std::map<std::string, std::vector<std::string>> result =
      parse_command_line("committed", line);

  ASSERT_EQ(result.size(), 2);

  std::vector<std::string> cfg_expected = {"1", "2", "3"};
  EXPECT_EQ(result["cfg"], cfg_expected);

  std::vector<std::string> idx_expected = {"4", "5"};
  EXPECT_EQ(result["idx"], idx_expected);
}

TEST(parse_command_line_tests, without_cmd) {
  std::string line = "cfg=(1, 2) idx=(_,_)";
  std::map<std::string, std::vector<std::string>> result =
      parse_command_line("committed", line);

  ASSERT_EQ(result.size(), 2);

  std::vector<std::string> cfg_expected = {"1", "2"};
  EXPECT_EQ(result["cfg"], cfg_expected);

  std::vector<std::string> idx_expected = {"_", "_"};
  EXPECT_EQ(result["idx"], idx_expected);
}

TEST(parse_command_line_tests, empty_input) {
  std::string line = "";
  std::map<std::string, std::vector<std::string>> result =
      parse_command_line("committed", line);

  ASSERT_EQ(result.size(), 0);
}

TEST(parse_command_line_tests, only_spaces) {
  std::string line = "     ";
  std::map<std::string, std::vector<std::string>> result =
      parse_command_line("committed", line);

  ASSERT_EQ(result.size(), 0);
}

TEST(parse_command_line_tests, only_cmd) {
  std::string line = "committed";
  std::map<std::string, std::vector<std::string>> result =
      parse_command_line("committed", line);

  ASSERT_EQ(result.size(), 0);
}
