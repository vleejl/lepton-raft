#ifndef _LEPTON_TEST_CLI_H_
#define _LEPTON_TEST_CLI_H_

#include <map>
#include <string>
#include <vector>

std::vector<std::string> get_test_files(const std::string& dir);
std::map<std::string, std::vector<std::string>> parse_command_line(const std::string& cmd, const std::string& line);

#endif  // _LEPTON_TEST_CLI_H_
