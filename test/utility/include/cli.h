#include <map>
#include <string>
#include <vector>

std::vector<std::string> get_test_files(const std::string& dir);
std::map<std::string, std::vector<std::string>> parse_command_line(
    const std::string& cmd, const std::string& line);