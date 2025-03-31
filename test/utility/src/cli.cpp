#include "cli.h"

#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <filesystem>
#include <map>
#include <sstream>
#include <string>
#include <vector>
// Function to read files from a directory
std::vector<std::string> get_test_files(const std::string& dir) {
  std::vector<std::string> files;
  for (const auto& entry : std::filesystem::directory_iterator(dir)) {
    if (entry.is_regular_file() && entry.path().extension() == ".txt") {
      files.push_back(entry.path().string());
    }
  }
  std::sort(files.begin(), files.end());
  return files;
}

std::string join(const std::vector<std::string>& vec, const std::string& delimiter) {
  std::ostringstream oss;
  for (size_t i = 0; i < vec.size(); ++i) {
    oss << vec[i];
    if (i != vec.size() - 1) {
      oss << delimiter;  // 添加分隔符，避免最后一个元素后也有分隔符
    }
  }
  return oss.str();
}

std::string pre_parse_space_char(const std::string& cmd, const std::string& line) {
  std::stringstream ss(line);
  std::string token;
  std::vector<std::string> tokens;
  bool find_bracket = false;
  while (std::getline(ss, token, ' ')) {  // 按空格分割
    if (token == cmd) {
      continue;
    }

    if (token.find('(') != std::string::npos) {
      tokens.push_back(token);
      // 找不到右括号，贪心算法，一直找到右括号为止
      if (token.find(')') == std::string::npos) {
        find_bracket = true;
        continue;
      }
    }
    if (find_bracket) {
      if (token.find(')') != std::string::npos) {
        find_bracket = false;
      }
      tokens.back() += token;
      continue;
    }

    tokens.push_back(token);
  }
  return join(tokens, " ");
}

// 通用命令行解析函数
std::map<std::string, std::vector<std::string>> parse_command_line(const std::string& cmd, const std::string& line) {
  std::stringstream ss(pre_parse_space_char(cmd, line));
  std::string token;
  std::map<std::string, std::vector<std::string>> result;
  while (std::getline(ss, token, ' ')) {  // 按空格分割
    if (token == cmd) {
      continue;
    }
    if (token.find('=') != std::string::npos) {  // 只处理包含'='的部分
      size_t equal_pos = token.find('=');
      std::string key = token.substr(0, equal_pos);
      std::string value = token.substr(equal_pos + 1);

      // 如果值是用括号包围的数组
      if (value.front() == '(' && value.back() == ')') {
        // 去掉括号并按逗号分割
        value = value.substr(1, value.size() - 2);
        std::stringstream value_stream(value);
        std::string element;
        std::vector<std::string> values;
        while (std::getline(value_stream, element, ',')) {
          values.push_back(element);
        }
        result[key] = values;
      } else {
        // 如果没有括号，就是单一值
        result[key] = {value};
      }
    }
  }
  return result;
}
