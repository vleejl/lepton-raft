#pragma once
#include <sstream>
#include <stdexcept>
#include <string>

namespace rafttest {

class redirect_logger {
 public:
  enum level {
    DEBUG = 0,
    INFO = 1,
    WARN = 2,
    ERROR = 3,
    FATAL = 4,
    NONE = 5,
  };

 private:
  std::ostringstream* builder_;  // ⭐ 指向外部 buffer
  level lvl_;

  static constexpr const char* lvl_names_[6] = {"DEBUG", "INFO", "WARN", "ERROR", "FATAL", "NONE"};

  bool quiet() const noexcept { return lvl_ == NONE; }

  void vprint(level msg_lvl, const std::string& msg) {
    if (lvl_ <= msg_lvl && !quiet() && builder_) {
      (*builder_) << lvl_names_[msg_lvl] << " " << msg;
      if (!msg.empty() && msg.back() != '\n') (*builder_) << "\n";
    }
  }

 public:
  explicit redirect_logger(level lvl = INFO, std::ostringstream* buf = nullptr) : builder_(buf), lvl_(lvl) {}

  void set_level(level lvl) { lvl_ = lvl; }
  level get_level() const noexcept { return lvl_; }

  void set_builder(std::ostringstream* buf) { builder_ = buf; }
  std::ostringstream* get_builder() const { return builder_; }

  std::string str() const { return builder_ ? builder_->str() : ""; }

  // log functions
  template <typename... Args>
  void debug(Args&&... args) {
    print(DEBUG, std::forward<Args>(args)...);
  }

  template <typename... Args>
  void info(Args&&... args) {
    print(INFO, std::forward<Args>(args)...);
  }

  template <typename... Args>
  void warn(Args&&... args) {
    print(WARN, std::forward<Args>(args)...);
  }

  template <typename... Args>
  void error(Args&&... args) {
    print(ERROR, std::forward<Args>(args)...);
  }

  template <typename... Args>
  [[noreturn]] void fatal(Args&&... args) {
    print(FATAL, std::forward<Args>(args)...);
    throw std::runtime_error(join(std::forward<Args>(args)...));
  }

  template <typename... Args>
  [[noreturn]] void panic(Args&&... args) {
    print(FATAL, std::forward<Args>(args)...);
    throw std::runtime_error(join(std::forward<Args>(args)...));
  }

 private:
  template <typename... Args>
  void print(level msg_lvl, Args&&... args) {
    if (lvl_ <= msg_lvl && !quiet() && builder_) {
      (*builder_) << lvl_names_[msg_lvl] << " " << join(std::forward<Args>(args)...) << "\n";
    }
  }

  template <typename... Args>
  std::string join(Args&&... args) {
    std::ostringstream oss;
    (oss << ... << args);
    return oss.str();
  }
};

constexpr const char* redirect_logger::lvl_names_[6];

}  // namespace rafttest
