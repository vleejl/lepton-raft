#include "basic/logger.h"

#include "basic/spdlog_logger.h"

namespace lepton {
// 内部使用的锁和指针
static std::mutex& logger_mutex() {
  static std::mutex mtx;
  return mtx;
}

// 获取全局指针
static std::shared_ptr<logger_interface>& global_logger_ptr() {
  static std::shared_ptr<logger_interface> instance;
  return instance;
}

// 手动初始化（也是线程安全的）
void set_default_logger(std::shared_ptr<logger_interface> l) {
  std::lock_guard<std::mutex> lock(logger_mutex());
  global_logger_ptr() = std::move(l);
}

// 实现 default_logger
logger_interface& default_logger() {
  auto& ptr = global_logger_ptr();
  // 双重检查锁定 (DCLP)
  if (!ptr) {
    std::lock_guard<std::mutex> lock(logger_mutex());
    if (!ptr) {
      // 此时 spdlog_logger 已在上方定义完毕
      ptr = std::make_shared<spdlog_logger>();
    }
  }
  return *ptr;
}
}  // namespace lepton