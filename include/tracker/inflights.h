#ifndef _LEPTON_INFLIGHTS_H_
#define _LEPTON_INFLIGHTS_H_
#include <cstdint>
#include <deque>

#include "log.h"
#include "utility_macros.h"
namespace lepton {
namespace tracker {
// Inflights limits the number of MsgApp (represented by the largest index
// contained within) sent to followers but not yet acknowledged by them. Callers
// use Full() to check whether more messages can be sent, call Add() whenever
// they are sending a new append, and release "quota" via FreeLE() whenever an
// ack is received.
// 用来限制未确认的日志消息（MsgApp）的数量，确保在向跟随者发送新的日志条目时，不会超出消息队列的容量。它充当了一个“缓冲区”，用于跟踪已经发送但尚未得到确认的日志消息。
// 也就是一个环形队列实现
class inflights {
  NOT_COPYABLE(inflights)
  inflights(size_t size, std::deque<std::uint64_t> &&buffer) : capacity_(size), buffer_(buffer) {}

 public:
  // NewInflights sets up an Inflights that allows up to 'size' inflight
  // messages.
  inflights(size_t size) : capacity_(size) {}

  inflights(inflights &&) = default;

  inflights clone() const {
    std::deque<std::uint64_t> buffer = buffer_;
    return inflights{capacity_, std::move(buffer)};
  }

  bool full() const { return buffer_.size() == capacity_; }

  void add(std::uint64_t inflight) {
    if (full()) {
      LEPTON_CRITICAL("cannot add into a Full inflights");
      return;
    }
    buffer_.push_back(inflight);
  }

  void free_le(std::uint64_t to) {
    if (buffer_.empty() || (to < buffer_.front())) {
      return;
    }
    while (!buffer_.empty()) {
      auto front = buffer_.front();
      if (to < front) {
        break;
      }
      buffer_.pop_front();
    }
  }

  void free_first_one() {
    if (buffer_.empty()) {  // 在空容器释放元素是未定义的
      return;
    }
    buffer_.pop_front();
  }

  auto count() const { return buffer_.size(); }

  auto empty() const { return buffer_.empty(); }

  void reset() { buffer_.clear(); }

  auto capacity() { return capacity_; }

  const std::deque<std::uint64_t> &buffer_view() { return buffer_; }

 private:
  // the size of the buffer
  size_t capacity_;

  // buffer contains the index of the last entry
  // inside one message.
  std::deque<std::uint64_t> buffer_;
};
}  // namespace tracker
}  // namespace lepton

#endif  // _LEPTON_INFLIGHTS_H_
