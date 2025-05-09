#ifndef _LEPTON_INFLIGHTS_H_
#define _LEPTON_INFLIGHTS_H_
#include <cassert>
#include <cstdint>
#include <deque>

#include "error.h"
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
struct inflights_data {
  std::uint64_t index;
  std::uint64_t bytes;
  auto operator<=>(const inflights_data &) const = default;
};
class inflights {
  inflights(size_t size, std::deque<inflights_data> &&buffer) : capacity_(size), buffer_(buffer) {}

 public:
  MOVABLE_BUT_NOT_COPYABLE(inflights)
  inflights(size_t size) : capacity_(size) {}
  // NewInflights sets up an Inflights that allows up to 'size' inflight
  // messages.
  inflights(size_t size, std::uint64_t max_bytes) : capacity_(size), max_bytes_(max_bytes) {}

  inflights clone() const {
    std::deque<inflights_data> buffer = buffer_;
    return inflights{capacity_, std::move(buffer)};
  }

  bool full() const { return buffer_.size() == capacity_ || ((max_bytes_ != 0) && (bytes_ >= max_bytes_)); }

  void add(std::uint64_t inflight, std::uint64_t bytes) {
    if (full()) {
      panic("cannot add into a Full inflights");
      return;
    }
    bytes_ += bytes;
    buffer_.push_back(inflights_data{inflight, bytes});
  }

  void free_le(std::uint64_t to) {
    if (buffer_.empty() || (to < buffer_.front().index)) {
      return;
    }
    while (!buffer_.empty()) {
      auto front = buffer_.front();
      if (to < front.index) {
        break;
      }
      assert(bytes_ >= front.bytes);
      bytes_ -= front.bytes;
      buffer_.pop_front();
    }
  }

  void free_first_one() {
    if (buffer_.empty()) {  // 在空容器释放元素是未定义的
      return;
    }
    auto front = buffer_.front();
    assert(bytes_ >= front.bytes);
    bytes_ -= front.bytes;
    buffer_.pop_front();
  }

  auto count() const { return buffer_.size(); }

  auto bytes() const { return bytes_; }

  auto empty() const { return buffer_.empty(); }

  void reset() {
    buffer_.clear();
    bytes_ = 0;
  }

  auto capacity() const { return capacity_; }

  auto max_bytes() const { return max_bytes_; }

  const std::deque<inflights_data> &buffer_view() { return buffer_; }

 private:
  // the max number of inflight messages
  size_t capacity_ = 0;
  // the max total byte size of inflight messages
  std::uint64_t max_bytes_ = 0;

  // number of inflight bytes
  std::uint64_t bytes_ = 0;

  // buffer contains the index of the last entry
  // inside one message.
  std::deque<inflights_data> buffer_;
};
}  // namespace tracker
}  // namespace lepton

#endif  // _LEPTON_INFLIGHTS_H_
