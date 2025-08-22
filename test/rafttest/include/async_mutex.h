// async_mutex.hpp
#pragma once
#include <asio.hpp>
#include <asio/experimental/channel.hpp>

class async_mutex {
  using ch_t = asio::experimental::channel<void(asio::error_code)>;

 public:
  explicit async_mutex(asio::any_io_executor ex)
      : ch_(ex, 1)  // capacity = 1
  {
    // 预放入一个“令牌”表示“未加锁”
    asio::error_code ec;
    (void)ch_.try_send(ec);
  }

  // 不可拷贝，可移动
  async_mutex(const async_mutex&) = delete;
  async_mutex& operator=(const async_mutex&) = delete;
  async_mutex(async_mutex&&) = default;
  async_mutex& operator=(async_mutex&&) = default;

  // 协程获取锁：co_await async_lock(use_awaitable)
  template <typename CompletionToken>
  auto async_lock(CompletionToken&& token) {
    // 取走令牌 -> 获得锁；若无令牌则挂起等待
    return ch_.async_receive(std::forward<CompletionToken>(token));
  }

  // 释放锁：归还令牌
  void unlock() {
    asio::error_code ec;
    bool ok = ch_.try_send(ec);
    // 若 ok == false，说明重复 unlock 或并发错误；开发期可加 assert
    // assert(ok && !ec);
    (void)ok;
    (void)ec;
  }

  // （可选）尝试获取锁：成功返回 true，失败返回 false（不挂起）
  bool try_lock() {
    asio::error_code ec;
    // 取令牌（无令牌则返回 false）
    bool ok = ch_.try_receive(ec);
    return ok && !ec;
  }

  // RAII 守卫：析构自动 unlock（可 co_await 获得）
  class scoped_lock {
   public:
    scoped_lock() = default;
    explicit scoped_lock(async_mutex* m) : m_(m) {}
    scoped_lock(const scoped_lock&) = delete;
    scoped_lock& operator=(const scoped_lock&) = delete;
    scoped_lock(scoped_lock&& other) noexcept : m_(other.m_) { other.m_ = nullptr; }
    scoped_lock& operator=(scoped_lock&& other) noexcept {
      if (this != &other) {
        if (m_) m_->unlock();
        m_ = other.m_;
        other.m_ = nullptr;
      }
      return *this;
    }
    ~scoped_lock() {
      if (m_) m_->unlock();
    }
    bool owns_lock() const noexcept { return m_ != nullptr; }
    void unlock() {
      if (m_) {
        m_->unlock();
        m_ = nullptr;
      }
    }

   private:
    async_mutex* m_ = nullptr;
  };

  // 直接 co_await 拿到 RAII 守卫
  asio::awaitable<scoped_lock> co_scoped_lock() {
    co_await async_lock(asio::use_awaitable);
    co_return scoped_lock{this};
  }

 private:
  ch_t ch_;
};
