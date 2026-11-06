#ifndef NETLIB_INCLUDE_NETLIB_THREAD_POOL_H_
#define NETLIB_INCLUDE_NETLIB_THREAD_POOL_H_

#include <cstddef>
#include <functional>
#include <memory>

#include "netlib/frame_message.h"
#include "netlib/log/logger_fwd.h"

namespace netlib {

// Processes complete frames on a moodycamel MPMC queue. Handler calls run
// concurrently and make no ordering guarantee across connections.
class ThreadPool {
 public:
  using Handler = std::function<void(FrameMessage)>;

  // logger is optional and must outlive the ThreadPool when non-null.
  static std::unique_ptr<ThreadPool> Create(std::size_t worker_count,
                                            std::size_t max_pending_messages,
                                            Handler handler,
                                            log::Logger* logger = nullptr);

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;
  ThreadPool(ThreadPool&&) = delete;
  ThreadPool& operator=(ThreadPool&&) = delete;
  ~ThreadPool();

  // Thread-safe. False means the pool is stopping or its queue is full.
  bool Submit(FrameMessage message);
  void Stop();
  std::size_t queued_count() const;

 private:
  class Impl;

  explicit ThreadPool(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_THREAD_POOL_H_
