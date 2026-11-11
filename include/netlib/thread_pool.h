#ifndef NETLIB_INCLUDE_NETLIB_THREAD_POOL_H_
#define NETLIB_INCLUDE_NETLIB_THREAD_POOL_H_

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>

#include "netlib/frame_message.h"

namespace netlib {

// Processes complete frames on a moodycamel MPMC queue. Handler calls run
// concurrently and make no ordering guarantee across connections.
class ThreadPool {
 private:
  class Impl;

 public:
  using Handler = std::function<void(FrameMessage)>;

  // One explicit producer for a persistent submission source. Sequential use
  // from different threads is allowed; concurrent use requires external
  // synchronization. Keeps its queue alive and rejects submissions after Stop.
  class Submitter {
   public:
    Submitter();
    Submitter(Submitter&&) noexcept;
    Submitter& operator=(Submitter&&) noexcept;
    Submitter(const Submitter&) = delete;
    Submitter& operator=(const Submitter&) = delete;
    ~Submitter();

    bool valid() const;
    bool Submit(FrameMessage message);

   private:
    class State;
    friend class ThreadPool;
    explicit Submitter(std::unique_ptr<State> state);
    std::unique_ptr<State> state_;
  };

  static std::unique_ptr<ThreadPool> Create(
      std::size_t worker_count, std::size_t max_pending_messages,
      Handler handler, std::size_t max_retained_bytes = 64 * 1024 * 1024);

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;
  ThreadPool(ThreadPool&&) = delete;
  ThreadPool& operator=(ThreadPool&&) = delete;
  ~ThreadPool();

  // Thread-safe. False means the pool is stopping or its queue is full.
  bool Submit(FrameMessage message);
  Submitter CreateSubmitter();
  std::size_t worker_count() const;
  // Closes admission, waits for accepted handlers, then sends implicit stop
  // messages and joins workers. Must not be called from a handler in this pool.
  void Stop();
  std::size_t queued_count() const;
  // Includes accepted requests still owned by handlers, even after
  // cancellation.
  std::size_t retained_bytes() const;

 private:
  friend class SubReactor;
  // Only a worker of this pool has an index; other threads use implicit
  // enqueue.
  static std::optional<std::size_t> CurrentWorkerIndex(
      const std::shared_ptr<Impl>& pool);

  explicit ThreadPool(std::shared_ptr<Impl> impl);

  std::shared_ptr<Impl> impl_;
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_THREAD_POOL_H_
