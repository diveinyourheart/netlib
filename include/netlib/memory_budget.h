#ifndef NETLIB_INCLUDE_NETLIB_MEMORY_BUDGET_H_
#define NETLIB_INCLUDE_NETLIB_MEMORY_BUDGET_H_

#include <atomic>
#include <cstddef>
#include <memory>
#include <utility>

namespace netlib {

// Shared across the owner, queues and asynchronous handlers. Counts retained
// allocation capacity, not process RSS or memory independently allocated by
// apps.
class MemoryBudget {
 public:
  explicit MemoryBudget(std::size_t limit) : limit_(limit) {}
  bool TryAcquire(std::size_t bytes);
  void Release(std::size_t bytes);
  std::size_t used() const { return used_.load(std::memory_order_relaxed); }
  std::size_t limit() const { return limit_; }

 private:
  const std::size_t limit_;
  std::atomic<std::size_t> used_{0};
};

// Copies of a message share its charge. Only its current library owner resizes
// the reservation; cancellation never releases bytes still held by a Worker.
class MemoryReservation {
 public:
  static std::shared_ptr<MemoryReservation> Acquire(
      std::shared_ptr<MemoryBudget> budget, std::size_t bytes);
  ~MemoryReservation();
  MemoryReservation(const MemoryReservation&) = delete;
  MemoryReservation& operator=(const MemoryReservation&) = delete;
  bool Resize(std::size_t bytes);
  std::size_t size() const { return bytes_; }
  const std::shared_ptr<MemoryBudget>& budget() const { return budget_; }

 private:
  explicit MemoryReservation(std::shared_ptr<MemoryBudget> budget)
      : budget_(std::move(budget)) {}
  const std::shared_ptr<MemoryBudget> budget_;
  std::size_t bytes_ = 0;
};

}  // namespace netlib
#endif  // NETLIB_INCLUDE_NETLIB_MEMORY_BUDGET_H_
