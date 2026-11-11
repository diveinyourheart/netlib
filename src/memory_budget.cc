#include "netlib/memory_budget.h"

#include <cassert>
#include <utility>

namespace netlib {

bool MemoryBudget::TryAcquire(std::size_t bytes) {
  std::size_t used = used_.load(std::memory_order_relaxed);
  while (bytes <= limit_ - used) {
    if (used_.compare_exchange_weak(used, used + bytes,
                                    std::memory_order_relaxed))
      return true;
  }
  return false;
}

void MemoryBudget::Release(std::size_t bytes) {
  [[maybe_unused]] const std::size_t previous =
      used_.fetch_sub(bytes, std::memory_order_relaxed);
  assert(previous >= bytes);
}

std::shared_ptr<MemoryReservation> MemoryReservation::Acquire(
    std::shared_ptr<MemoryBudget> budget, std::size_t bytes) {
  if (!budget) return nullptr;
  std::shared_ptr<MemoryReservation> result(
      new MemoryReservation(std::move(budget)));
  return result->Resize(bytes) ? result : nullptr;
}

MemoryReservation::~MemoryReservation() { budget_->Release(bytes_); }

bool MemoryReservation::Resize(std::size_t bytes) {
  if (bytes > bytes_ && !budget_->TryAcquire(bytes - bytes_)) return false;
  if (bytes < bytes_) budget_->Release(bytes_ - bytes);
  bytes_ = bytes;
  return true;
}

}  // namespace netlib
