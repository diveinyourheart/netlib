#include "netlib/memory_budget.h"

#include <atomic>
#include <cstdio>
#include <future>
#include <memory>
#include <stop_token>
#include <thread>
#include <vector>

#include "netlib/frame_message.h"
#include "netlib/thread_pool.h"

namespace {
bool TestReservations() {
  std::shared_ptr<netlib::MemoryBudget> budget =
      std::make_shared<netlib::MemoryBudget>(1024);
  std::shared_ptr<netlib::MemoryReservation> first =
      netlib::MemoryReservation::Acquire(budget, 1024);
  if (!first || first->Resize(1025) || budget->used() != 1024 ||
      netlib::MemoryReservation::Acquire(budget, 1))
    return false;
  if (!first->Resize(0) || budget->used() != 0) return false;
  std::atomic<bool> success{true};
  std::vector<std::thread> threads;
  for (int i = 0; i < 8; ++i) {
    threads.emplace_back([&] {
      for (int n = 0; n < 10000; ++n) {
        std::shared_ptr<netlib::MemoryReservation> lease =
            netlib::MemoryReservation::Acquire(budget, 128);
        if (budget->used() > budget->limit()) success.store(false);
      }
    });
  }
  for (std::thread& thread : threads) thread.join();
  return success.load() && budget->used() == 0;
}

bool TestWorkerLifetime() {
  std::promise<netlib::FrameMessage> received;
  std::future<netlib::FrameMessage> future = received.get_future();
  std::atomic<int> calls{0};
  constexpr std::size_t kPayloadCapacity = 4096;
  const std::size_t limit = sizeof(netlib::FrameMessage) + kPayloadCapacity;
  std::unique_ptr<netlib::ThreadPool> pool = netlib::ThreadPool::Create(
      1, 8,
      [&](netlib::FrameMessage message) {
        if (calls.fetch_add(1) == 0) received.set_value(std::move(message));
      },
      limit);
  if (!pool) return false;
  std::stop_source cancel;
  netlib::FrameMessage request;
  request.payload.reserve(kPayloadCapacity);
  request.payload.push_back('q');
  request.stop_token = cancel.get_token();
  if (!pool->Submit(std::move(request))) return false;
  if (future.wait_for(std::chrono::seconds(2)) != std::future_status::ready)
    return false;
  netlib::FrameMessage held = future.get();
  cancel.request_stop();
  bool success = held.IsCancelled() && pool->retained_bytes() == limit &&
                 pool->queued_count() == 0 &&
                 !pool->Submit(netlib::FrameMessage{});
  // A completed handler has retained the request asynchronously. Releasing its
  // final message, rather than cancelling or dequeuing, returns the byte
  // credit.
  held = netlib::FrameMessage{};
  success = success && pool->retained_bytes() == 0 &&
            pool->Submit(netlib::FrameMessage{});
  pool->Stop();
  return success && calls.load() == 2 && pool->retained_bytes() == 0;
}
}  // namespace

int main() {
  if (!TestReservations() || !TestWorkerLifetime()) {
    std::fprintf(stderr, "memory reservation/lifetime test failed\n");
    return 1;
  }
  return 0;
}
