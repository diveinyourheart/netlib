#include "netlib/thread_pool.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <latch>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "netlib/frame_message.h"
#include "netlib/log/logging.h"

namespace {

class CountingSink final : public netlib::log::LogSink {
 public:
  void Write(const netlib::log::LogRecord&) override {
    record_count_.fetch_add(1, std::memory_order_relaxed);
  }

  void Flush() override {}

  std::size_t record_count() const {
    return record_count_.load(std::memory_order_relaxed);
  }

 private:
  std::atomic<std::size_t> record_count_{0};
};

bool TestMixedProducersAndConcurrentStop() {
  constexpr std::size_t kExplicitProducers = 4;
  constexpr std::size_t kPerProducer = 256;
  constexpr std::size_t kTotal = 1 + (kExplicitProducers + 1) * kPerProducer;
  std::mutex mutex;
  std::condition_variable condition;
  bool entered = false;
  bool release = false;
  bool exactly_once = true;
  std::vector<bool> seen(kTotal, false);
  std::size_t completed = 0;
  std::unique_ptr<netlib::ThreadPool> pool =
      netlib::ThreadPool::Create(3, kTotal, [&](netlib::FrameMessage message) {
        std::unique_lock<std::mutex> lock(mutex);
        entered = true;
        condition.notify_all();
        condition.wait(lock, [&] { return release; });
        if (message.sequence >= seen.size() || seen[message.sequence]) {
          exactly_once = false;
        } else {
          seen[message.sequence] = true;
        }
        ++completed;
      });
  if (!pool || pool->worker_count() != 3 ||
      !pool->Submit(netlib::FrameMessage{.sequence = 0})) {
    return false;
  }
  bool success = true;
  {
    std::unique_lock<std::mutex> lock(mutex);
    success = condition.wait_for(lock, std::chrono::seconds(3),
                                 [&] { return entered; });
  }
  std::atomic<bool> accepted{true};
  std::vector<std::thread> producers;
  for (std::size_t p = 0; p < kExplicitProducers; ++p) {
    netlib::ThreadPool::Submitter submitter = pool->CreateSubmitter();
    success = submitter.valid() && success;
    producers.emplace_back([submitter = std::move(submitter), p,
                            &accepted]() mutable {
      for (std::size_t i = 0; i < kPerProducer; ++i) {
        if (!submitter.Submit(
                netlib::FrameMessage{.sequence = 1 + p * kPerProducer + i})) {
          accepted.store(false);
        }
      }
    });
  }
  producers.emplace_back([&] {
    for (std::size_t i = 0; i < kPerProducer; ++i) {
      if (!pool->Submit(netlib::FrameMessage{
              .sequence = 1 + kExplicitProducers * kPerProducer + i})) {
        accepted.store(false);
      }
    }
  });
  for (std::thread& producer : producers) {
    producer.join();
  }
  // Producer tokens have been destroyed with their source threads, but their
  // queued messages must still drain before implicit stop controls are posted.
  std::future<void> stop1 =
      std::async(std::launch::async, [&] { pool->Stop(); });
  std::future<void> stop2 =
      std::async(std::launch::async, [&] { pool->Stop(); });
  success = stop1.wait_for(std::chrono::milliseconds(30)) ==
                std::future_status::timeout &&
            stop2.wait_for(std::chrono::milliseconds(30)) ==
                std::future_status::timeout &&
            success;
  {
    std::lock_guard<std::mutex> lock(mutex);
    release = true;
    condition.notify_all();
  }
  stop1.get();
  stop2.get();
  return success && accepted.load() && exactly_once && completed == kTotal &&
         pool->queued_count() == 0 && !pool->Submit(netlib::FrameMessage{});
}

bool TestSubmitStopRace() {
  std::atomic<std::size_t> accepted{0};
  std::atomic<std::size_t> completed{0};
  std::unique_ptr<netlib::ThreadPool> pool = netlib::ThreadPool::Create(
      3, 64, [&](netlib::FrameMessage) { completed.fetch_add(1); });
  if (!pool) {
    return false;
  }
  std::latch start(5);
  std::vector<std::thread> producers;
  for (std::size_t p = 0; p < 4; ++p) {
    netlib::ThreadPool::Submitter submitter = pool->CreateSubmitter();
    if (!submitter.valid()) {
      std::abort();
    }
    producers.emplace_back([&, submitter = std::move(submitter)]() mutable {
      start.arrive_and_wait();
      for (std::size_t i = 0; i < 4000; ++i) {
        if (submitter.Submit(netlib::FrameMessage{})) {
          accepted.fetch_add(1);
        }
      }
    });
  }
  start.arrive_and_wait();
  while (accepted.load() == 0) {
    std::this_thread::yield();
  }
  pool->Stop();
  for (std::thread& producer : producers) {
    producer.join();
  }
  return accepted.load() == completed.load() && pool->queued_count() == 0;
}

bool TestSubmitterLifetime() {
  netlib::ThreadPool::Submitter empty;
  if (empty.valid() || empty.Submit(netlib::FrameMessage{})) {
    return false;
  }
  std::atomic<std::size_t> completed{0};
  std::unique_ptr<netlib::ThreadPool> pool = netlib::ThreadPool::Create(
      1, 8, [&](netlib::FrameMessage) { completed.fetch_add(1); });
  if (!pool) {
    return false;
  }
  netlib::ThreadPool::Submitter original = pool->CreateSubmitter();
  netlib::ThreadPool::Submitter retained = std::move(original);
  // Moved-from validity is part of this test's contract.
  // NOLINTNEXTLINE(clang-analyzer-cplusplus.Move)
  if (original.valid() || !retained.valid() ||
      !retained.Submit(netlib::FrameMessage{})) {
    return false;
  }
  pool.reset();  // Stops and joins even though the submitter keeps state alive.
  return completed.load() == 1 && !retained.Submit(netlib::FrameMessage{});
}

bool TestDefaultLoggerFallback() {
  std::unique_ptr<CountingSink> sink = std::make_unique<CountingSink>();
  CountingSink* sink_pointer = sink.get();
  netlib::log::LoggerOptions options{
      .name = "netlib.thread_pool.default",
      .minimum_level = netlib::log::LogLevel::kDebug,
      .queue_capacity = 16,
      .color_mode = netlib::log::ColorMode::kNever,
  };
  std::unique_ptr<netlib::log::Logger> logger =
      netlib::log::Logger::Create(std::move(options), std::move(sink));
  if (!logger) {
    return false;
  }

  {
    netlib::log::DefaultLoggerScope default_scope(*logger);
    std::unique_ptr<netlib::ThreadPool> pool =
        netlib::ThreadPool::Create(1, 8, [](netlib::FrameMessage) {});
    if (!pool) {
      return false;
    }
    pool->Stop();
  }
  logger->Stop();
  return sink_pointer->record_count() >= 3;
}

}  // namespace

int main() {
  if (!TestMixedProducersAndConcurrentStop() || !TestSubmitStopRace() ||
      !TestSubmitterLifetime() || !TestDefaultLoggerFallback()) {
    std::fprintf(stderr, "thread pool producer/stop regression failed\n");
    return 1;
  }
  return 0;
}
