#include "netlib/timer.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace {
using namespace std::chrono_literals;

bool TestOneShotAndPeriodicTimers() {
  std::atomic<int> cancelled_calls{0};
  netlib::TimerScheduler::TimerId cancelled =
      netlib::TimerScheduler::ScheduleAfter(1h, [&] { ++cancelled_calls; });
  if (cancelled == 0 || !netlib::TimerScheduler::Cancel(cancelled) ||
      netlib::TimerScheduler::Cancel(cancelled)) {
    return false;
  }

  std::promise<netlib::TimerScheduler::Clock::time_point> fired;
  std::future<netlib::TimerScheduler::Clock::time_point> ready =
      fired.get_future();
  netlib::TimerScheduler::Clock::time_point deadline =
      netlib::TimerScheduler::Clock::now() + 20ms;
  netlib::TimerScheduler::ScheduleAt(
      deadline, [&] { fired.set_value(netlib::TimerScheduler::Clock::now()); });
  if (ready.wait_for(3s) != std::future_status::ready ||
      ready.get() < deadline) {
    return false;
  }

  std::shared_ptr<std::atomic<int>> repeats =
      std::make_shared<std::atomic<int>>(0);
  std::promise<void> repeated;
  std::future<void> repeated_ready = repeated.get_future();
  netlib::TimerScheduler::TimerId periodic =
      netlib::TimerScheduler::ScheduleEvery(5ms, [&, repeats] {
        if (++*repeats == 3) {
          repeated.set_value();
        }
      });
  if (periodic == 0 ||
      repeated_ready.wait_for(3s) != std::future_status::ready ||
      !netlib::TimerScheduler::Cancel(periodic) ||
      netlib::TimerScheduler::ScheduleEvery(0ms, [] {}) != 0) {
    return false;
  }
  return cancelled_calls == 0;
}

bool TestReentry() {
  std::promise<void> done;
  std::future<void> ready = done.get_future();
  netlib::TimerScheduler::ScheduleAfter(0ms, [&] {
    netlib::TimerScheduler::ScheduleAfter(0ms, [&] { done.set_value(); });
  });
  return ready.wait_for(3s) == std::future_status::ready;
}

bool TestHeapOrderAndCancellation() {
  std::promise<void> entered;
  std::future<void> entered_ready = entered.get_future();
  std::promise<void> release;
  std::shared_future<void> gate = release.get_future().share();
  netlib::TimerScheduler::ScheduleAfter(0ms, [&] {
    entered.set_value();
    gate.wait();
  });
  if (entered_ready.wait_for(3s) != std::future_status::ready) {
    release.set_value();
    return false;
  }

  constexpr int kCount = 1024;
  constexpr int kExpectedCount = kCount * 3 / 4;
  netlib::TimerScheduler::Clock::time_point base =
      netlib::TimerScheduler::Clock::now() - 1s;
  std::shared_ptr<std::vector<int>> actual =
      std::make_shared<std::vector<int>>();
  std::shared_ptr<std::mutex> actual_mutex = std::make_shared<std::mutex>();
  std::promise<void> completed;
  std::future<void> done = completed.get_future();
  std::vector<std::pair<int, int>> expected;
  std::vector<netlib::TimerScheduler::TimerId> ids;
  for (int i = 0; i < kCount; ++i) {
    int offset = (i * 37) % 31;
    ids.push_back(netlib::TimerScheduler::ScheduleAt(
        base + std::chrono::milliseconds(offset),
        [actual, actual_mutex, &completed, i] {
          std::lock_guard<std::mutex> lock(*actual_mutex);
          actual->push_back(i);
          if (actual->size() == kExpectedCount) {
            completed.set_value();
          }
        }));
    if (i % 4 != 0) {
      expected.emplace_back(offset, i);
    }
  }
  bool ok = true;
  for (int i = kCount - 4; i >= 0; i -= 4) {
    ok = netlib::TimerScheduler::Cancel(ids[i]) && ok;
  }
  std::sort(expected.begin(), expected.end());
  release.set_value();
  ok = done.wait_for(3s) == std::future_status::ready && ok;
  std::lock_guard<std::mutex> lock(*actual_mutex);
  if (!ok || actual->size() != expected.size()) {
    return false;
  }
  for (std::size_t i = 0; i < actual->size(); ++i) {
    if ((*actual)[i] != expected[i].second) {
      return false;
    }
  }
  return true;
}

bool TestConcurrentScheduling() {
  std::atomic<bool> ok{true};
  std::vector<std::thread> producers;
  for (int p = 0; p < 4; ++p) {
    producers.emplace_back([&] {
      for (int i = 0; i < 1000; ++i) {
        netlib::TimerScheduler::TimerId id =
            netlib::TimerScheduler::ScheduleAfter(1h, [] {});
        if (id == 0 || !netlib::TimerScheduler::Cancel(id) ||
            netlib::TimerScheduler::Cancel(id)) {
          ok = false;
        }
      }
    });
  }
  for (std::thread& producer : producers) {
    producer.join();
  }
  return ok.load();
}
}  // namespace

int main() {
  if (!TestOneShotAndPeriodicTimers() || !TestReentry() ||
      !TestHeapOrderAndCancellation() || !TestConcurrentScheduling()) {
    std::fprintf(stderr, "timer regression failed\n");
    return 1;
  }
  return 0;
}
