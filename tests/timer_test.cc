#include "netlib/timer.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <future>
#include <thread>
#include <vector>
namespace {
using namespace std::chrono_literals;

bool TestTimers() {
  netlib::TimerScheduler timer;
  std::atomic<int> cancelled_calls{0};
  netlib::TimerScheduler::TimerId cancelled =
      timer.ScheduleAfter(1h, [&] { ++cancelled_calls; });
  if (!timer.Cancel(cancelled) || timer.Cancel(cancelled)) return false;
  // An earlier insertion must wake the worker sleeping on a distant deadline.
  timer.ScheduleAfter(1h, [] {});
  std::promise<netlib::TimerScheduler::Clock::time_point> fired;
  std::future<netlib::TimerScheduler::Clock::time_point> ready =
      fired.get_future();
  netlib::TimerScheduler::Clock::time_point deadline =
      netlib::TimerScheduler::Clock::now() + 20ms;
  timer.ScheduleAt(
      deadline, [&] { fired.set_value(netlib::TimerScheduler::Clock::now()); });
  if (ready.wait_for(3s) != std::future_status::ready || ready.get() < deadline)
    return false;
  std::promise<void> repeated;
  std::future<void> repeated_ready = repeated.get_future();
  std::atomic<int> repeats{0};
  netlib::TimerScheduler::TimerId periodic = timer.ScheduleEvery(5ms, [&] {
    if (++repeats == 3) repeated.set_value();
  });
  if (repeated_ready.wait_for(3s) != std::future_status::ready) return false;
  if (!timer.Cancel(periodic)) return false;
  timer.Stop();
  return cancelled_calls == 0 && timer.ScheduleAfter(0ms, [] {}) == 0;
}

bool TestRunningCancellation() {
  netlib::TimerScheduler timer;
  std::promise<void> entered, release;
  std::future<void> entered_ready = entered.get_future();
  std::shared_future<void> gate = release.get_future().share();
  std::atomic<int> calls{0};
  netlib::TimerScheduler::TimerId id = timer.ScheduleEvery(1ms, [&] {
    ++calls;
    entered.set_value();
    gate.wait();
  });
  if (entered_ready.wait_for(3s) != std::future_status::ready) {
    release.set_value();
    return false;
  }
  bool cancelled = timer.Cancel(id);
  std::future<void> stop1 =
      std::async(std::launch::async, [&] { timer.Stop(); });
  std::future<void> stop2 =
      std::async(std::launch::async, [&] { timer.Stop(); });
  bool waiting = stop1.wait_for(20ms) == std::future_status::timeout &&
                 stop2.wait_for(20ms) == std::future_status::timeout;
  release.set_value();
  stop1.get();
  stop2.get();
  return cancelled && waiting && calls == 1;
}

bool TestReentry() {
  netlib::TimerScheduler timer;
  std::promise<void> done;
  std::future<void> ready = done.get_future();
  timer.ScheduleAfter(0ms, [&] {
    timer.ScheduleAfter(0ms, [&] {
      timer.Stop();
      done.set_value();
    });
  });
  bool ok = ready.wait_for(3s) == std::future_status::ready;
  timer.Stop();
  return ok && timer.ScheduleEvery(0ms, [] {}) == 0;
}
bool TestHeapOrderAndCancellation() {
  netlib::TimerScheduler timer;
  std::promise<void> entered, release, completed;
  std::future<void> entered_ready = entered.get_future();
  std::shared_future<void> gate = release.get_future().share();
  std::future<void> done = completed.get_future();
  timer.ScheduleAfter(0ms, [&] {
    entered.set_value();
    gate.wait();
  });
  if (entered_ready.wait_for(3s) != std::future_status::ready) {
    release.set_value();
    return false;
  }
  constexpr int kCount = 1024;
  std::vector<netlib::TimerScheduler::TimerId> ids;
  std::vector<std::pair<int, int>> expected;
  std::vector<int> actual;
  netlib::TimerScheduler::Clock::time_point base =
      netlib::TimerScheduler::Clock::now() - 1s;
  for (int i = 0; i < kCount; ++i) {
    int offset = (i * 37) % 31;
    ids.push_back(
        timer.ScheduleAt(base + std::chrono::milliseconds(offset), [&, i] {
          actual.push_back(i);
          if (actual.size() == kCount * 3 / 4) completed.set_value();
        }));
    if (i % 4 != 0) expected.emplace_back(offset, i);
  }
  bool ok = true;
  // Remove internal and terminal heap nodes, not only the minimum.
  for (int i = kCount - 4; i >= 0; i -= 4) ok = timer.Cancel(ids[i]) && ok;
  std::sort(expected.begin(), expected.end());
  release.set_value();
  ok = done.wait_for(3s) == std::future_status::ready && ok;
  timer.Stop();
  if (!ok || actual.size() != expected.size()) return false;
  for (std::size_t i = 0; i < actual.size(); ++i)
    if (actual[i] != expected[i].second) return false;
  return true;
}

bool TestConcurrentScheduling() {
  netlib::TimerScheduler timer;
  std::atomic<bool> ok{true};
  std::vector<std::thread> producers;
  for (int p = 0; p < 4; ++p)
    producers.emplace_back([&] {
      for (int i = 0; i < 1000; ++i) {
        netlib::TimerScheduler::TimerId id = timer.ScheduleAfter(1h, [] {});
        if (id == 0 || !timer.Cancel(id) || timer.Cancel(id)) ok = false;
      }
    });
  for (std::thread& producer : producers) producer.join();
  timer.Stop();
  return ok;
}
}  // namespace

int main() {
  if (!TestTimers() || !TestRunningCancellation() || !TestReentry() ||
      !TestHeapOrderAndCancellation() || !TestConcurrentScheduling()) {
    std::fprintf(stderr, "timer regression failed\n");
    return 1;
  }
}
