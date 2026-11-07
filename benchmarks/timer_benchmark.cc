#include <algorithm>
#include <chrono>
#include <cstdio>
#include <random>
#include <vector>

#include "netlib/timer.h"

int main() {
  constexpr int kTimerCount = 100000;
  using Clock = netlib::TimerScheduler::Clock;
  using Milliseconds = std::chrono::duration<double, std::milli>;
  for (int trial = 0; trial < 3; ++trial) {
    netlib::TimerScheduler timer;
    std::vector<netlib::TimerScheduler::TimerId> ids;
    ids.reserve(kTimerCount);
    Clock::time_point start = Clock::now();
    for (int i = 0; i < kTimerCount; ++i) {
      ids.push_back(timer.ScheduleAfter(
          std::chrono::hours(24) + std::chrono::milliseconds(i), [] {}));
    }
    Clock::time_point scheduled = Clock::now();
    std::mt19937 rng(42);
    std::shuffle(ids.begin(), ids.end(), rng);
    Clock::time_point cancel_start = Clock::now();
    for (netlib::TimerScheduler::TimerId id : ids) {
      if (!timer.Cancel(id)) return 1;
    }
    Clock::time_point finish = Clock::now();
    timer.Stop();
    std::printf("schedule_ms=%.2f cancel_ms=%.2f\n",
                Milliseconds(scheduled - start).count(),
                Milliseconds(finish - cancel_start).count());
  }
}
