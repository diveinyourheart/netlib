#ifndef NETLIB_INCLUDE_NETLIB_TIMER_H_
#define NETLIB_INCLUDE_NETLIB_TIMER_H_

#include <chrono>
#include <cstdint>
#include <functional>

namespace netlib {
// Process-wide, steady-clock scheduler with one detached callback thread.
// Callbacks must not throw and should be short: post work to the Reactor.
// Invalid arguments return 0/false. Resource exhaustion is fail-fast; the
// scheduler has no exception recovery. std::function construction by callers
// is outside this contract.
// The scheduler is never destroyed or stopped. Captures must remain valid for
// the process lifetime unless their timer is cancelled.
class TimerScheduler {
 public:
  using Clock = std::chrono::steady_clock;
  using TimerId = std::uint64_t;
  using Callback = std::function<void()>;
  static constexpr TimerId kInvalidTimer = 0;

  TimerScheduler() = delete;
  ~TimerScheduler() = delete;
  TimerScheduler(const TimerScheduler&) = delete;
  TimerScheduler& operator=(const TimerScheduler&) = delete;

  static TimerId ScheduleAt(Clock::time_point deadline,
                            Callback callback) noexcept;
  static TimerId ScheduleAfter(Clock::duration delay,
                               Callback callback) noexcept;
  // Fixed delay after callback completion, no catch-up bursts. Interval > 0.
  static TimerId ScheduleEvery(Clock::duration interval,
                               Callback callback) noexcept;
  // Prevents future executions; an already selected callback may finish.
  static bool Cancel(TimerId id) noexcept;

 private:
  class Impl;
  static Impl& GetImpl() noexcept;
};
}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_TIMER_H_
