#ifndef NETLIB_INCLUDE_NETLIB_TIMER_H_
#define NETLIB_INCLUDE_NETLIB_TIMER_H_

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>

namespace netlib {
// Thread-safe, steady-clock scheduler with one serial callback thread.
// Callbacks must not throw and should be short: post work to the Reactor.
// Invalid arguments return 0/false. Resource exhaustion is fail-fast; the
// scheduler has no exception recovery. std::function construction by callers
// is outside this contract.
// Captures must outlive callbacks. Destruction inside a callback is
// unsupported.
class TimerScheduler {
 public:
  using Clock = std::chrono::steady_clock;
  using TimerId = std::uint64_t;
  using Callback = std::function<void()>;
  static constexpr TimerId kInvalidTimer = 0;
  TimerScheduler() noexcept;
  ~TimerScheduler();
  TimerScheduler(const TimerScheduler&) = delete;
  TimerScheduler& operator=(const TimerScheduler&) = delete;
  TimerId ScheduleAt(Clock::time_point deadline, Callback callback) noexcept;
  TimerId ScheduleAfter(Clock::duration delay, Callback callback) noexcept;
  // Fixed delay after callback completion, no catch-up bursts. Interval > 0.
  TimerId ScheduleEvery(Clock::duration interval, Callback callback) noexcept;
  // Prevents future executions; an already selected callback may finish.
  bool Cancel(TimerId id) noexcept;
  // Cancels pending timers and joins. Called inside a callback: requests stop
  // only; a subsequent external Stop/destructor must join.
  void Stop() noexcept;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};
}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_TIMER_H_
