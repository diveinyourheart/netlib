#include "netlib/timer.h"

#include <condition_variable>
#include <limits>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace netlib {
class TimerScheduler::Impl {
 public:
  using PositionMap = std::unordered_map<TimerId, std::size_t>;
  struct Entry {
    TimerId id;
    Clock::time_point deadline;
    Clock::duration interval;
    Callback callback;
  };
  Impl() noexcept {
    heap_.reserve(64);
    positions_.reserve(64);
    worker_ = std::thread([this] { Run(); });
    worker_id_ = worker_.get_id();
  }
  ~Impl() { Stop(); }

  TimerId Schedule(Clock::time_point deadline, Clock::duration interval,
                   Callback callback) noexcept {
    if (!callback) return kInvalidTimer;
    // Construct captures before locking so rejected callbacks are destroyed
    // after unlocking (their destructors may reenter the scheduler).
    Entry entry{0, deadline, interval, std::move(callback)};
    bool wake = false;
    TimerId id = kInvalidTimer;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (stopping_ || next_id_ == 0) return kInvalidTimer;
      id = entry.id = next_id_++;
      wake = heap_.empty() || deadline < heap_.front().deadline;
      positions_.emplace(id, heap_.size());
      PushEntry(std::move(entry));
    }
    if (wake) changed_.notify_one();
    return id;
  }

  bool Cancel(TimerId id) noexcept {
    Entry removed{};
    bool wake = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      PositionMap::iterator found = positions_.find(id);
      if (found == positions_.end()) return false;
      std::size_t index = found->second;
      if (index != kRunning) {
        wake = index == 0;
        removed = RemoveEntry(index);
      }
      // A running callback owns its Entry locally. Removing its index prevents
      // rearming, without interrupting or destroying that callback.
      positions_.erase(id);
    }
    if (wake) changed_.notify_one();
    return true;
  }

  void Stop() noexcept {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopping_ = true;
    }
    changed_.notify_one();
    if (std::this_thread::get_id() == worker_id_) return;
    std::lock_guard<std::mutex> join_lock(join_mutex_);
    if (worker_.joinable()) worker_.join();
  }

 private:
  static constexpr std::size_t kRunning =
      std::numeric_limits<std::size_t>::max();
  static bool Earlier(const Entry& a, const Entry& b) noexcept {
    return a.deadline < b.deadline || (a.deadline == b.deadline && a.id < b.id);
  }
  void SwapEntries(std::size_t a, std::size_t b) noexcept {
    std::swap(heap_[a], heap_[b]);
    positions_.find(heap_[a].id)->second = a;
    positions_.find(heap_[b].id)->second = b;
  }
  void SiftUp(std::size_t index) noexcept {
    while (index != 0) {
      std::size_t parent = (index - 1) / 2;
      if (!Earlier(heap_[index], heap_[parent])) break;
      SwapEntries(index, parent);
      index = parent;
    }
  }
  void SiftDown(std::size_t index) noexcept {
    while (index < heap_.size() / 2) {
      std::size_t child = index * 2 + 1;
      if (child + 1 < heap_.size() && Earlier(heap_[child + 1], heap_[child]))
        ++child;
      if (!Earlier(heap_[child], heap_[index])) break;
      SwapEntries(index, child);
      index = child;
    }
  }
  void PushEntry(Entry entry) noexcept {
    std::size_t index = heap_.size();
    heap_.push_back(std::move(entry));
    positions_.find(heap_.back().id)->second = index;
    SiftUp(index);
  }
  Entry RemoveEntry(std::size_t index) noexcept {
    Entry removed = std::move(heap_[index]);
    if (index != heap_.size() - 1) {
      heap_[index] = std::move(heap_.back());
      positions_.find(heap_[index].id)->second = index;
    }
    heap_.pop_back();
    if (index < heap_.size()) {
      if (index > 0 && Earlier(heap_[index], heap_[(index - 1) / 2]))
        SiftUp(index);
      else
        SiftDown(index);
    }
    return removed;
  }

  void Run() noexcept {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!stopping_) {
      if (heap_.empty()) {
        changed_.wait(lock);
        continue;
      }
      Clock::time_point deadline = heap_.front().deadline;
      if (deadline > Clock::now()) {
        changed_.wait_until(lock, deadline);
        continue;
      }
      Entry entry = RemoveEntry(0);
      positions_.find(entry.id)->second = kRunning;
      lock.unlock();
      entry.callback();  // Contract: callbacks do not throw.
      lock.lock();
      PositionMap::iterator found = positions_.find(entry.id);
      if (!stopping_ && found != positions_.end() &&
          entry.interval > Clock::duration::zero()) {
        entry.deadline = DeadlineAfter(entry.interval);
        PushEntry(std::move(entry));  // Reuses the existing ID-index node.
      } else if (found != positions_.end()) {
        positions_.erase(found);
      }
      lock.unlock();
      entry.callback = {};  // Release captures without holding mutex_.
      lock.lock();
    }
    std::vector<Entry> pending = std::move(heap_);
    positions_.clear();
    lock.unlock();  // Destroy pending callback captures outside the lock.
  }

 public:
  static Clock::time_point DeadlineAfter(Clock::duration delay) noexcept {
    Clock::time_point now = Clock::now();
    if (delay <= Clock::duration::zero()) return now;
    if (delay > Clock::time_point::max() - now) return Clock::time_point::max();
    return now + delay;
  }

 private:
  std::mutex mutex_;
  std::condition_variable changed_;
  bool stopping_ = false;
  TimerId next_id_ = 1;
  std::vector<Entry> heap_;
  PositionMap positions_;
  std::mutex join_mutex_;
  std::thread worker_;
  std::thread::id worker_id_;
};

TimerScheduler::TimerScheduler() noexcept : impl_(std::make_unique<Impl>()) {}
TimerScheduler::~TimerScheduler() = default;
TimerScheduler::TimerId TimerScheduler::ScheduleAt(Clock::time_point deadline,
                                                   Callback callback) noexcept {
  return impl_->Schedule(deadline, Clock::duration::zero(),
                         std::move(callback));
}
TimerScheduler::TimerId TimerScheduler::ScheduleAfter(
    Clock::duration delay, Callback callback) noexcept {
  return ScheduleAt(Impl::DeadlineAfter(delay), std::move(callback));
}
TimerScheduler::TimerId TimerScheduler::ScheduleEvery(
    Clock::duration interval, Callback callback) noexcept {
  if (interval <= Clock::duration::zero()) return kInvalidTimer;
  return impl_->Schedule(Impl::DeadlineAfter(interval), interval,
                         std::move(callback));
}
bool TimerScheduler::Cancel(TimerId id) noexcept { return impl_->Cancel(id); }
void TimerScheduler::Stop() noexcept { impl_->Stop(); }
}  // namespace netlib
