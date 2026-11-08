#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <future>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

#include "netlib/log/logging.h"
namespace {
using namespace std::chrono_literals;
using netlib::log::LogLevel;

struct State {
  std::mutex mutex;
  std::condition_variable cv;
  bool block = false;
  bool entered = false;
  bool block_flush = false;
  bool flush_entered = false;
  std::set<std::string> written;
  std::vector<std::uint64_t> sequences;
  std::set<std::string> flushed;
  int flushes = 0;
};
class Sink : public netlib::log::LogSink {
 public:
  explicit Sink(std::shared_ptr<State> state) : state_(std::move(state)) {}
  void Write(const netlib::log::LogRecord& record) override {
    std::unique_lock<std::mutex> lock(state_->mutex);
    state_->entered = true;
    state_->cv.notify_all();
    state_->cv.wait(lock, [&] { return !state_->block; });
    state_->written.insert(record.message);
    state_->sequences.push_back(record.sequence);
  }
  void Flush() override {
    std::unique_lock<std::mutex> lock(state_->mutex);
    state_->flush_entered = true;
    state_->cv.notify_all();
    state_->cv.wait(lock, [&] { return !state_->block_flush; });
    state_->flushed = state_->written;
    ++state_->flushes;
    state_->cv.notify_all();
  }

 private:
  std::shared_ptr<State> state_;
};
std::unique_ptr<netlib::log::Logger> MakeLogger(
    std::shared_ptr<State> state, std::chrono::milliseconds interval = 0ms,
    std::size_t capacity = 8192) {
  netlib::log::LoggerOptions options{.name = "barrier",
                                     .queue_capacity = capacity};
  options.flush_interval = interval;
  return netlib::log::Logger::Create(options, std::make_unique<Sink>(state));
}
bool WaitEntered(std::shared_ptr<State> state) {
  std::unique_lock<std::mutex> lock(state->mutex);
  return state->cv.wait_for(lock, 3s, [&] { return state->entered; });
}
void Release(std::shared_ptr<State> state) {
  std::lock_guard<std::mutex> lock(state->mutex);
  state->block = false;
  state->cv.notify_all();
}

bool TestBarrierAndAdmission() {
  std::shared_ptr<State> state = std::make_shared<State>();
  state->block = true;
  std::unique_ptr<netlib::log::Logger> logger = MakeLogger(state);
  logger->Log(LogLevel::kInfo, "gate");
  if (!WaitEntered(state)) {
    Release(state);
    return false;
  }
  std::vector<std::thread> producers;
  std::atomic<bool> accepted{true};
  for (int p = 0; p < 4; ++p)
    producers.emplace_back([&, p] {
      for (int i = 0; i < 300; ++i)
        if (!logger->Log(LogLevel::kInfo,
                         std::to_string(p) + ":" + std::to_string(i)))
          accepted = false;
    });
  for (std::thread& producer : producers) producer.join();
  std::vector<std::future<bool>> flushes;
  for (int i = 0; i < 4; ++i)
    flushes.push_back(
        std::async(std::launch::async, [&] { return logger->Flush(); }));
  bool waiting = flushes[0].wait_for(30ms) == std::future_status::timeout;
  std::future<bool> later = std::async(std::launch::async, [&] {
    return logger->Log(LogLevel::kInfo, "later");
  });
  bool nonblocking = later.wait_for(1s) == std::future_status::ready;
  Release(state);
  bool ok = accepted && waiting && nonblocking && later.get();
  for (std::future<bool>& flush : flushes) ok = flush.get() && ok;
  // Inspect BEFORE Stop: Stop must not mask a broken explicit barrier.
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    for (int p = 0; p < 4; ++p)
      for (int i = 0; i < 300; ++i)
        ok = state->flushed.contains(std::to_string(p) + ":" +
                                     std::to_string(i)) &&
             ok;
  }
  logger->Stop();
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    ok = state->sequences.size() == 1202 && ok;
    for (std::size_t i = 0; i < state->sequences.size(); ++i) {
      ok = state->sequences[i] == i && ok;
    }
  }
  return ok;
}

bool TestStopRace() {
  std::shared_ptr<State> state = std::make_shared<State>();
  state->block = true;
  std::unique_ptr<netlib::log::Logger> logger = MakeLogger(state);
  logger->Log(LogLevel::kInfo, "gate");
  if (!WaitEntered(state)) {
    Release(state);
    return false;
  }
  std::atomic<int> accepted{1};
  std::vector<std::thread> producers;
  for (int p = 0; p < 4; ++p)
    producers.emplace_back([&, p] {
      for (int i = 0; i < 1000; ++i)
        if (logger->Log(LogLevel::kInfo,
                        std::to_string(p) + ":" + std::to_string(i)))
          ++accepted;
    });
  std::future<void> stop1 =
      std::async(std::launch::async, [&] { logger->Stop(); });
  std::future<void> stop2 =
      std::async(std::launch::async, [&] { logger->Stop(); });
  bool waiting = stop1.wait_for(30ms) == std::future_status::timeout &&
                 stop2.wait_for(30ms) == std::future_status::timeout;
  Release(state);
  for (std::thread& producer : producers) producer.join();
  stop1.get();
  stop2.get();
  std::lock_guard<std::mutex> lock(state->mutex);
  return waiting &&
         state->flushed.size() == static_cast<std::size_t>(accepted.load()) &&
         logger->queued_count() == 0 &&
         !logger->Log(LogLevel::kInfo, "rejected") && !logger->Flush();
}

bool TestPeriodicAndDrops() {
  std::shared_ptr<State> state = std::make_shared<State>();
  std::unique_ptr<netlib::log::Logger> logger = MakeLogger(state, 20ms);
  logger->Log(LogLevel::kInfo, "automatic");
  bool automatic;
  {
    std::unique_lock<std::mutex> lock(state->mutex);
    automatic = state->cv.wait_for(
        lock, 3s, [&] { return state->flushed.contains("automatic"); });
  }
  logger->Stop();
  std::shared_ptr<State> blocked = std::make_shared<State>();
  blocked->block = true;
  std::unique_ptr<netlib::log::Logger> bounded = MakeLogger(blocked, 0ms, 1);
  bounded->Log(LogLevel::kInfo, "accepted");
  if (!WaitEntered(blocked)) {
    Release(blocked);
    return false;
  }
  bool dropped = !bounded->Log(LogLevel::kInfo, "dropped") &&
                 bounded->dropped_count() == 1;
  Release(blocked);
  bool flushed = bounded->Flush();  // Rejection must not leave a sequence hole.
  bounded->Stop();
  return automatic && dropped && flushed;
}

class ThrowingSink : public netlib::log::LogSink {
  void Write(const netlib::log::LogRecord&) override {
    throw std::runtime_error("write failed");
  }
  void Flush() override {}
};
bool TestFailure() {
  std::unique_ptr<netlib::log::Logger> logger = netlib::log::Logger::Create(
      {.name = "failure"}, std::make_unique<ThrowingSink>());
  logger->Log(LogLevel::kInfo, "fail");
  bool failed = !logger->Flush();
  logger->Stop();
  return failed;
}
bool TestSubmissionDuringFlush() {
  std::shared_ptr<State> state = std::make_shared<State>();
  state->block_flush = true;
  std::unique_ptr<netlib::log::Logger> logger = MakeLogger(state);
  logger->Log(LogLevel::kInfo, "before");
  std::future<bool> flush =
      std::async(std::launch::async, [&] { return logger->Flush(); });
  bool entered;
  {
    std::unique_lock<std::mutex> lock(state->mutex);
    entered =
        state->cv.wait_for(lock, 3s, [&] { return state->flush_entered; });
  }
  std::future<bool> submit = std::async(std::launch::async, [&] {
    return logger->Log(LogLevel::kInfo, "during");
  });
  bool nonblocking = submit.wait_for(1s) == std::future_status::ready;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->block_flush = false;
    state->cv.notify_all();
  }
  bool ok = submit.get() && flush.get();
  logger->Stop();
  return entered && nonblocking && ok;
}
bool TestFlushWhileProducing() {
  std::shared_ptr<State> state = std::make_shared<State>();
  std::unique_ptr<netlib::log::Logger> logger = MakeLogger(state);
  logger->Log(LogLevel::kInfo, "before-stream");
  std::atomic<bool> quit{false};
  std::thread producer([&] {
    while (!quit) {
      logger->Log(LogLevel::kInfo, "stream");
      std::this_thread::yield();
    }
  });
  std::future<bool> flush =
      std::async(std::launch::async, [&] { return logger->Flush(); });
  bool ready = flush.wait_for(3s) == std::future_status::ready;
  quit = true;
  producer.join();
  bool ok = flush.get();
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    ok = state->flushed.contains("before-stream") && ok;
  }
  logger->Stop();
  return ready && ok;
}
}  // namespace

int main() {
  if (!TestBarrierAndAdmission() || !TestStopRace() ||
      !TestPeriodicAndDrops() || !TestSubmissionDuringFlush() ||
      !TestFlushWhileProducing() || !TestFailure()) {
    std::fprintf(stderr, "log barrier regression failed\n");
    return 1;
  }
}
