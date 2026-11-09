#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <future>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "netlib/log/logging.h"

namespace {
using namespace std::chrono_literals;
using netlib::log::LogLevel;

struct State {
  std::mutex mutex;
  std::condition_variable condition;
  bool block = false;
  bool entered = false;
  bool block_flush = false;
  bool flush_entered = false;
  std::set<std::string> written;
  std::vector<std::uint64_t> sequences;
  std::set<std::string> flushed;
};

class Sink final : public netlib::log::LogSink {
 public:
  explicit Sink(std::shared_ptr<State> state) : state_(std::move(state)) {}

  void Write(const netlib::log::LogRecord& record) override {
    std::unique_lock<std::mutex> lock(state_->mutex);
    state_->entered = true;
    state_->condition.notify_all();
    state_->condition.wait(lock, [&] { return !state_->block; });
    state_->written.insert(record.message);
    state_->sequences.push_back(record.sequence);
  }

  void Flush() override {
    std::unique_lock<std::mutex> lock(state_->mutex);
    state_->flush_entered = true;
    state_->condition.notify_all();
    state_->condition.wait(lock, [&] { return !state_->block_flush; });
    state_->flushed = state_->written;
    state_->condition.notify_all();
  }

 private:
  std::shared_ptr<State> state_;
};

bool InitializeLogger(
    std::shared_ptr<State> state,
    std::chrono::milliseconds interval = std::chrono::milliseconds::zero(),
    std::size_t capacity = 8192) {
  netlib::log::LoggerOptions options{
      .name = "barrier",
      .queue_capacity = capacity,
      .flush_interval = interval,
  };
  return netlib::log::Logger::Initialize(
      std::move(options), std::make_unique<Sink>(std::move(state)));
}

bool WaitEntered(const std::shared_ptr<State>& state) {
  std::unique_lock<std::mutex> lock(state->mutex);
  return state->condition.wait_for(lock, 3s, [&] { return state->entered; });
}

void Release(const std::shared_ptr<State>& state) {
  std::lock_guard<std::mutex> lock(state->mutex);
  state->block = false;
  state->condition.notify_all();
}

int TestBarrierAndAdmission() {
  std::shared_ptr<State> state = std::make_shared<State>();
  state->block = true;
  if (!InitializeLogger(state)) {
    return 1;
  }
  if (!netlib::log::Logger::Log(LogLevel::kInfo, "gate") ||
      !WaitEntered(state)) {
    Release(state);
    return 1;
  }

  std::vector<std::thread> producers;
  std::atomic<bool> accepted{true};
  for (int p = 0; p < 4; ++p) {
    producers.emplace_back([&, p] {
      for (int i = 0; i < 300; ++i) {
        if (!netlib::log::Logger::Log(
                LogLevel::kInfo, std::to_string(p) + ":" + std::to_string(i))) {
          accepted = false;
        }
      }
    });
  }
  for (std::thread& producer : producers) {
    producer.join();
  }

  std::vector<std::future<bool>> flushes;
  for (int i = 0; i < 4; ++i) {
    flushes.push_back(std::async(std::launch::async,
                                 [] { return netlib::log::Logger::Flush(); }));
  }
  bool waiting = flushes[0].wait_for(30ms) == std::future_status::timeout;
  std::future<bool> later = std::async(std::launch::async, [] {
    return netlib::log::Logger::Log(LogLevel::kInfo, "later");
  });
  bool nonblocking = later.wait_for(1s) == std::future_status::ready;
  Release(state);
  bool ok = accepted && waiting && nonblocking && later.get();
  for (std::future<bool>& flush : flushes) {
    ok = flush.get() && ok;
  }
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    for (int p = 0; p < 4; ++p) {
      for (int i = 0; i < 300; ++i) {
        ok = state->flushed.contains(std::to_string(p) + ":" +
                                     std::to_string(i)) &&
             ok;
      }
    }
    ok = state->sequences.size() == 1202 && ok;
    for (std::size_t i = 0; i < state->sequences.size(); ++i) {
      ok = state->sequences[i] == i && ok;
    }
  }
  return ok ? 0 : 1;
}

int TestPeriodicFlush() {
  std::shared_ptr<State> state = std::make_shared<State>();
  if (!InitializeLogger(state, 20ms)) {
    return 1;
  }
  if (!netlib::log::Logger::Log(LogLevel::kInfo, "automatic")) {
    return 1;
  }
  std::unique_lock<std::mutex> lock(state->mutex);
  return state->condition.wait_for(
             lock, 3s, [&] { return state->flushed.contains("automatic"); })
             ? 0
             : 1;
}

int TestDrops() {
  std::shared_ptr<State> state = std::make_shared<State>();
  state->block = true;
  if (!InitializeLogger(state, std::chrono::milliseconds::zero(), 1)) {
    return 1;
  }
  if (!netlib::log::Logger::Log(LogLevel::kInfo, "accepted") ||
      !WaitEntered(state)) {
    Release(state);
    return 1;
  }
  bool dropped = !netlib::log::Logger::Log(LogLevel::kInfo, "dropped") &&
                 netlib::log::Logger::dropped_count() == 1;
  Release(state);
  return dropped && netlib::log::Logger::Flush() ? 0 : 1;
}

class ThrowingSink final : public netlib::log::LogSink {
 public:
  void Write(const netlib::log::LogRecord&) override {
    throw std::runtime_error("write failed");
  }

  void Flush() override {}
};

int TestFailure() {
  netlib::log::LoggerOptions options{
      .name = "failure",
      .flush_interval = std::chrono::milliseconds::zero(),
  };
  if (!netlib::log::Logger::Initialize(std::move(options),
                                       std::make_unique<ThrowingSink>())) {
    return 1;
  }
  netlib::log::Logger::Log(LogLevel::kInfo, "fail");
  return netlib::log::Logger::Flush() ? 1 : 0;
}

int TestSubmissionDuringFlush() {
  std::shared_ptr<State> state = std::make_shared<State>();
  state->block_flush = true;
  if (!InitializeLogger(state)) {
    return 1;
  }
  netlib::log::Logger::Log(LogLevel::kInfo, "before");
  std::future<bool> flush = std::async(
      std::launch::async, [] { return netlib::log::Logger::Flush(); });
  bool entered = false;
  {
    std::unique_lock<std::mutex> lock(state->mutex);
    entered = state->condition.wait_for(lock, 3s,
                                        [&] { return state->flush_entered; });
  }
  if (!entered) {
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->block_flush = false;
      state->condition.notify_all();
    }
    flush.get();
    return 1;
  }
  std::future<bool> submit = std::async(std::launch::async, [] {
    return netlib::log::Logger::Log(LogLevel::kInfo, "during");
  });
  if (submit.wait_for(1s) != std::future_status::ready) {
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->block_flush = false;
      state->condition.notify_all();
    }
    submit.get();
    flush.get();
    return 1;
  }
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->block_flush = false;
    state->condition.notify_all();
  }
  return submit.get() && flush.get() ? 0 : 1;
}

int TestFlushWhileProducing() {
  std::shared_ptr<State> state = std::make_shared<State>();
  if (!InitializeLogger(state)) {
    return 1;
  }
  netlib::log::Logger::Log(LogLevel::kInfo, "before-stream");
  std::atomic<bool> quit{false};
  std::thread producer([&] {
    while (!quit) {
      netlib::log::Logger::Log(LogLevel::kInfo, "stream");
      std::this_thread::yield();
    }
  });
  std::future<bool> flush = std::async(
      std::launch::async, [] { return netlib::log::Logger::Flush(); });
  bool ready = flush.wait_for(3s) == std::future_status::ready;
  quit = true;
  producer.join();
  bool ok = flush.get();
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    ok = state->flushed.contains("before-stream") && ok;
  }
  return ready && ok ? 0 : 1;
}
}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    return 2;
  }
  std::string_view mode = argv[1];
  if (mode == "barrier") {
    return TestBarrierAndAdmission();
  }
  if (mode == "periodic") {
    return TestPeriodicFlush();
  }
  if (mode == "drops") {
    return TestDrops();
  }
  if (mode == "failure") {
    return TestFailure();
  }
  if (mode == "submission-during-flush") {
    return TestSubmissionDuringFlush();
  }
  if (mode == "flush-while-producing") {
    return TestFlushWhileProducing();
  }
  return 2;
}
