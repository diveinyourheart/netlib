#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "netlib/frame_message.h"
#include "netlib/main_reactor.h"
#include "netlib/platform.h"
#include "netlib/protocol_session.h"
#include "netlib/sub_reactor.h"
#include "netlib/thread_pool.h"
#include "netlib/unique_fd.h"

namespace {

enum class TestMode {
  kNormal,
  kZeroProgress,
  kOverConsume,
  kBadNeedMore,
  kBadRequestId,
  kOutputOverflow,
  kProduceFailure,
  kCloseOnInput,
  kSmallInputLimit,
};

struct Observation {
  std::mutex mutex;
  std::condition_variable condition;
  std::vector<netlib::FrameMessage> requests;
  bool closed = false;
  std::atomic<bool> wrong_thread{false};
  std::atomic<bool> oversized_input{false};
};

// Deliberately not an HTTP/2 implementation. Exercises the transport contract
// independently: batched requests, stream routing, control output and shutdown.
class TestSession final : public netlib::ProtocolSession {
 public:
  TestSession(TestMode mode, std::size_t output_limit, Observation& observation)
      : mode_(mode), output_limit_(output_limit), observation_(observation) {}

  netlib::SessionInputResult OnInput(std::span<const char> input) override {
    CheckThread();
    if (input.empty()) {
      return {.requests = {}};
    }
    if (input.size() > MaxInputBufferSize()) {
      observation_.oversized_input.store(true);
    }
    netlib::SessionInputResult result{
        .status = netlib::SessionInputStatus::kProgress,
        .consumed_bytes = 1,
        .requests = {}};
    switch (mode_) {
      case TestMode::kZeroProgress:
        result.consumed_bytes = 0;
        return result;
      case TestMode::kOverConsume:
        result.consumed_bytes = input.size() + 1;
        return result;
      case TestMode::kBadNeedMore:
        return {.required_bytes = input.size(), .requests = {}};
      case TestMode::kBadRequestId:
        result.requests.push_back(
            {.context = {.request_id = 1, .stream_id = 11},
             .metadata = netlib::BinaryFrameMetadata{},
             .payload = {'x'}});
        return result;
      case TestMode::kOutputOverflow:
        output_.assign(output_limit_ + 1, 'x');
        return result;
      case TestMode::kProduceFailure:
        produce_failure_ = true;
        return result;
      case TestMode::kCloseOnInput:
        BeginDrain();
        return result;
      case TestMode::kSmallInputLimit:
        input_bytes_ += input.size();
        result.consumed_bytes = input.size();
        if (input_bytes_ == 70) {
          output_ += 'B';
          BeginDrain();
        }
        return result;
      case TestMode::kNormal:
        break;
    }
    if (input.front() == '!') {
      output_ += 'C';
    } else if (input.front() == 'R') {
      if (input.size() < 2) {
        return {.required_bytes = 2, .requests = {}};
      }
      result.requests.push_back({.context = {.request_id = 0, .stream_id = 11},
                                 .metadata = netlib::BinaryFrameMetadata{},
                                 .payload = {'a'}});
      result.requests.push_back({.context = {.request_id = 1, .stream_id = 7},
                                 .metadata = netlib::BinaryFrameMetadata{},
                                 .payload = {'b'}});
      // No socket interests while waiting for Workers. A response resumes read
      // registration; this must receive a fresh event token.
      paused_ = true;
    } else if (input.front() == 'T') {
      result.requests.push_back({.context = {.request_id = 2, .stream_id = 19},
                                 .metadata = netlib::BinaryFrameMetadata{},
                                 .payload = {'t'}});
    }
    return result;
  }

  netlib::SessionResponseStatus OnResponse(
      const netlib::RequestContext& context,
      netlib::ApplicationResponse response, netlib::SessionResponseMode,
      bool) override {
    CheckThread();
    paused_ = false;
    const netlib::BinaryResponse& binary =
        std::get<netlib::BinaryResponse>(response);
    output_ += std::to_string(context.stream_id.value_or(0));
    output_.append(binary.payload.begin(), binary.payload.end());
    return netlib::SessionResponseStatus::kQueued;
  }
  netlib::SessionResponseStatus OnTimeout(
      const netlib::RequestContext& context) override {
    CheckThread();
    output_ += 'T';
    output_ += std::to_string(context.stream_id.value_or(0));
    return netlib::SessionResponseStatus::kQueued;
  }
  void BeginDrain() override {
    CheckThread();
    closing_ = true;
  }
  bool ProduceOutput(std::size_t) override {
    CheckThread();
    // Overflow mode intentionally lies about its output; Reactor must bound it.
    return !produce_failure_;
  }
  std::span<const char> PendingOutput() const override { return output_; }
  void ConsumeOutput(std::size_t size) override {
    CheckThread();
    output_.erase(0, size);
  }
  bool WantsInput() const override { return !paused_ && !closing_; }
  bool ShouldClose() const override { return closing_; }
  std::size_t MaxInputBufferSize() const override { return 32; }

 private:
  void CheckThread() {
    if (owner_ == std::thread::id{}) {
      owner_ = std::this_thread::get_id();
    } else if (owner_ != std::this_thread::get_id()) {
      observation_.wrong_thread.store(true);
    }
  }

  const TestMode mode_;
  const std::size_t output_limit_;
  Observation& observation_;
  std::thread::id owner_{};
  std::string output_ = "G";
  bool paused_ = false;
  bool closing_ = false;
  bool produce_failure_ = false;
  std::size_t input_bytes_ = 0;
};

bool ReadExpected(netlib::SocketHandle socket, std::string_view expected) {
  std::string received(expected.size(), '\0');
  std::size_t offset = 0;
  while (offset != received.size()) {
    std::ptrdiff_t size = netlib::platform::Receive(
        socket, received.data() + offset, received.size() - offset);
    if (size <= 0) {
      if (size < 0 &&
          netlib::platform::Interrupted(netlib::platform::LastError())) {
        continue;
      }
      return false;
    }
    offset += static_cast<std::size_t>(size);
  }
  return received == expected;
}

bool SendByte(netlib::SocketHandle socket, char byte) {
  return netlib::platform::Send(socket, &byte, 1) == 1;
}

bool WaitForRequests(Observation& observation, std::size_t count) {
  std::unique_lock<std::mutex> lock(observation.mutex);
  return observation.condition.wait_for(lock, std::chrono::seconds(2), [&] {
    return observation.requests.size() == count;
  });
}

bool RunSessionTest(TestMode mode) {
  Observation observation;
  std::unique_ptr<netlib::ThreadPool> workers =
      netlib::ThreadPool::Create(1, 8, [&](netlib::FrameMessage request) {
        std::lock_guard<std::mutex> lock(observation.mutex);
        observation.requests.push_back(std::move(request));
        observation.condition.notify_all();
      });
  if (!workers) {
    return false;
  }
  netlib::SubReactorOptions options{
      .max_pending_connections = 8,
      .max_pending_responses = 8,
      .max_pending_output_bytes = 32,
      .request_timeout = std::chrono::milliseconds(750)};
  netlib::SubReactor::ConnectionCallbacks callbacks;
  callbacks.on_closed = [&](netlib::SocketHandle) {
    std::lock_guard<std::mutex> lock(observation.mutex);
    observation.closed = true;
    observation.condition.notify_all();
  };
  std::unique_ptr<netlib::SubReactor> sub =
      netlib::SubReactor::CreateWithSession(
          options, *workers, std::move(callbacks),
          [&](netlib::ProtocolSessionOptions limits) {
            return std::make_unique<TestSession>(
                mode, limits.max_pending_output_bytes, observation);
          });
  if (!sub) {
    return false;
  }
  std::unique_ptr<netlib::MainReactor> main =
      netlib::MainReactor::Create(0, {sub.get()});
  if (!main) {
    return false;
  }
  std::thread sub_thread([&] { sub->Run(); });
  std::thread main_thread([&] { main->Run(); });
  netlib::UniqueFd client(netlib::platform::CreateTcpSocket(false));
  bool success =
      client && netlib::platform::SetReceiveTimeout(client.get(), 2500) &&
      netlib::platform::ConnectLoopback(client.get(), main->port()) &&
      ReadExpected(client.get(), "G");
  if (mode == TestMode::kNormal) {
    success = success && SendByte(client.get(), '!') &&
              ReadExpected(client.get(), "C");
    {
      std::lock_guard<std::mutex> lock(observation.mutex);
      success = success && observation.requests.empty();
    }
    // R consumes only the first byte. The coalesced control byte must remain
    // in Reactor storage through pause and be parsed when a response resumes
    // it.
    success = success && netlib::platform::Send(client.get(), "R!", 2) == 2 &&
              WaitForRequests(observation, 2);
    if (success) {
      netlib::FrameMessage first;
      netlib::FrameMessage second;
      {
        std::lock_guard<std::mutex> lock(observation.mutex);
        first = observation.requests[0];
        second = observation.requests[1];
      }
      success = first.sequence == 0 && first.request_context.request_id == 0 &&
                first.request_context.stream_id == 11 &&
                first.payload == std::vector<char>{'a'} &&
                second.sequence == 1 && second.request_context.stream_id == 7;
      // Worker-visible copies are not the trusted routing table.
      second.request_context.stream_id = 999;
      success =
          success && second.Respond(std::vector<char>{'b'}) &&
          ReadExpected(client.get(), "7bC") &&
          second.Respond(std::vector<char>{'x'}) &&
          first.Respond(std::vector<char>{'a'}) &&
          ReadExpected(client.get(), "11a") && SendByte(client.get(), '!') &&
          ReadExpected(client.get(), "C") && SendByte(client.get(), 'T') &&
          WaitForRequests(observation, 3) && ReadExpected(client.get(), "T19");
      if (success) {
        netlib::FrameMessage expired;
        {
          std::lock_guard<std::mutex> lock(observation.mutex);
          expired = observation.requests[2];
        }
        success =
            expired.IsCancelled() && !expired.Respond(std::vector<char>{'l'}) &&
            SendByte(client.get(), '!') && ReadExpected(client.get(), "C");
      }
    }
  } else {
    if (mode == TestMode::kSmallInputLimit) {
      const std::string bytes(70, 'x');
      success =
          success &&
          netlib::platform::Send(client.get(), bytes.data(), bytes.size()) ==
              static_cast<std::ptrdiff_t>(bytes.size()) &&
          ReadExpected(client.get(), "B");
    } else {
      success = success && SendByte(client.get(), 'x');
    }
    std::unique_lock<std::mutex> lock(observation.mutex);
    success =
        success &&
        observation.condition.wait_for(lock, std::chrono::seconds(2),
                                       [&] { return observation.closed; }) &&
        observation.requests.empty();
  }
  client.Reset();
  main->Stop();
  sub->Stop();
  main_thread.join();
  sub_thread.join();
  workers->Stop();
  return success && !observation.wrong_thread.load() &&
         !observation.oversized_input.load();
}

}  // namespace

int main() {
  for (TestMode mode :
       {TestMode::kNormal, TestMode::kZeroProgress, TestMode::kOverConsume,
        TestMode::kBadNeedMore, TestMode::kBadRequestId,
        TestMode::kOutputOverflow, TestMode::kProduceFailure,
        TestMode::kCloseOnInput, TestMode::kSmallInputLimit}) {
    if (!RunSessionTest(mode)) {
      std::fprintf(stderr, "native Session transport test failed: mode=%d\n",
                   static_cast<int>(mode));
      return 1;
    }
  }
  return 0;
}
