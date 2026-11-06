#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "netlib/frame_protocol.h"
#include "netlib/log/logging.h"
#include "netlib/main_reactor.h"
#include "netlib/sub_reactor.h"
#include "netlib/thread_pool.h"
#include "netlib/unique_fd.h"

namespace {

constexpr auto kWaitTimeout = std::chrono::seconds(2);

struct Observation {
  std::mutex mutex;
  std::condition_variable condition;
  std::string data;
  std::size_t frame_count = 0;
  bool connected = false;
  bool closed = false;
};

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

void ReportFailure(const char* message) {
  std::fprintf(stderr, "%s\n", message);
}

bool ConnectTo(uint16_t port, netlib::UniqueFd* connection) {
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

  connection->Reset(socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
  if (!*connection) {
    return false;
  }
  return connect(connection->get(), reinterpret_cast<const sockaddr*>(&address),
                 sizeof(address)) == 0;
}

bool SendAll(int fd, std::string_view data) {
  std::size_t sent = 0;
  while (sent < data.size()) {
    ssize_t result = send(fd, data.data() + sent, data.size() - sent, 0);
    if (result > 0) {
      sent += static_cast<std::size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    return false;
  }
  return true;
}

std::string MakeLengthPrefix(std::uint32_t payload_size) {
  std::string prefix;
  prefix.reserve(netlib::kFrameLengthPrefixSize);
  prefix.push_back(static_cast<char>((payload_size >> 24U) & 0xFFU));
  prefix.push_back(static_cast<char>((payload_size >> 16U) & 0xFFU));
  prefix.push_back(static_cast<char>((payload_size >> 8U) & 0xFFU));
  prefix.push_back(static_cast<char>(payload_size & 0xFFU));
  return prefix;
}

std::string MakeFrame(std::string_view payload) {
  std::string frame =
      MakeLengthPrefix(static_cast<std::uint32_t>(payload.size()));
  frame.append(payload);
  return frame;
}

netlib::SubReactor::ConnectionCallbacks MakeConnectionCallbacks(
    Observation& observation) {
  netlib::SubReactor::ConnectionCallbacks callbacks;
  callbacks.on_connected = [&observation](int) {
    std::lock_guard<std::mutex> lock(observation.mutex);
    observation.connected = true;
    observation.condition.notify_all();
  };
  callbacks.on_closed = [&observation](int) {
    std::lock_guard<std::mutex> lock(observation.mutex);
    observation.closed = true;
    observation.condition.notify_all();
  };
  return callbacks;
}

std::unique_ptr<netlib::ThreadPool> CreateDroppingThreadPool(
    std::size_t max_pending_messages = 64) {
  return netlib::ThreadPool::Create(1, max_pending_messages,
                                    [](netlib::FrameMessage) {});
}

bool WaitForConnected(Observation& observation) {
  std::unique_lock<std::mutex> lock(observation.mutex);
  return observation.condition.wait_for(
      lock, kWaitTimeout, [&observation] { return observation.connected; });
}

bool WaitForData(Observation& observation, std::string_view expected,
                 std::size_t expected_frame_count) {
  std::unique_lock<std::mutex> lock(observation.mutex);
  return observation.condition.wait_for(
      lock, kWaitTimeout, [&observation, expected, expected_frame_count] {
        return observation.data.size() == expected.size() &&
               observation.data == expected &&
               observation.frame_count == expected_frame_count;
      });
}

bool WaitForClosed(Observation& observation) {
  std::unique_lock<std::mutex> lock(observation.mutex);
  return observation.condition.wait_for(
      lock, kWaitTimeout, [&observation] { return observation.closed; });
}

int RunPendingCapacityTest() {
  std::unique_ptr<netlib::ThreadPool> worker_pool = CreateDroppingThreadPool();
  if (!worker_pool) {
    ReportFailure("failed to create ThreadPool");
    return 1;
  }
  std::unique_ptr<netlib::SubReactor> sub_reactor =
      netlib::SubReactor::Create(1, *worker_pool, {});
  if (!sub_reactor) {
    ReportFailure("failed to create bounded SubReactor");
    return 1;
  }

  netlib::UniqueFd first(dup(STDIN_FILENO));
  netlib::UniqueFd second(dup(STDIN_FILENO));
  if (!first || !second || !sub_reactor->AdoptConnection(first) || first ||
      sub_reactor->AdoptConnection(second) || !second) {
    ReportFailure("pending queue capacity was not enforced");
    return 1;
  }
  return 0;
}

int RunThreadPoolDrainTest() {
  constexpr std::size_t kMessageCount = 1000;
  std::atomic<std::size_t> processed{0};
  std::unique_ptr<netlib::ThreadPool> worker_pool = netlib::ThreadPool::Create(
      2, kMessageCount + 1, [&processed](netlib::FrameMessage) {
        processed.fetch_add(1, std::memory_order_relaxed);
      });
  if (!worker_pool) {
    ReportFailure("failed to create drain-test ThreadPool");
    return 1;
  }

  for (std::size_t i = 0; i < kMessageCount; ++i) {
    netlib::FrameMessage message;
    message.sequence = i;
    if (!worker_pool->Submit(std::move(message))) {
      ReportFailure("drain-test ThreadPool queue overflowed");
      return 1;
    }
  }
  worker_pool->Stop();

  if (processed.load(std::memory_order_relaxed) != kMessageCount ||
      worker_pool->queued_count() != 0) {
    ReportFailure("ThreadPool did not drain all messages before stopping");
    return 1;
  }
  if (worker_pool->Submit(netlib::FrameMessage{})) {
    ReportFailure("ThreadPool accepted work after stopping");
    return 1;
  }
  return 0;
}

int RunFullSubReactorFallbackTest() {
  Observation first_observation;
  Observation second_observation;
  std::unique_ptr<netlib::ThreadPool> worker_pool = CreateDroppingThreadPool();
  if (!worker_pool) {
    ReportFailure("failed to create fallback ThreadPool");
    return 1;
  }
  std::unique_ptr<netlib::SubReactor> first_sub_reactor =
      netlib::SubReactor::Create(1, *worker_pool,
                                 MakeConnectionCallbacks(first_observation));
  std::unique_ptr<netlib::SubReactor> second_sub_reactor =
      netlib::SubReactor::Create(1, *worker_pool,
                                 MakeConnectionCallbacks(second_observation));
  if (!first_sub_reactor || !second_sub_reactor) {
    ReportFailure("failed to create fallback SubReactors");
    return 1;
  }

  netlib::UniqueFd queued_connection(dup(STDIN_FILENO));
  if (!queued_connection ||
      !first_sub_reactor->AdoptConnection(queued_connection)) {
    ReportFailure("failed to fill first SubReactor queue");
    return 1;
  }

  std::unique_ptr<netlib::MainReactor> main_reactor =
      netlib::MainReactor::Create(
          0, {first_sub_reactor.get(), second_sub_reactor.get()});
  if (!main_reactor) {
    ReportFailure("failed to create fallback MainReactor");
    return 1;
  }

  std::thread second_sub_thread(
      [&second_sub_reactor] { second_sub_reactor->Run(); });
  std::thread main_thread([&main_reactor] { main_reactor->Run(); });

  bool success = true;
  netlib::UniqueFd client;
  if (!ConnectTo(main_reactor->port(), &client)) {
    ReportFailure("failed to connect fallback client");
    success = false;
  }
  if (success && !WaitForConnected(second_observation)) {
    ReportFailure("connection was not dispatched to the next SubReactor");
    success = false;
  }

  {
    std::lock_guard<std::mutex> lock(first_observation.mutex);
    if (first_observation.connected) {
      ReportFailure("full SubReactor unexpectedly received the connection");
      success = false;
    }
  }

  if (client) {
    shutdown(client.get(), SHUT_WR);
    client.Reset();
  }
  if (success && !WaitForClosed(second_observation)) {
    ReportFailure("fallback SubReactor did not observe close");
    success = false;
  }

  main_reactor->Stop();
  first_sub_reactor->Stop();
  second_sub_reactor->Stop();
  main_thread.join();
  second_sub_thread.join();
  worker_pool->Stop();
  return success ? 0 : 1;
}

int RunOversizedFrameTest() {
  Observation observation;
  std::unique_ptr<netlib::ThreadPool> worker_pool = CreateDroppingThreadPool();
  if (!worker_pool) {
    ReportFailure("failed to create protocol-test ThreadPool");
    return 1;
  }
  std::unique_ptr<netlib::SubReactor> sub_reactor = netlib::SubReactor::Create(
      16, *worker_pool, MakeConnectionCallbacks(observation));
  if (!sub_reactor) {
    ReportFailure("failed to create protocol-test SubReactor");
    return 1;
  }

  std::unique_ptr<netlib::MainReactor> main_reactor =
      netlib::MainReactor::Create(0, {sub_reactor.get()});
  if (!main_reactor) {
    ReportFailure("failed to create protocol-test MainReactor");
    return 1;
  }

  std::thread sub_thread([&sub_reactor] { sub_reactor->Run(); });
  std::thread main_thread([&main_reactor] { main_reactor->Run(); });

  bool success = true;
  netlib::UniqueFd client;
  if (!ConnectTo(main_reactor->port(), &client) ||
      !SendAll(client.get(),
               MakeLengthPrefix(netlib::kMaxFramePayloadSize + 1U))) {
    ReportFailure("failed to send oversized frame header");
    success = false;
  }
  if (success && !WaitForClosed(observation)) {
    ReportFailure("oversized frame did not close the connection");
    success = false;
  }

  {
    std::lock_guard<std::mutex> lock(observation.mutex);
    if (observation.frame_count != 0) {
      ReportFailure("oversized frame was delivered to business");
      success = false;
    }
  }

  client.Reset();
  main_reactor->Stop();
  sub_reactor->Stop();
  main_thread.join();
  sub_thread.join();
  worker_pool->Stop();
  return success ? 0 : 1;
}

int RunReactorTest() {
  Observation first_observation;
  Observation second_observation;
  std::unique_ptr<CountingSink> log_sink = std::make_unique<CountingSink>();
  CountingSink* log_sink_pointer = log_sink.get();
  netlib::log::LoggerOptions logger_options{
      .name = "netlib.reactor.test",
      .minimum_level = netlib::log::LogLevel::kDebug,
      .queue_capacity = 256,
      .color_mode = netlib::log::ColorMode::kNever,
      .flush_each_record = false,
  };
  std::unique_ptr<netlib::log::Logger> logger = netlib::log::Logger::Create(
      std::move(logger_options), std::move(log_sink));
  if (!logger) {
    ReportFailure("failed to create Reactor logger");
    return 1;
  }
  netlib::ThreadPool::Handler frame_handler =
      [&first_observation, &second_observation](netlib::FrameMessage message) {
        Observation* observation = nullptr;
        if (!message.payload.empty() && message.payload.front() == 'a') {
          observation = &first_observation;
        } else if (!message.payload.empty()) {
          observation = &second_observation;
        }
        if (observation == nullptr) {
          return;
        }

        std::lock_guard<std::mutex> lock(observation->mutex);
        observation->data.append(message.payload.data(),
                                 message.payload.size());
        ++observation->frame_count;
        observation->condition.notify_all();
      };
  std::unique_ptr<netlib::ThreadPool> worker_pool =
      netlib::ThreadPool::Create(2, 32, std::move(frame_handler), logger.get());
  if (!worker_pool) {
    ReportFailure("failed to create Reactor ThreadPool");
    return 1;
  }
  std::unique_ptr<netlib::SubReactor> first_sub_reactor =
      netlib::SubReactor::Create(16, *worker_pool,
                                 MakeConnectionCallbacks(first_observation),
                                 logger.get());
  std::unique_ptr<netlib::SubReactor> second_sub_reactor =
      netlib::SubReactor::Create(16, *worker_pool,
                                 MakeConnectionCallbacks(second_observation),
                                 logger.get());
  if (!first_sub_reactor || !second_sub_reactor) {
    ReportFailure("failed to create SubReactors");
    return 1;
  }

  std::unique_ptr<netlib::MainReactor> main_reactor =
      netlib::MainReactor::Create(
          0, {first_sub_reactor.get(), second_sub_reactor.get()}, logger.get());
  if (!main_reactor) {
    ReportFailure("failed to create MainReactor");
    return 1;
  }

  std::thread first_sub_thread(
      [&first_sub_reactor] { first_sub_reactor->Run(); });
  std::thread second_sub_thread(
      [&second_sub_reactor] { second_sub_reactor->Run(); });
  std::thread main_thread([&main_reactor] { main_reactor->Run(); });

  bool success = true;
  netlib::UniqueFd first_connection;
  netlib::UniqueFd second_connection;
  if (!ConnectTo(main_reactor->port(), &first_connection) ||
      !ConnectTo(main_reactor->port(), &second_connection)) {
    ReportFailure("failed to connect clients");
    success = false;
  }

  if (success && (!WaitForConnected(first_observation) ||
                  !WaitForConnected(second_observation))) {
    ReportFailure("round-robin handoff did not reach both SubReactors");
    success = false;
  }

  const std::string first_payload(128 * 1024, 'a');
  const std::string second_payload(128 * 1024, 'b');
  const std::string third_payload(64 * 1024, 'c');
  const std::string first_frame = MakeFrame(first_payload);
  const std::string second_frame = MakeFrame(second_payload);
  const std::string third_frame = MakeFrame(third_payload);
  const std::size_t first_split = first_frame.size() / 2;
  const std::string second_wire = second_frame + third_frame;
  const std::string second_expected = second_payload + third_payload;
  if (success &&
      (!SendAll(first_connection.get(),
                std::string_view(first_frame).substr(0, first_split)) ||
       !SendAll(first_connection.get(),
                std::string_view(first_frame).substr(first_split)) ||
       !SendAll(second_connection.get(), second_wire))) {
    ReportFailure("failed to send test data");
    success = false;
  }

  if (success && (!WaitForData(first_observation, first_payload, 1) ||
                  !WaitForData(second_observation, second_expected, 2))) {
    ReportFailure("SubReactors did not assemble complete frames");
    success = false;
  }

  if (first_connection) {
    shutdown(first_connection.get(), SHUT_WR);
    first_connection.Reset();
  }
  if (second_connection) {
    shutdown(second_connection.get(), SHUT_WR);
    second_connection.Reset();
  }
  if (success && (!WaitForClosed(first_observation) ||
                  !WaitForClosed(second_observation))) {
    ReportFailure("SubReactors did not observe close");
    success = false;
  }

  main_reactor->Stop();
  first_sub_reactor->Stop();
  second_sub_reactor->Stop();
  main_thread.join();
  first_sub_thread.join();
  second_sub_thread.join();
  worker_pool->Stop();
  logger->Flush();
  if (log_sink_pointer->record_count() == 0) {
    ReportFailure("Reactor lifecycle did not emit logs");
    success = false;
  }
  logger->Stop();
  return success ? 0 : 1;
}

}  // namespace

int main() {
  int capacity_result = RunPendingCapacityTest();
  if (capacity_result != 0) {
    return capacity_result;
  }
  int thread_pool_result = RunThreadPoolDrainTest();
  if (thread_pool_result != 0) {
    return thread_pool_result;
  }
  int fallback_result = RunFullSubReactorFallbackTest();
  if (fallback_result != 0) {
    return fallback_result;
  }
  int protocol_result = RunOversizedFrameTest();
  if (protocol_result != 0) {
    return protocol_result;
  }
  return RunReactorTest();
}
