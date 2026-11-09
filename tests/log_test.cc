#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <source_location>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "netlib/log/logging.h"

namespace {

void ReportFailure(const char* message) {
  std::fprintf(stderr, "%s\n", message);
}

class CapturingSink final : public netlib::log::LogSink {
 public:
  void Write(const netlib::log::LogRecord& record) override {
    std::lock_guard<std::mutex> lock(mutex_);
    records_.push_back(record);
  }

  void Flush() override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++flush_count_;
  }

  std::size_t record_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return records_.size();
  }

  std::size_t flush_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return flush_count_;
  }

 private:
  mutable std::mutex mutex_;
  std::vector<netlib::log::LogRecord> records_;
  std::size_t flush_count_ = 0;
};

std::string ReadFile(std::FILE* file) {
  if (std::fflush(file) != 0 || std::fseek(file, 0, SEEK_SET) != 0) {
    return {};
  }

  std::string content;
  char buffer[4096];
  while (true) {
    std::size_t size = std::fread(buffer, 1, sizeof(buffer), file);
    content.append(buffer, size);
    if (size < sizeof(buffer)) {
      break;
    }
  }
  return content;
}

int RunStdoutTest(bool colored) {
  std::FILE* output = std::tmpfile();
  if (output == nullptr) {
    ReportFailure("failed to create temporary log output");
    return 1;
  }

  netlib::log::LoggerOptions options{
      .name = colored ? "netlib.color" : "netlib.test",
      .minimum_level = netlib::log::LogLevel::kTrace,
      .queue_capacity = 128,
      .color_mode = colored ? netlib::log::ColorMode::kAlways
                            : netlib::log::ColorMode::kNever,
      .flush_each_record = false,
      .flush_interval = std::chrono::milliseconds::zero(),
  };
  if (!netlib::log::Logger::InitializeStdout(std::move(options), output)) {
    std::fclose(output);
    ReportFailure("failed to initialize stdout logger");
    return 1;
  }

  if (colored) {
    NETLIB_LOG_WARN("colored {}\nnext", "message");
  } else {
    NETLIB_LOG_INFO("hello {}", "world\nnext");
    NETLIB_LOG_INFO("count={} name={}", 42, "worker");
    NETLIB_LOG_ERROR("escape:\x1b[31mred");
  }
  if (!netlib::log::Logger::Flush()) {
    std::fclose(output);
    ReportFailure("failed to flush stdout logger");
    return 1;
  }
  std::string content = ReadFile(output);
  std::fclose(output);

  if (!colored &&
      (content.find("INFO ") == std::string::npos ||
       content.find("seq=") == std::string::npos ||
       content.find("tid=") == std::string::npos ||
       content.find("netlib.test") == std::string::npos ||
       content.find("log_test.cc") == std::string::npos ||
       content.find("hello world\n    | next") == std::string::npos ||
       content.find("count=42 name=worker") == std::string::npos ||
       content.find("\\x1B[31mred") == std::string::npos ||
       content.find('\x1b') != std::string::npos)) {
    ReportFailure("stdout log format is incomplete or unsafe");
    return 1;
  }
  if (colored &&
      (!content.starts_with("\x1b[0;90m") ||
       content.find("\x1b[33mWARN ") == std::string::npos ||
       content.find("\x1b[34mseq=") == std::string::npos ||
       content.find("\x1b[35mtid=") == std::string::npos ||
       content.find("\x1b[36mnetlib.color") == std::string::npos ||
       content.find("\x1b[94mlog_test.cc:") == std::string::npos ||
       content.find("\x1b[0;90m | \x1b[0mcolored message\n"
                    "\x1b[0;90m    | \x1b[0mnext\n") == std::string::npos)) {
    ReportFailure("metadata colors are missing or leak into the message");
    return 1;
  }
  return 0;
}

int RunConcurrentLoggerTest() {
  constexpr std::size_t kThreadCount = 4;
  constexpr std::size_t kMessagesPerThread = 500;
  constexpr std::size_t kFlushCount = 8;

  std::unique_ptr<CapturingSink> sink = std::make_unique<CapturingSink>();
  CapturingSink* sink_pointer = sink.get();
  netlib::log::LoggerOptions options{
      .name = "netlib.concurrent",
      .minimum_level = netlib::log::LogLevel::kTrace,
      .queue_capacity = kThreadCount * kMessagesPerThread,
      .color_mode = netlib::log::ColorMode::kNever,
      .flush_each_record = false,
      .flush_interval = std::chrono::milliseconds::zero(),
  };
  if (!netlib::log::Logger::Initialize(std::move(options), std::move(sink))) {
    ReportFailure("failed to initialize concurrent logger");
    return 1;
  }

  std::atomic<std::size_t> enqueued{0};
  std::vector<std::thread> producers;
  producers.reserve(kThreadCount);
  for (std::size_t thread_index = 0; thread_index < kThreadCount;
       ++thread_index) {
    producers.emplace_back([&enqueued, thread_index] {
      for (std::size_t message_index = 0; message_index < kMessagesPerThread;
           ++message_index) {
        if (netlib::log::Logger::Logf(netlib::log::LogLevel::kInfo,
                                      std::source_location::current(),
                                      "concurrent thread={} message={}",
                                      thread_index, message_index)) {
          enqueued.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  for (std::thread& producer : producers) {
    producer.join();
  }

  std::atomic<std::size_t> successful_flushes{0};
  std::vector<std::thread> flushers;
  flushers.reserve(kFlushCount);
  for (std::size_t i = 0; i < kFlushCount; ++i) {
    flushers.emplace_back([&successful_flushes] {
      if (netlib::log::Logger::Flush()) {
        successful_flushes.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  for (std::thread& flusher : flushers) {
    flusher.join();
  }
  if (successful_flushes.load(std::memory_order_relaxed) != kFlushCount) {
    ReportFailure("concurrent flush calls did not complete");
    return 1;
  }
  if (enqueued.load(std::memory_order_relaxed) !=
          kThreadCount * kMessagesPerThread ||
      sink_pointer->record_count() != kThreadCount * kMessagesPerThread ||
      netlib::log::Logger::dropped_count() != 0 ||
      netlib::log::Logger::queued_count() != 0) {
    ReportFailure("concurrent logger lost messages");
    return 1;
  }
  if (sink_pointer->flush_count() < kFlushCount) {
    ReportFailure("concurrent explicit flushes were not delivered");
    return 1;
  }
  return 0;
}

int RunInvalidConfigurationTest() {
  netlib::log::LoggerOptions options{
      .name = "",
      .queue_capacity = 1,
      .flush_interval = std::chrono::milliseconds::zero(),
  };
  if (netlib::log::Logger::Initialize(std::move(options),
                                      std::make_unique<CapturingSink>()) ||
      netlib::log::Logger::IsInitialized()) {
    ReportFailure("logger accepted an invalid configuration");
    return 1;
  }
  return 0;
}

int RunSingleInitializationTest() {
  std::unique_ptr<CapturingSink> first = std::make_unique<CapturingSink>();
  CapturingSink* first_pointer = first.get();
  netlib::log::LoggerOptions options{
      .name = "netlib.singleton",
      .queue_capacity = 16,
      .flush_interval = std::chrono::milliseconds::zero(),
  };
  if (!netlib::log::Logger::Initialize(std::move(options), std::move(first))) {
    ReportFailure("failed to initialize singleton logger");
    return 1;
  }

  std::unique_ptr<CapturingSink> second = std::make_unique<CapturingSink>();
  netlib::log::LoggerOptions duplicate_options{
      .name = "netlib.duplicate",
      .queue_capacity = 16,
      .flush_interval = std::chrono::milliseconds::zero(),
  };
  if (netlib::log::Logger::Initialize(std::move(duplicate_options),
                                      std::move(second))) {
    ReportFailure("logger was replaced after initialization");
    return 1;
  }

  NETLIB_LOG_INFO("single {}", 1);
  if (!netlib::log::Logger::Flush() || first_pointer->record_count() != 1) {
    ReportFailure("singleton logger routing was incorrect");
    return 1;
  }
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    return 2;
  }
  std::string_view mode = argv[1];
  if (mode == "stdout-colorless") {
    return RunStdoutTest(false);
  }
  if (mode == "stdout-colored") {
    return RunStdoutTest(true);
  }
  if (mode == "concurrent") {
    return RunConcurrentLoggerTest();
  }
  if (mode == "invalid") {
    return RunInvalidConfigurationTest();
  }
  if (mode == "singleton") {
    return RunSingleInitializationTest();
  }
  return 2;
}
