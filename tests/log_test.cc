#include <unistd.h>

#include <atomic>
#include <cstddef>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
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

int RunStdoutFormatTest() {
  std::FILE* colorless_output = std::tmpfile();
  std::FILE* colored_output = std::tmpfile();
  if (colorless_output == nullptr || colored_output == nullptr) {
    ReportFailure("failed to create temporary log output");
    return 1;
  }

  netlib::log::LoggerOptions colorless_options{
      .name = "netlib.test",
      .minimum_level = netlib::log::LogLevel::kTrace,
      .queue_capacity = 128,
      .color_mode = netlib::log::ColorMode::kNever,
      .flush_each_record = true,
  };
  std::unique_ptr<netlib::log::Logger> colorless_logger =
      netlib::log::Logger::CreateStdout(std::move(colorless_options),
                                        colorless_output);
  if (!colorless_logger) {
    ReportFailure("failed to create colorless logger");
    return 1;
  }

  NETLIB_LOG_INFO(*colorless_logger, "hello\nworld");
  NETLIB_LOG_ERROR(*colorless_logger, "escape:\x1b[31mred");
  if (!colorless_logger->Flush()) {
    ReportFailure("failed to flush colorless logger");
    return 1;
  }
  colorless_logger->Stop();
  std::string colorless = ReadFile(colorless_output);
  std::fclose(colorless_output);

  if (colorless.find("INFO ") == std::string::npos ||
      colorless.find("seq=") == std::string::npos ||
      colorless.find("tid=") == std::string::npos ||
      colorless.find("netlib.test") == std::string::npos ||
      colorless.find("log_test.cc") == std::string::npos ||
      colorless.find("hello\n    | world") == std::string::npos ||
      colorless.find("\\x1B[31mred") == std::string::npos) {
    ReportFailure("stdout log format is incomplete or unsafe");
    return 1;
  }

  netlib::log::LoggerOptions colored_options{
      .name = "netlib.color",
      .minimum_level = netlib::log::LogLevel::kTrace,
      .queue_capacity = 128,
      .color_mode = netlib::log::ColorMode::kAlways,
      .flush_each_record = true,
  };
  std::unique_ptr<netlib::log::Logger> colored_logger =
      netlib::log::Logger::CreateStdout(std::move(colored_options),
                                        colored_output);
  if (!colored_logger) {
    ReportFailure("failed to create colored logger");
    return 1;
  }
  NETLIB_LOG_WARN(*colored_logger, "colored message");
  if (!colored_logger->Flush()) {
    ReportFailure("failed to flush colored logger");
    return 1;
  }
  colored_logger->Stop();
  std::string colored = ReadFile(colored_output);
  std::fclose(colored_output);
  if (colored.find("\x1b[33mWARN ") == std::string::npos ||
      colored.find("\x1b[0m") == std::string::npos) {
    ReportFailure("stdout log did not contain ANSI color");
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
  };
  std::unique_ptr<netlib::log::Logger> logger =
      netlib::log::Logger::Create(std::move(options), std::move(sink));
  if (!logger) {
    ReportFailure("failed to create concurrent logger");
    return 1;
  }

  std::atomic<std::size_t> enqueued{0};
  std::vector<std::thread> producers;
  producers.reserve(kThreadCount);
  for (std::size_t thread_index = 0; thread_index < kThreadCount;
       ++thread_index) {
    producers.emplace_back([logger_pointer = logger.get(), &enqueued] {
      for (std::size_t message_index = 0; message_index < kMessagesPerThread;
           ++message_index) {
        if (logger_pointer->Log(netlib::log::LogLevel::kInfo,
                                "concurrent message")) {
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
    flushers.emplace_back([logger_pointer = logger.get(), &successful_flushes] {
      if (logger_pointer->Flush()) {
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
  logger->Stop();
  if (enqueued.load(std::memory_order_relaxed) !=
          kThreadCount * kMessagesPerThread ||
      sink_pointer->record_count() != kThreadCount * kMessagesPerThread ||
      logger->dropped_count() != 0 || logger->queued_count() != 0) {
    ReportFailure("concurrent logger lost messages");
    return 1;
  }
  if (sink_pointer->flush_count() < kFlushCount + 1) {
    ReportFailure("concurrent explicit and stop flushes were not delivered");
    return 1;
  }
  return 0;
}

int RunInvalidConfigurationTest() {
  netlib::log::LoggerOptions options{
      .name = "",
      .minimum_level = netlib::log::LogLevel::kInfo,
      .queue_capacity = 1,
      .color_mode = netlib::log::ColorMode::kNever,
      .flush_each_record = false,
  };
  if (netlib::log::Logger::Create(std::move(options),
                                  std::make_unique<CapturingSink>())) {
    ReportFailure("logger accepted an empty name");
    return 1;
  }
  return 0;
}

}  // namespace

int main() {
  int format_result = RunStdoutFormatTest();
  if (format_result != 0) {
    return format_result;
  }
  int concurrent_result = RunConcurrentLoggerTest();
  if (concurrent_result != 0) {
    return concurrent_result;
  }
  return RunInvalidConfigurationTest();
}
