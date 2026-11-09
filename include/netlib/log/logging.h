#ifndef NETLIB_INCLUDE_NETLIB_LOG_LOGGING_H_
#define NETLIB_INCLUDE_NETLIB_LOG_LOGGING_H_

#include <fmt/format.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <source_location>
#include <string>
#include <string_view>
#include <utility>

namespace netlib::log {

enum class LogLevel : std::uint8_t {
  kTrace,
  kDebug,
  kInfo,
  kWarn,
  kError,
  kFatal,
};

enum class ColorMode : std::uint8_t {
  kAuto,
  kAlways,
  kNever,
};

// A sink is called serially by the Logger writer thread.
struct LogRecord {
  std::chrono::system_clock::time_point timestamp{};
  LogLevel level = LogLevel::kInfo;
  std::uint64_t sequence = 0;
  std::uint64_t thread_id = 0;
  std::uint64_t dropped_messages = 0;
  std::source_location source{};
  std::string_view logger_name;
  std::string message;
};

class LogSink {
 public:
  LogSink() = default;
  LogSink(const LogSink&) = delete;
  LogSink& operator=(const LogSink&) = delete;
  LogSink(LogSink&&) = delete;
  LogSink& operator=(LogSink&&) = delete;
  virtual ~LogSink() = default;

  virtual void Write(const LogRecord& record) = 0;
  virtual void Flush() = 0;
};

struct LoggerOptions {
  std::string name;
  LogLevel minimum_level = LogLevel::kInfo;
  std::size_t queue_capacity = 8192;
  ColorMode color_mode = ColorMode::kAuto;
  bool flush_each_record = false;
  // Zero disables background flush requests; negative intervals are invalid.
  std::chrono::milliseconds flush_interval{1000};
};

// Process-wide logger with one detached asynchronous sink writer.
// Initialize() succeeds at most once. The logger is never stopped or replaced.
class Logger {
 public:
  Logger() = delete;
  ~Logger() = delete;
  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;
  Logger(Logger&&) = delete;
  Logger& operator=(Logger&&) = delete;

  static bool Initialize(LoggerOptions options, std::unique_ptr<LogSink> sink);
  static bool InitializeStdout(LoggerOptions options,
                               std::FILE* output = stdout);
  static bool IsInitialized() noexcept;
  static bool IsEnabled(LogLevel level) noexcept;
  static bool Log(
      LogLevel level, std::string_view message,
      std::source_location source = std::source_location::current());
  template <typename... Args>
  static bool Logf(LogLevel level, std::source_location source,
                   fmt::format_string<Args...> format_string, Args&&... args) {
    if (!IsEnabled(level)) {
      return false;
    }
    return LogFormatted(
        level, fmt::format(format_string, std::forward<Args>(args)...), source);
  }
  // Waits for all previously accepted logs to be written and the sink flushed.
  // Later submissions may proceed. False before initialization or after sink
  // failure. Flush must not be called from a sink callback.
  static bool Flush();
  static std::size_t queued_count() noexcept;
  static std::uint64_t dropped_count() noexcept;

 private:
  class Impl;
  class Registry;

  static Impl* GetImpl() noexcept;
  static bool LogFormatted(LogLevel level, std::string message,
                           std::source_location source);
};

}  // namespace netlib::log

#define NETLIB_LOG(level, format, ...)                                      \
  do {                                                                      \
    if (::netlib::log::Logger::IsEnabled(level)) {                          \
      ::netlib::log::Logger::Logf(level, ::std::source_location::current(), \
                                  format __VA_OPT__(, __VA_ARGS__));        \
    }                                                                       \
  } while (false)

#define NETLIB_LOG_TRACE(...) \
  NETLIB_LOG(::netlib::log::LogLevel::kTrace, __VA_ARGS__)
#define NETLIB_LOG_DEBUG(...) \
  NETLIB_LOG(::netlib::log::LogLevel::kDebug, __VA_ARGS__)
#define NETLIB_LOG_INFO(...) \
  NETLIB_LOG(::netlib::log::LogLevel::kInfo, __VA_ARGS__)
#define NETLIB_LOG_WARN(...) \
  NETLIB_LOG(::netlib::log::LogLevel::kWarn, __VA_ARGS__)
#define NETLIB_LOG_ERROR(...) \
  NETLIB_LOG(::netlib::log::LogLevel::kError, __VA_ARGS__)
#define NETLIB_LOG_FATAL(...) \
  NETLIB_LOG(::netlib::log::LogLevel::kFatal, __VA_ARGS__)

#endif  // NETLIB_INCLUDE_NETLIB_LOG_LOGGING_H_
