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

// Multi-producer logger with one asynchronous sink writer.
class Logger {
 public:
  static std::unique_ptr<Logger> Create(LoggerOptions options,
                                        std::unique_ptr<LogSink> sink);
  static std::unique_ptr<Logger> CreateStdout(LoggerOptions options,
                                              std::FILE* output = stdout);

  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;
  Logger(Logger&&) = delete;
  Logger& operator=(Logger&&) = delete;
  ~Logger();

  bool IsEnabled(LogLevel level) const;
  bool Log(LogLevel level, std::string_view message,
           std::source_location source = std::source_location::current());
  template <typename... Args>
  bool Logf(LogLevel level, std::source_location source,
            fmt::format_string<Args...> format_string, Args&&... args) {
    if (!IsEnabled(level)) {
      return false;
    }
    return LogFormatted(
        level, fmt::format(format_string, std::forward<Args>(args)...), source);
  }
  // Waits for all previously accepted logs to be written and the sink flushed.
  // Later submissions may proceed. False after shutdown or sink failure.
  // Flush/Stop must not be called from a sink callback.
  bool Flush();
  // Closes admission, drains accepted logs, flushes, and joins all background
  // threads. Concurrent callers all wait for completion.
  void Stop();
  std::size_t queued_count() const;
  std::uint64_t dropped_count() const;

 private:
  class Impl;

  explicit Logger(std::unique_ptr<Impl> impl);
  bool LogFormatted(LogLevel level, std::string message,
                    std::source_location source);

  std::unique_ptr<Impl> impl_;
};

// Installs a Logger for the current scope and restores the previous default.
// The Logger must outlive the scope. Nested or concurrent scopes are not
// supported.
class DefaultLoggerScope {
 public:
  explicit DefaultLoggerScope(Logger& logger) noexcept;
  ~DefaultLoggerScope() noexcept;

  DefaultLoggerScope(const DefaultLoggerScope&) = delete;
  DefaultLoggerScope& operator=(const DefaultLoggerScope&) = delete;
  DefaultLoggerScope(DefaultLoggerScope&&) = delete;
  DefaultLoggerScope& operator=(DefaultLoggerScope&&) = delete;

 private:
  Logger* previous_;
};

// Sets the process-wide non-owning fallback Logger. The caller must clear or
// replace it before destroying the Logger. Safe to call concurrently with
// logging; an explicit NETLIB_LOG_*_TO target always takes precedence.
void SetDefaultLogger(Logger* logger) noexcept;
Logger* DefaultLogger() noexcept;

inline Logger* ResolveLogger(Logger* logger) noexcept {
  return logger != nullptr ? logger : DefaultLogger();
}
inline Logger* ResolveLogger(Logger& logger) noexcept { return &logger; }
inline Logger* ResolveLogger(std::nullptr_t) noexcept {
  return DefaultLogger();
}

}  // namespace netlib::log

#define NETLIB_LOG_TO(logger, level, format, ...)                   \
  do {                                                              \
    auto* netlib_log_target = ::netlib::log::ResolveLogger(logger); \
    if (netlib_log_target != nullptr &&                             \
        (netlib_log_target)->IsEnabled(level)) {                    \
      (netlib_log_target)                                           \
          ->Logf(level, ::std::source_location::current(),          \
                 format __VA_OPT__(, __VA_ARGS__));                 \
    }                                                               \
  } while (false)

#define NETLIB_LOG_TRACE_TO(logger, ...) \
  NETLIB_LOG_TO(logger, ::netlib::log::LogLevel::kTrace, __VA_ARGS__)
#define NETLIB_LOG_DEBUG_TO(logger, ...) \
  NETLIB_LOG_TO(logger, ::netlib::log::LogLevel::kDebug, __VA_ARGS__)
#define NETLIB_LOG_INFO_TO(logger, ...) \
  NETLIB_LOG_TO(logger, ::netlib::log::LogLevel::kInfo, __VA_ARGS__)
#define NETLIB_LOG_WARN_TO(logger, ...) \
  NETLIB_LOG_TO(logger, ::netlib::log::LogLevel::kWarn, __VA_ARGS__)
#define NETLIB_LOG_ERROR_TO(logger, ...) \
  NETLIB_LOG_TO(logger, ::netlib::log::LogLevel::kError, __VA_ARGS__)
#define NETLIB_LOG_FATAL_TO(logger, ...) \
  NETLIB_LOG_TO(logger, ::netlib::log::LogLevel::kFatal, __VA_ARGS__)

#define NETLIB_LOG(level, ...) NETLIB_LOG_TO(nullptr, level, __VA_ARGS__)
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
