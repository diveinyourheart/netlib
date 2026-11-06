#ifndef NETLIB_INCLUDE_NETLIB_LOG_LOGGING_H_
#define NETLIB_INCLUDE_NETLIB_LOG_LOGGING_H_

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <source_location>
#include <string>
#include <string_view>

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
  bool Flush();
  void Stop();
  std::size_t queued_count() const;
  std::uint64_t dropped_count() const;

 private:
  class Impl;

  explicit Logger(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

}  // namespace netlib::log

#define NETLIB_LOG(logger, level, message)                               \
  do {                                                                   \
    if ((logger).IsEnabled(level)) {                                     \
      (logger).Log(level, (message), ::std::source_location::current()); \
    }                                                                    \
  } while (false)

#define NETLIB_LOG_TRACE(logger, message) \
  NETLIB_LOG(logger, ::netlib::log::LogLevel::kTrace, message)
#define NETLIB_LOG_DEBUG(logger, message) \
  NETLIB_LOG(logger, ::netlib::log::LogLevel::kDebug, message)
#define NETLIB_LOG_INFO(logger, message) \
  NETLIB_LOG(logger, ::netlib::log::LogLevel::kInfo, message)
#define NETLIB_LOG_WARN(logger, message) \
  NETLIB_LOG(logger, ::netlib::log::LogLevel::kWarn, message)
#define NETLIB_LOG_ERROR(logger, message) \
  NETLIB_LOG(logger, ::netlib::log::LogLevel::kError, message)
#define NETLIB_LOG_FATAL(logger, message) \
  NETLIB_LOG(logger, ::netlib::log::LogLevel::kFatal, message)

#endif  // NETLIB_INCLUDE_NETLIB_LOG_LOGGING_H_
