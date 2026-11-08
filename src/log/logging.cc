#include "netlib/log/logging.h"

#ifdef _WIN32
#include <io.h>
#include <windows.h>
#else
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "concurrentqueue.h"
#include "netlib/timer.h"

namespace netlib::log {
namespace {

constexpr std::size_t kWriterBatchSize = 64;

std::string_view LevelName(LogLevel level) {
  switch (level) {
    case LogLevel::kTrace:
      return "TRACE";
    case LogLevel::kDebug:
      return "DEBUG";
    case LogLevel::kInfo:
      return "INFO ";
    case LogLevel::kWarn:
      return "WARN ";
    case LogLevel::kError:
      return "ERROR";
    case LogLevel::kFatal:
      return "FATAL";
  }
  return "UNKWN";
}

const char* LevelColor(LogLevel level) {
  switch (level) {
    case LogLevel::kTrace:
      return "\x1b[90m";
    case LogLevel::kDebug:
      return "\x1b[36m";
    case LogLevel::kInfo:
      return "\x1b[32m";
    case LogLevel::kWarn:
      return "\x1b[33m";
    case LogLevel::kError:
      return "\x1b[31m";
    case LogLevel::kFatal:
      return "\x1b[1;31m";
  }
  return "";
}

bool IsValidLevel(LogLevel level) {
  return level >= LogLevel::kTrace && level <= LogLevel::kFatal;
}

std::uint64_t CurrentThreadId() {
#ifdef _WIN32
  return static_cast<std::uint64_t>(GetCurrentThreadId());
#else
  return static_cast<std::uint64_t>(static_cast<long>(syscall(SYS_gettid)));
#endif
}

std::string_view BaseName(std::string_view path) {
  std::size_t slash = path.find_last_of("/\\");
  if (slash == std::string_view::npos) {
    return path;
  }
  return path.substr(slash + 1);
}

void AppendUnsigned(std::string& output, std::uint64_t value) {
  std::array<char, 32> buffer{};
  std::to_chars_result result =
      std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
  output.append(buffer.data(), result.ptr);
}

std::string FormatTimestamp(std::chrono::system_clock::time_point timestamp) {
  std::chrono::system_clock::time_point seconds_time =
      std::chrono::time_point_cast<std::chrono::seconds>(timestamp);
  std::chrono::milliseconds milliseconds =
      std::chrono::duration_cast<std::chrono::milliseconds>(timestamp -
                                                            seconds_time);

  std::time_t raw_time = std::chrono::system_clock::to_time_t(seconds_time);
  std::tm local_time{};
#ifdef _WIN32
  if (localtime_s(&local_time, &raw_time) != 0) {
#else
  if (localtime_r(&raw_time, &local_time) == nullptr) {
#endif
    return "0000-00-00 00:00:00.000";
  }

  std::array<char, 32> date{};
  if (std::strftime(date.data(), date.size(), "%Y-%m-%d %H:%M:%S",
                    &local_time) == 0) {
    return "0000-00-00 00:00:00.000";
  }

  std::array<char, 48> result{};
  int size =
      std::snprintf(result.data(), result.size(), "%s.%03lld", date.data(),
                    static_cast<long long>(milliseconds.count()));
  if (size <= 0) {
    return "0000-00-00 00:00:00.000";
  }
  return std::string(result.data(), static_cast<std::size_t>(size));
}

std::string MakePrefix(const LogRecord& record, bool color) {
  std::string prefix;
  prefix.reserve(512);
  // Reset styles between fields too, so FATAL's bold style cannot leak into
  // the other metadata. The business message starts after the final reset.
  const std::string_view separator = color ? "\x1b[0;90m | " : " | ";

  if (color) {
    prefix.append("\x1b[0;90m");
  }
  prefix.append(FormatTimestamp(record.timestamp));

  prefix.append(separator);
  if (color) {
    prefix.append(LevelColor(record.level));
  }
  prefix.append(LevelName(record.level));

  prefix.append(separator);
  if (color) {
    prefix.append("\x1b[34m");
  }
  prefix.append("seq=");
  AppendUnsigned(prefix, record.sequence);
  prefix.append(separator);
  if (color) {
    prefix.append("\x1b[35m");
  }
  prefix.append("tid=");
  AppendUnsigned(prefix, record.thread_id);
  if (record.dropped_messages != 0) {
    prefix.append(separator);
    if (color) {
      prefix.append("\x1b[33m");
    }
    prefix.append("drops=");
    AppendUnsigned(prefix, record.dropped_messages);
  }
  prefix.append(separator);
  if (color) {
    prefix.append("\x1b[36m");
  }
  prefix.append(record.logger_name);
  prefix.append(separator);
  if (color) {
    prefix.append("\x1b[94m");
  }
  prefix.append(BaseName(record.source.file_name()));
  prefix.push_back(':');
  AppendUnsigned(prefix, record.source.line());
  if (record.source.function_name()[0] != '\0') {
    prefix.push_back(' ');
    prefix.append(record.source.function_name());
  }
  prefix.append(separator);
  if (color) {
    prefix.append("\x1b[0m");
  }
  return prefix;
}

bool IsTerminal(std::FILE* output) {
#ifdef _WIN32
  if (_isatty(_fileno(output)) == 0) return false;
  HANDLE handle = reinterpret_cast<HANDLE>(_get_osfhandle(_fileno(output)));
  DWORD mode = 0;
  return GetConsoleMode(handle, &mode) &&
         SetConsoleMode(handle, mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING);
#else
  return isatty(fileno(output)) != 0;
#endif
}

class StdoutSink final : public LogSink {
 public:
  StdoutSink(ColorMode color_mode, std::FILE* output) : output_(output) {
    color_ = color_mode == ColorMode::kAlways ||
             (color_mode == ColorMode::kAuto && IsTerminal(output_));
  }

  void Write(const LogRecord& record) override {
    std::string prefix = MakePrefix(record, color_);
    std::fwrite(prefix.data(), 1, prefix.size(), output_);
    WriteMessage(record.message);
    std::fputc('\n', output_);
  }

  void Flush() override { std::fflush(output_); }

 private:
  void WriteMessage(std::string_view message) {
    std::size_t segment_start = 0;
    for (std::size_t i = 0; i < message.size(); ++i) {
      char character = message[i];
      if (character == '\n') {
        WriteSegment(message, segment_start, i);
        const std::string_view continuation =
            color_ ? "\n\x1b[0;90m    | \x1b[0m" : "\n    | ";
        std::fwrite(continuation.data(), 1, continuation.size(), output_);
        segment_start = i + 1;
        continue;
      }
      if (character == '\t') {
        WriteSegment(message, segment_start, i);
        std::fwrite("  ", 1, 2, output_);
        segment_start = i + 1;
        continue;
      }
      unsigned char byte = static_cast<unsigned char>(character);
      if (byte < 0x20U || byte == 0x7FU) {
        WriteSegment(message, segment_start, i);
        char escaped[5] = {'\\', 'x', '0', '0', '\0'};
        constexpr char kHex[] = "0123456789ABCDEF";
        escaped[2] = kHex[(byte >> 4U) & 0x0FU];
        escaped[3] = kHex[byte & 0x0FU];
        std::fwrite(escaped, 1, 4, output_);
        segment_start = i + 1;
      }
    }
    WriteSegment(message, segment_start, message.size());
  }

  void WriteSegment(std::string_view message, std::size_t start,
                    std::size_t end) {
    if (start != end) {
      std::fwrite(message.data() + start, 1, end - start, output_);
    }
  }

  std::FILE* output_;
  bool color_ = false;
};

}  // namespace

class Logger::Impl {
 public:
  struct FlushRequest {
    std::uint64_t target;
    bool done = false;
  };

  Impl(LoggerOptions options, std::unique_ptr<LogSink> sink)
      : logger_name_(std::move(options.name)),
        minimum_level_(options.minimum_level),
        capacity_(options.queue_capacity),
        flush_each_record_(options.flush_each_record),
        queue_(capacity_),
        sink_(std::move(sink)),
        consumer_(queue_) {
    if (options.flush_interval.count() > 0) {
      timer_ = std::make_unique<TimerScheduler>();
      timer_->ScheduleEvery(options.flush_interval, [this] {
        std::lock_guard<std::mutex> lock(mutex_);
        // Keep the first pending target fixed: continuous logging must not
        // postpone its completion. Coalesce ticks while a request is pending.
        if (!stopping_ && !periodic_target_) {
          periodic_target_ = accepted_;
          work_.notify_one();
        }
      });
    }
    writer_ = std::thread([this] { WriterLoop(); });
  }
  ~Impl() { Stop(); }

  bool IsEnabled(LogLevel level) const {
    return IsValidLevel(level) && level >= minimum_level_;
  }
  bool Log(LogLevel level, std::string_view message,
           std::source_location source) {
    return LogOwned(level, std::string(message), source);
  }
  bool LogOwned(LogLevel level, std::string message,
                std::source_location source) {
    if (!IsEnabled(level)) return false;
    LogRecord record{
        .timestamp = std::chrono::system_clock::now(),
        .level = level,
        .thread_id = CurrentThreadId(),
        .source = source,
        .logger_name = logger_name_,
        .message = std::move(message),
    };
    // Admission and publication are atomic with respect to Flush and Stop.
    // No sink I/O, formatting, or completion wait holds this lock.
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) return false;
    if (completion_.size() >= capacity_) {
      Drop();
      return false;
    }
    record.sequence = accepted_;
    record.dropped_messages = unreported_drops_;
    try {
      completion_.push_back(false);
    } catch (...) {
      Drop();
      return false;
    }
    bool enqueued = false;
    try {
      enqueued = queue_.enqueue(std::move(record));
    } catch (...) {
    }
    if (!enqueued) {
      completion_.pop_back();  // No ticket was accepted: no permanent hole.
      Drop();
      return false;
    }
    unreported_drops_ = 0;
    ++accepted_;
    ++queued_;
    work_.notify_one();
    return true;
  }
  bool Flush() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (stopping_) return false;
    std::shared_ptr<FlushRequest> request =
        std::make_shared<FlushRequest>(FlushRequest{accepted_});
    requests_.push_back(request);
    work_.notify_one();
    progress_.wait(lock, [&] { return request->done || failed_; });
    return request->done;
  }
  void Stop() {
    // Serializes join, so even concurrent Stop calls wait for full shutdown.
    std::lock_guard<std::mutex> join_lock(stop_mutex_);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopping_ = true;
      work_.notify_one();
    }
    if (timer_) timer_->Stop();
    if (writer_.joinable()) writer_.join();
  }
  std::size_t queued_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queued_;
  }
  std::uint64_t dropped_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return dropped_;
  }

 private:
  void Drop() {
    ++dropped_;
    ++unreported_drops_;
  }
  // Called by writer between records, never while holding the admission lock.
  bool ServiceFlushes() {
    for (;;) {
      std::shared_ptr<FlushRequest> request;
      bool final_flush = false;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!requests_.empty() && requests_.front()->target <= completed_) {
          request = requests_.front();
          requests_.pop_front();
        } else if (periodic_target_ && *periodic_target_ <= completed_) {
          periodic_target_.reset();
        } else if (stopping_ && completed_ == accepted_) {
          final_flush = true;
        } else {
          return false;
        }
      }
      sink_->Flush();
      if (request) {
        std::lock_guard<std::mutex> lock(mutex_);
        request->done = true;
        progress_.notify_all();
      }
      if (final_flush) return true;
    }
  }
  void WriterLoop() {
    try {
      std::array<LogRecord, kWriterBatchSize> records{};
      for (;;) {
        if (ServiceFlushes()) return;
        std::size_t count;
        {
          std::unique_lock<std::mutex> lock(mutex_);
          work_.wait(lock, [&] {
            return queued_ != 0 || stopping_ ||
                   (!requests_.empty() &&
                    requests_.front()->target <= completed_) ||
                   (periodic_target_ && *periodic_target_ <= completed_);
          });
          count = queue_.try_dequeue_bulk(consumer_, records.data(),
                                          records.size());
        }
        for (std::size_t i = 0; i < count; ++i) {
          LogRecord& record = records[i];
          sink_->Write(record);
          if (flush_each_record_ || record.level >= LogLevel::kWarn)
            sink_->Flush();
          {
            std::lock_guard<std::mutex> lock(mutex_);
            --queued_;
            // A high sequence alone is insufficient: preserve every hole.
            completion_[static_cast<std::size_t>(record.sequence -
                                                 completed_)] = true;
            while (!completion_.empty() && completion_.front()) {
              completion_.pop_front();
              ++completed_;
            }
          }
          if (ServiceFlushes()) return;
        }
      }
    } catch (...) {
      // Sink failures must wake Flush callers instead of hanging forever.
      std::lock_guard<std::mutex> lock(mutex_);
      failed_ = true;
      stopping_ = true;
      progress_.notify_all();
    }
  }

  std::string logger_name_;
  const LogLevel minimum_level_;
  const std::size_t capacity_;
  const bool flush_each_record_;
  moodycamel::ConcurrentQueue<LogRecord> queue_;
  std::unique_ptr<LogSink> sink_;
  moodycamel::ConsumerToken consumer_;
  mutable std::mutex mutex_;
  std::condition_variable work_;
  std::condition_variable progress_;
  std::deque<bool> completion_;
  std::deque<std::shared_ptr<FlushRequest>> requests_;
  std::optional<std::uint64_t> periodic_target_;
  std::uint64_t accepted_ = 0;
  std::uint64_t completed_ = 0;
  std::size_t queued_ = 0;
  std::uint64_t dropped_ = 0;
  std::uint64_t unreported_drops_ = 0;
  bool stopping_ = false;
  bool failed_ = false;
  std::mutex stop_mutex_;
  std::unique_ptr<TimerScheduler> timer_;
  std::thread writer_;
};

std::unique_ptr<Logger> Logger::Create(LoggerOptions options,
                                       std::unique_ptr<LogSink> sink) {
  if (options.name.empty() || options.queue_capacity == 0 || !sink ||
      !IsValidLevel(options.minimum_level) ||
      options.flush_interval.count() < 0) {
    return nullptr;
  }

  std::unique_ptr<Impl> impl =
      std::make_unique<Impl>(std::move(options), std::move(sink));
  return std::unique_ptr<Logger>(new Logger(std::move(impl)));
}

std::unique_ptr<Logger> Logger::CreateStdout(LoggerOptions options,
                                             std::FILE* output) {
  if (output == nullptr) {
    return nullptr;
  }
  ColorMode color_mode = options.color_mode;
  std::unique_ptr<LogSink> sink =
      std::make_unique<StdoutSink>(color_mode, output);
  return Create(std::move(options), std::move(sink));
}

Logger::Logger(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

Logger::~Logger() = default;

bool Logger::IsEnabled(LogLevel level) const { return impl_->IsEnabled(level); }

bool Logger::Log(LogLevel level, std::string_view message,
                 std::source_location source) {
  return impl_->Log(level, message, source);
}

bool Logger::LogFormatted(LogLevel level, std::string message,
                          std::source_location source) {
  return impl_->LogOwned(level, std::move(message), source);
}

bool Logger::Flush() { return impl_->Flush(); }

void Logger::Stop() { impl_->Stop(); }

std::size_t Logger::queued_count() const { return impl_->queued_count(); }

std::uint64_t Logger::dropped_count() const { return impl_->dropped_count(); }

}  // namespace netlib::log
