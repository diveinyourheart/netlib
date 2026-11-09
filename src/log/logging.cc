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
#include <cassert>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
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

struct LogQueueTraits : moodycamel::ConcurrentQueueDefaultTraits {
  // All log submissions share one explicit producer under Logger::mutex_.
  static constexpr std::size_t INITIAL_IMPLICIT_PRODUCER_HASH_SIZE = 0;
};
using LogQueue = moodycamel::ConcurrentQueue<LogRecord, LogQueueTraits>;

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
        producer_(queue_),
        sink_(std::move(sink)),
        consumer_(queue_) {
    if (options.flush_interval.count() > 0) {
      TimerScheduler::ScheduleEvery(options.flush_interval, [this] {
        std::lock_guard<std::mutex> lock(mutex_);
        // Keep the first pending target fixed: continuous logging must not
        // postpone its completion. Coalesce ticks while a request is pending.
        if (!failed_ && !periodic_target_) {
          periodic_target_ = accepted_;
          work_.notify_one();
        }
      });
    }
    std::thread([this] { WriterLoop(); }).detach();
  }
  bool valid() const { return producer_.valid(); }

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
    // Admission and publication are atomic with respect to Flush.
    // No sink I/O, formatting, or completion wait holds this lock.
    std::lock_guard<std::mutex> lock(mutex_);
    if (failed_) return false;
    if (accepted_ - completed_ >= capacity_) {
      Drop();
      return false;
    }
    record.sequence = accepted_;
    record.dropped_messages = unreported_drops_;
    bool enqueued = false;
    try {
      enqueued = queue_.enqueue(producer_, std::move(record));
    } catch (...) {
    }
    if (!enqueued) {
      Drop();
      return false;
    }
    unreported_drops_ = 0;
    ++accepted_;
    work_.notify_one();
    return true;
  }
  bool Flush() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (failed_) return false;
    std::shared_ptr<FlushRequest> request =
        std::make_shared<FlushRequest>(FlushRequest{accepted_});
    requests_.push_back(request);
    work_.notify_one();
    progress_.wait(lock, [&] { return request->done || failed_; });
    return request->done;
  }
  std::size_t queued_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return static_cast<std::size_t>(accepted_ - completed_);
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
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!requests_.empty() && requests_.front()->target <= completed_) {
          request = requests_.front();
          requests_.pop_front();
        } else if (periodic_target_ && *periodic_target_ <= completed_) {
          periodic_target_.reset();
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
    }
  }
  void WriterLoop() {
    try {
      std::array<LogRecord, kWriterBatchSize> records{};
      for (;;) {
        if (ServiceFlushes()) return;
        std::size_t count = 0;
        {
          std::unique_lock<std::mutex> lock(mutex_);
          work_.wait(lock, [&] {
            return accepted_ != completed_ || failed_ ||
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
            // One producer and one consumer preserve acceptance order, even
            // when consecutive submissions originate from different threads.
            assert(record.sequence == completed_);
            ++completed_;
          }
          if (ServiceFlushes()) return;
        }
      }
    } catch (...) {
      // Sink failures must wake Flush callers instead of hanging forever.
      std::lock_guard<std::mutex> lock(mutex_);
      failed_ = true;
      progress_.notify_all();
    }
  }

  std::string logger_name_;
  const LogLevel minimum_level_;
  const std::size_t capacity_;
  const bool flush_each_record_;
  LogQueue queue_;
  moodycamel::ProducerToken producer_;
  std::unique_ptr<LogSink> sink_;
  moodycamel::ConsumerToken consumer_;
  mutable std::mutex mutex_;
  std::condition_variable work_;
  std::condition_variable progress_;
  std::deque<std::shared_ptr<FlushRequest>> requests_;
  std::optional<std::uint64_t> periodic_target_;
  std::uint64_t accepted_ = 0;
  std::uint64_t completed_ = 0;
  std::uint64_t dropped_ = 0;
  std::uint64_t unreported_drops_ = 0;
  bool failed_ = false;
};

class Logger::Registry {
 public:
  static Registry& Instance() noexcept {
    // Reachable for the process lifetime; intentionally not destroyed.
    static Registry* registry = new Registry();
    return *registry;
  }

  std::mutex init_mutex;
  std::atomic<Impl*> impl{nullptr};
};

Logger::Impl* Logger::GetImpl() noexcept {
  return Registry::Instance().impl.load(std::memory_order_acquire);
}

bool Logger::Initialize(LoggerOptions options, std::unique_ptr<LogSink> sink) {
  if (options.name.empty() || options.queue_capacity == 0 || !sink ||
      !IsValidLevel(options.minimum_level) ||
      options.flush_interval.count() < 0) {
    return false;
  }

  Registry& registry = Registry::Instance();
  std::lock_guard<std::mutex> lock(registry.init_mutex);
  if (registry.impl.load(std::memory_order_relaxed) != nullptr) {
    return false;
  }
  Impl* impl = new Impl(std::move(options), std::move(sink));
  if (!impl->valid()) {
    std::abort();
  }
  registry.impl.store(impl, std::memory_order_release);
  return true;
}

bool Logger::InitializeStdout(LoggerOptions options, std::FILE* output) {
  if (output == nullptr) {
    return false;
  }
  ColorMode color_mode = options.color_mode;
  std::unique_ptr<LogSink> sink =
      std::make_unique<StdoutSink>(color_mode, output);
  return Initialize(std::move(options), std::move(sink));
}

bool Logger::IsInitialized() noexcept { return GetImpl() != nullptr; }

bool Logger::IsEnabled(LogLevel level) noexcept {
  const Impl* impl = GetImpl();
  return impl != nullptr && impl->IsEnabled(level);
}

bool Logger::Log(LogLevel level, std::string_view message,
                 std::source_location source) {
  Impl* impl = GetImpl();
  return impl != nullptr && impl->Log(level, message, source);
}

bool Logger::LogFormatted(LogLevel level, std::string message,
                          std::source_location source) {
  Impl* impl = GetImpl();
  return impl != nullptr && impl->LogOwned(level, std::move(message), source);
}

bool Logger::Flush() {
  Impl* impl = GetImpl();
  return impl != nullptr && impl->Flush();
}

std::size_t Logger::queued_count() noexcept {
  Impl* impl = GetImpl();
  return impl != nullptr ? impl->queued_count() : 0;
}

std::uint64_t Logger::dropped_count() noexcept {
  Impl* impl = GetImpl();
  return impl != nullptr ? impl->dropped_count() : 0;
}

}  // namespace netlib::log
