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
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "blockingconcurrentqueue.h"

namespace netlib::log {
namespace {

constexpr std::size_t kWriterBatchSize = 64;

enum class WorkItemKind : std::uint8_t {
  kMessage,
  kFlush,
  kStop,
};

struct WorkItem {
  WorkItemKind kind = WorkItemKind::kMessage;
  LogRecord record;
  std::uint64_t flush_id = 0;
};

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

  if (color) {
    prefix.append("\x1b[2m");
  }
  prefix.append(FormatTimestamp(record.timestamp));
  if (color) {
    prefix.append("\x1b[0m");
  }

  prefix.append(" | ");
  if (color) {
    prefix.append(LevelColor(record.level));
  }
  prefix.append(LevelName(record.level));
  if (color) {
    prefix.append("\x1b[0m");
  }

  prefix.append(" | seq=");
  AppendUnsigned(prefix, record.sequence);
  prefix.append(" | tid=");
  AppendUnsigned(prefix, record.thread_id);
  if (record.dropped_messages != 0) {
    prefix.append(" | drops=");
    AppendUnsigned(prefix, record.dropped_messages);
  }
  prefix.append(" | ");
  prefix.append(record.logger_name);
  prefix.append(" | ");
  prefix.append(BaseName(record.source.file_name()));
  prefix.push_back(':');
  AppendUnsigned(prefix, record.source.line());
  if (record.source.function_name()[0] != '\0') {
    prefix.push_back(' ');
    prefix.append(record.source.function_name());
  }
  prefix.append(" | ");
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
        std::fwrite("\n    | ", 1, 7, output_);
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
  Impl(LoggerOptions options, std::unique_ptr<LogSink> sink);
  ~Impl();

  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;
  Impl(Impl&&) = delete;
  Impl& operator=(Impl&&) = delete;

  bool IsEnabled(LogLevel level) const;
  bool Log(LogLevel level, std::string_view message,
           std::source_location source);
  bool LogOwned(LogLevel level, std::string message,
                std::source_location source);
  bool Flush();
  void Stop();
  std::size_t queued_count() const;
  std::uint64_t dropped_count() const;

 private:
  bool ReserveQueueSlot();
  void WriterLoop();

  std::string logger_name_;
  std::atomic<LogLevel> minimum_level_;
  const std::size_t queue_capacity_;
  const bool flush_each_record_;
  moodycamel::BlockingConcurrentQueue<WorkItem> queue_;
  moodycamel::ProducerToken control_token_;
  std::unique_ptr<LogSink> sink_;
  moodycamel::ConsumerToken consumer_token_;
  std::atomic<std::size_t> queued_count_{0};
  std::atomic<std::uint64_t> dropped_count_{0};
  std::atomic<std::uint64_t> unreported_drop_count_{0};
  std::atomic<std::uint64_t> next_sequence_{0};
  std::atomic<bool> stopping_{false};
  std::uint64_t next_flush_id_ = 1;
  std::uint64_t completed_flush_id_ = 0;
  std::mutex lifecycle_mutex_;
  std::mutex flush_mutex_;
  std::condition_variable flush_condition_;
  std::thread writer_;
};

Logger::Impl::Impl(LoggerOptions options, std::unique_ptr<LogSink> sink)
    : logger_name_(std::move(options.name)),
      minimum_level_(options.minimum_level),
      queue_capacity_(options.queue_capacity),
      flush_each_record_(options.flush_each_record),
      queue_(options.queue_capacity),
      control_token_(queue_),
      sink_(std::move(sink)),
      consumer_token_(queue_) {
  writer_ = std::thread([this] { WriterLoop(); });
}

Logger::Impl::~Impl() { Stop(); }

bool Logger::Impl::IsEnabled(LogLevel level) const {
  return IsValidLevel(level) &&
         level >= minimum_level_.load(std::memory_order_relaxed);
}

bool Logger::Impl::Log(LogLevel level, std::string_view message,
                       std::source_location source) {
  return LogOwned(level, std::string(message), source);
}

bool Logger::Impl::LogOwned(LogLevel level, std::string message,
                            std::source_location source) {
  if (!IsEnabled(level) || stopping_.load(std::memory_order_acquire) ||
      !ReserveQueueSlot()) {
    return false;
  }

  std::uint64_t dropped_messages =
      unreported_drop_count_.exchange(0, std::memory_order_acq_rel);
  LogRecord record{
      .timestamp = std::chrono::system_clock::now(),
      .level = level,
      .sequence = next_sequence_.fetch_add(1, std::memory_order_relaxed),
      .thread_id = CurrentThreadId(),
      .dropped_messages = dropped_messages,
      .source = source,
      .logger_name = logger_name_,
      .message = std::move(message),
  };
  WorkItem item{
      .kind = WorkItemKind::kMessage,
      .record = std::move(record),
      .flush_id = 0,
  };
  if (!queue_.enqueue(std::move(item))) {
    queued_count_.fetch_sub(1, std::memory_order_release);
    return false;
  }
  return true;
}

bool Logger::Impl::Flush() {
  std::uint64_t flush_id = 0;
  {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (stopping_.load(std::memory_order_acquire)) {
      return false;
    }

    flush_id = next_flush_id_++;
    if (!queue_.enqueue(control_token_, WorkItem{
                                            .kind = WorkItemKind::kFlush,
                                            .record = LogRecord{},
                                            .flush_id = flush_id,
                                        })) {
      return false;
    }
  }

  std::unique_lock<std::mutex> lock(flush_mutex_);
  flush_condition_.wait(
      lock, [this, flush_id] { return completed_flush_id_ >= flush_id; });
  return true;
}

void Logger::Impl::Stop() {
  {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (stopping_.exchange(true, std::memory_order_acq_rel)) {
      return;
    }
    queue_.enqueue(control_token_, WorkItem{
                                       .kind = WorkItemKind::kStop,
                                       .record = LogRecord{},
                                       .flush_id = 0,
                                   });
  }
  if (writer_.joinable()) {
    writer_.join();
  }
}

std::size_t Logger::Impl::queued_count() const {
  return queued_count_.load(std::memory_order_acquire);
}

std::uint64_t Logger::Impl::dropped_count() const {
  return dropped_count_.load(std::memory_order_acquire);
}

bool Logger::Impl::ReserveQueueSlot() {
  std::size_t current = queued_count_.load(std::memory_order_relaxed);
  while (current < queue_capacity_) {
    if (queued_count_.compare_exchange_weak(current, current + 1,
                                            std::memory_order_acq_rel,
                                            std::memory_order_relaxed)) {
      return true;
    }
  }

  dropped_count_.fetch_add(1, std::memory_order_relaxed);
  unreported_drop_count_.fetch_add(1, std::memory_order_relaxed);
  return false;
}

void Logger::Impl::WriterLoop() {
  std::array<WorkItem, kWriterBatchSize> items{};
  while (true) {
    std::size_t count =
        queue_.wait_dequeue_bulk(consumer_token_, items.data(), items.size());
    for (std::size_t i = 0; i < count; ++i) {
      WorkItem& item = items[i];
      if (item.kind == WorkItemKind::kStop) {
        sink_->Flush();
        return;
      }
      if (item.kind == WorkItemKind::kFlush) {
        sink_->Flush();
        {
          std::lock_guard<std::mutex> lock(flush_mutex_);
          completed_flush_id_ = item.flush_id;
        }
        flush_condition_.notify_all();
        continue;
      }

      queued_count_.fetch_sub(1, std::memory_order_acq_rel);
      sink_->Write(item.record);
      if (flush_each_record_ || item.record.level >= LogLevel::kWarn) {
        sink_->Flush();
      }
    }
  }
}

std::unique_ptr<Logger> Logger::Create(LoggerOptions options,
                                       std::unique_ptr<LogSink> sink) {
  if (options.name.empty() || options.queue_capacity == 0 || !sink ||
      !IsValidLevel(options.minimum_level)) {
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
