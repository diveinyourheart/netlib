#include "netlib/sub_reactor.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "concurrentqueue.h"
#include "netlib/channel.h"
#include "netlib/connection_handle.h"
#include "netlib/event_loop.h"
#include "netlib/frame_message.h"
#include "netlib/log/logging.h"
#include "netlib/protocol_codec.h"
#include "netlib/thread_pool.h"
#include "netlib/unique_fd.h"

namespace netlib {
namespace {

constexpr std::size_t kInitialReadBufferSize = 4 * 1024;

constinit std::atomic<std::uint64_t> next_connection_id{1};

std::uint64_t AllocateConnectionId() {
  return next_connection_id.fetch_add(1, std::memory_order_relaxed);
}

}  // namespace

enum class ResponseMessageKind : std::uint8_t {
  kApplication,
  kTimeout,
};

struct ResponseMessage {
  ResponseMessageKind kind = ResponseMessageKind::kApplication;
  std::uint64_t connection_id = 0;
  std::uint64_t sequence = 0;
  ApplicationResponse response = BinaryResponse{};
  bool close_after_write = false;
};

class SubReactor::ResponseQueue {
 public:
  ResponseQueue(std::size_t capacity, std::shared_ptr<platform::Wakeup> wake_fd)
      : capacity_(capacity),
        wake_fd_(std::move(wake_fd)),
        queue_(capacity),
        consumer_token_(queue_) {}

  bool Enqueue(ResponseMessage message) {
    return EnqueueMessage(std::move(message), true);
  }

  bool EnqueueTimeout(std::uint64_t connection_id, std::uint64_t sequence) {
    // Timeout controls must reach the owner thread even when application
    // responses have filled the bounded queue. Their count is bounded by each
    // connection's in-flight request limit.
    return EnqueueMessage(
        ResponseMessage{
            .kind = ResponseMessageKind::kTimeout,
            .connection_id = connection_id,
            .sequence = sequence,
        },
        false);
  }

  bool TryDequeue(ResponseMessage& message) {
    if (!queue_.try_dequeue(consumer_token_, message)) {
      return false;
    }
    queued_count_.fetch_sub(1, std::memory_order_acq_rel);
    return true;
  }

  void Shutdown() { accepting_.store(false, std::memory_order_release); }

 private:
  bool EnqueueMessage(ResponseMessage message, bool enforce_capacity) {
    if (!accepting_.load(std::memory_order_acquire)) {
      return false;
    }
    if (enforce_capacity) {
      if (!ReserveQueueSlot()) {
        return false;
      }
    } else {
      queued_count_.fetch_add(1, std::memory_order_acq_rel);
    }
    if (!accepting_.load(std::memory_order_acquire)) {
      queued_count_.fetch_sub(1, std::memory_order_release);
      return false;
    }
    if (!queue_.enqueue(std::move(message))) {
      queued_count_.fetch_sub(1, std::memory_order_release);
      return false;
    }
    return Wake();
  }

  bool ReserveQueueSlot() {
    std::size_t current = queued_count_.load(std::memory_order_relaxed);
    while (current < capacity_) {
      if (queued_count_.compare_exchange_weak(current, current + 1,
                                              std::memory_order_acq_rel,
                                              std::memory_order_relaxed)) {
        return true;
      }
    }
    return false;
  }

  bool Wake() { return wake_fd_ != nullptr && wake_fd_->Notify(); }

  const std::size_t capacity_;
  const std::shared_ptr<platform::Wakeup> wake_fd_;
  moodycamel::ConcurrentQueue<ResponseMessage> queue_;
  moodycamel::ConsumerToken consumer_token_;
  std::atomic<std::size_t> queued_count_{0};
  std::atomic<bool> accepting_{true};
};

class SubReactor::Connection {
 public:
  enum class ResponseStatus : std::uint8_t {
    kQueued,
    kStale,
    kClosed,
    kOverflow,
  };

  Connection(UniqueFd socket, std::unique_ptr<Channel> channel,
             std::unique_ptr<ProtocolCodec> codec, ConnectionHandle handle,
             std::shared_ptr<std::atomic<bool>> alive,
             std::size_t max_output_buffer_size,
             std::size_t max_pending_responses);

  SocketHandle fd() const;
  std::uint64_t connection_id() const;
  const ConnectionHandle& handle() const;
  ProtocolCodec& codec();
  std::uint64_t NextSequence();
  bool HasRequestTimerCapacity() const;
  bool TrackRequestTimer(std::uint64_t sequence, TimerScheduler::TimerId id);
  TimerScheduler::TimerId TakeRequestTimer(std::uint64_t sequence);
  std::vector<TimerScheduler::TimerId> TakeRequestTimers();

  std::span<char> WritableBytes();
  void CommitWrite(std::size_t size);
  std::span<const char> ReadableBytes() const;
  void Consume(std::size_t size);

  bool AppendResponse(const ApplicationResponse& response,
                      bool close_after_write);
  ResponseStatus QueueResponse(std::uint64_t sequence,
                               ApplicationResponse response,
                               bool close_after_write);
  bool HasOutput() const;
  std::span<const char> PendingOutput() const;
  void ConsumeOutput(std::size_t size);
  bool close_after_write() const;
  bool write_interest_enabled() const;
  void set_write_interest_enabled(bool enabled);
  bool SetInterest(ChannelInterest interest);
  void MarkClosed();
  void SetDecodeTarget(std::size_t target_size);
  void ClearDecodeTarget();

 private:
  struct PendingResponse {
    ApplicationResponse response = BinaryResponse{};
    bool close_after_write = false;
  };

  void MakeWritable(std::size_t min_bytes);
  void CompactOutput();

  UniqueFd socket_;
  std::unique_ptr<Channel> channel_;
  std::unique_ptr<ProtocolCodec> codec_;
  const ConnectionHandle handle_;
  const std::uint64_t connection_id_;
  const std::shared_ptr<std::atomic<bool>> alive_;
  std::vector<char> storage_;
  std::size_t read_offset_ = 0;
  std::size_t write_offset_ = 0;
  const std::size_t max_read_buffer_size_;
  const std::size_t max_output_buffer_size_;
  std::size_t decode_target_size_ = 0;
  std::uint64_t next_request_sequence_ = 0;
  std::vector<char> output_buffer_;
  std::size_t output_offset_ = 0;
  const std::size_t max_pending_responses_;
  std::uint64_t next_response_sequence_ = 0;
  std::map<std::uint64_t, PendingResponse> pending_responses_;
  std::map<std::uint64_t, TimerScheduler::TimerId> request_timers_;
  bool close_after_write_ = false;
  bool write_interest_enabled_ = false;
};

SubReactor::Connection::Connection(UniqueFd socket,
                                   std::unique_ptr<Channel> channel,
                                   std::unique_ptr<ProtocolCodec> codec,
                                   ConnectionHandle handle,
                                   std::shared_ptr<std::atomic<bool>> alive,
                                   std::size_t max_output_buffer_size,
                                   std::size_t max_pending_responses)
    : socket_(std::move(socket)),
      channel_(std::move(channel)),
      codec_(std::move(codec)),
      handle_(std::move(handle)),
      connection_id_(handle_.id()),
      alive_(std::move(alive)),
      max_read_buffer_size_(codec_->MaxDecodeBufferSize()),
      max_output_buffer_size_(max_output_buffer_size),
      max_pending_responses_(max_pending_responses) {}

SocketHandle SubReactor::Connection::fd() const { return socket_.get(); }

std::uint64_t SubReactor::Connection::connection_id() const {
  return connection_id_;
}

const ConnectionHandle& SubReactor::Connection::handle() const {
  return handle_;
}

ProtocolCodec& SubReactor::Connection::codec() { return *codec_; }

std::uint64_t SubReactor::Connection::NextSequence() {
  return next_request_sequence_++;
}

bool SubReactor::Connection::HasRequestTimerCapacity() const {
  return request_timers_.size() < max_pending_responses_;
}

bool SubReactor::Connection::TrackRequestTimer(std::uint64_t sequence,
                                               TimerScheduler::TimerId id) {
  return request_timers_.emplace(sequence, id).second;
}

TimerScheduler::TimerId SubReactor::Connection::TakeRequestTimer(
    std::uint64_t sequence) {
  auto timer = request_timers_.find(sequence);
  if (timer == request_timers_.end()) {
    return TimerScheduler::kInvalidTimer;
  }
  TimerScheduler::TimerId id = timer->second;
  request_timers_.erase(timer);
  return id;
}

std::vector<TimerScheduler::TimerId>
SubReactor::Connection::TakeRequestTimers() {
  std::vector<TimerScheduler::TimerId> ids;
  ids.reserve(request_timers_.size());
  for (const auto& [_, id] : request_timers_) {
    ids.push_back(id);
  }
  request_timers_.clear();
  return ids;
}

std::span<char> SubReactor::Connection::WritableBytes() {
  std::size_t readable_size = write_offset_ - read_offset_;
  if (readable_size >= max_read_buffer_size_) {
    return {};
  }
  std::size_t writable_size =
      std::min(kInitialReadBufferSize, max_read_buffer_size_ - readable_size);
  if (decode_target_size_ > readable_size) {
    writable_size =
        std::min(writable_size, decode_target_size_ - readable_size);
  }
  assert(writable_size != 0);
  MakeWritable(writable_size);
  return std::span<char>(storage_.data() + write_offset_,
                         storage_.size() - write_offset_);
}

void SubReactor::Connection::CommitWrite(std::size_t size) {
  assert(size <= storage_.size() - write_offset_);
  write_offset_ += size;
}

std::span<const char> SubReactor::Connection::ReadableBytes() const {
  return std::span<const char>(storage_.data() + read_offset_,
                               write_offset_ - read_offset_);
}

void SubReactor::Connection::Consume(std::size_t size) {
  assert(size <= write_offset_ - read_offset_);
  read_offset_ += size;
  if (read_offset_ == write_offset_) {
    read_offset_ = 0;
    write_offset_ = 0;
  }
}

void SubReactor::Connection::MakeWritable(std::size_t min_bytes) {
  if (storage_.size() - write_offset_ >= min_bytes) {
    return;
  }

  std::size_t readable_size = write_offset_ - read_offset_;
  if (read_offset_ != 0) {
    if (readable_size != 0) {
      std::memmove(storage_.data(), storage_.data() + read_offset_,
                   readable_size);
    }
    read_offset_ = 0;
    write_offset_ = readable_size;
  }
  if (storage_.size() - write_offset_ >= min_bytes) {
    return;
  }

  std::size_t required_size = write_offset_ + min_bytes;
  assert(required_size <= max_read_buffer_size_);
  std::size_t grown_size =
      std::max(kInitialReadBufferSize,
               std::min(max_read_buffer_size_, storage_.size() * 2));
  storage_.resize(std::max(required_size, grown_size));
}

bool SubReactor::Connection::AppendResponse(const ApplicationResponse& response,
                                            bool close_after_write) {
  CompactOutput();
  std::size_t old_size = output_buffer_.size();
  if (!codec_->Encode(response, &output_buffer_)) {
    return false;
  }
  if (output_buffer_.size() > max_output_buffer_size_) {
    output_buffer_.resize(old_size);
    return false;
  }
  close_after_write_ = close_after_write_ || close_after_write ||
                       codec_->ShouldCloseAfterWrite(response);
  return true;
}

SubReactor::Connection::ResponseStatus SubReactor::Connection::QueueResponse(
    std::uint64_t sequence, ApplicationResponse response,
    bool close_after_write) {
  if (sequence < next_response_sequence_) {
    return ResponseStatus::kStale;
  }
  if (sequence >= next_request_sequence_) {
    return ResponseStatus::kStale;
  }
  if (close_after_write_) {
    return ResponseStatus::kClosed;
  }
  if (sequence == next_response_sequence_) {
    if (!AppendResponse(response, close_after_write)) {
      return ResponseStatus::kOverflow;
    }
    ++next_response_sequence_;
    if (close_after_write_) {
      pending_responses_.clear();
      return ResponseStatus::kQueued;
    }
  } else {
    if (pending_responses_.find(sequence) != pending_responses_.end()) {
      return ResponseStatus::kStale;
    }
    if (pending_responses_.size() >= max_pending_responses_) {
      return ResponseStatus::kOverflow;
    }
    pending_responses_.emplace(sequence,
                               PendingResponse{
                                   .response = std::move(response),
                                   .close_after_write = close_after_write,
                               });
  }

  while (true) {
    auto current = pending_responses_.find(next_response_sequence_);
    if (current == pending_responses_.end()) {
      return ResponseStatus::kQueued;
    }
    PendingResponse pending = std::move(current->second);
    pending_responses_.erase(current);
    if (!AppendResponse(pending.response, pending.close_after_write)) {
      return ResponseStatus::kOverflow;
    }
    ++next_response_sequence_;
    if (close_after_write_) {
      pending_responses_.clear();
      return ResponseStatus::kQueued;
    }
  }
}

bool SubReactor::Connection::HasOutput() const {
  return output_offset_ < output_buffer_.size();
}

std::span<const char> SubReactor::Connection::PendingOutput() const {
  return std::span<const char>(output_buffer_.data() + output_offset_,
                               output_buffer_.size() - output_offset_);
}

void SubReactor::Connection::ConsumeOutput(std::size_t size) {
  assert(size <= output_buffer_.size() - output_offset_);
  output_offset_ += size;
  if (output_offset_ == output_buffer_.size()) {
    output_buffer_.clear();
    output_offset_ = 0;
  }
}

bool SubReactor::Connection::close_after_write() const {
  return close_after_write_;
}

bool SubReactor::Connection::write_interest_enabled() const {
  return write_interest_enabled_;
}

void SubReactor::Connection::set_write_interest_enabled(bool enabled) {
  write_interest_enabled_ = enabled;
}

bool SubReactor::Connection::SetInterest(ChannelInterest interest) {
  return channel_->SetInterest(interest);
}

void SubReactor::Connection::MarkClosed() {
  alive_->store(false, std::memory_order_release);
}

void SubReactor::Connection::SetDecodeTarget(std::size_t target_size) {
  decode_target_size_ = target_size;
}

void SubReactor::Connection::ClearDecodeTarget() { decode_target_size_ = 0; }

void SubReactor::Connection::CompactOutput() {
  if (output_offset_ == 0) {
    return;
  }
  std::size_t remaining = output_buffer_.size() - output_offset_;
  if (remaining != 0) {
    std::memmove(output_buffer_.data(), output_buffer_.data() + output_offset_,
                 remaining);
  }
  output_buffer_.resize(remaining);
  output_offset_ = 0;
}

std::unique_ptr<SubReactor> SubReactor::Create(
    std::size_t max_pending_connections, ThreadPool& worker_pool,
    ConnectionCallbacks callbacks, log::Logger* logger) {
  SubReactorOptions options;
  options.max_pending_connections = max_pending_connections;
  options.max_pending_responses = max_pending_connections;
  return Create(options, worker_pool, std::move(callbacks), logger);
}

std::unique_ptr<SubReactor> SubReactor::Create(
    std::size_t max_pending_connections, ThreadPool& worker_pool,
    ConnectionCallbacks callbacks, ProtocolCodecFactory codec_factory,
    log::Logger* logger) {
  SubReactorOptions options;
  options.max_pending_connections = max_pending_connections;
  options.max_pending_responses = max_pending_connections;
  return Create(options, worker_pool, std::move(callbacks),
                std::move(codec_factory), logger);
}

std::unique_ptr<SubReactor> SubReactor::Create(SubReactorOptions options,
                                               ThreadPool& worker_pool,
                                               ConnectionCallbacks callbacks,
                                               log::Logger* logger) {
  return Create(
      options, worker_pool, std::move(callbacks),
      [] { return CreateLengthPrefixedCodec(); }, logger);
}

std::unique_ptr<SubReactor> SubReactor::Create(
    SubReactorOptions options, ThreadPool& worker_pool,
    ConnectionCallbacks callbacks, ProtocolCodecFactory codec_factory,
    log::Logger* logger) {
  if (options.max_pending_connections == 0 ||
      options.max_pending_responses == 0 ||
      options.max_pending_output_bytes == 0 ||
      options.request_timeout <= std::chrono::milliseconds::zero()) {
    if (logger != nullptr) {
      NETLIB_LOG_ERROR(*logger, "SubReactor option is outside its valid range");
    }
    return nullptr;
  }
  if (!codec_factory) {
    if (logger != nullptr) {
      NETLIB_LOG_ERROR(*logger, "SubReactor requires a ProtocolCodec factory");
    }
    return nullptr;
  }
  std::unique_ptr<ProtocolCodec> codec_probe = codec_factory();
  if (!codec_probe || codec_probe->MaxDecodeBufferSize() == 0) {
    if (logger != nullptr) {
      NETLIB_LOG_ERROR(
          *logger, "SubReactor ProtocolCodec factory returned invalid codec");
    }
    return nullptr;
  }

  std::unique_ptr<EventLoop> event_loop = EventLoop::Create(logger);
  if (!event_loop) {
    return nullptr;
  }

  std::shared_ptr<platform::Wakeup> wake_fd = platform::Wakeup::Create();
  if (!wake_fd) {
    if (logger != nullptr) {
      NETLIB_LOG_ERROR(*logger, "SubReactor notification failed: {}",
                       platform::ErrorMessage());
    }
    return nullptr;
  }

  auto reactor = std::unique_ptr<SubReactor>(new SubReactor(
      std::move(event_loop), std::move(wake_fd), options, worker_pool,
      std::move(codec_factory), std::move(callbacks), logger));
  SubReactor* reactor_pointer = reactor.get();
  ChannelCallbacks wake_callbacks{
      [reactor_pointer] { reactor_pointer->HandleWake(); },
      {},
      [reactor_pointer] { reactor_pointer->Stop(); },
  };
  reactor->wake_channel_ =
      Channel::Create(*reactor->event_loop_, reactor->wake_fd_->handle(),
                      ChannelInterest{.read = true}, std::move(wake_callbacks));
  if (!reactor->wake_channel_) {
    return nullptr;
  }
  if (logger != nullptr) {
    NETLIB_LOG_INFO(
        *logger,
        "SubReactor created with connection capacity={}, "
        "response capacity={}, output bytes={}, timeout_ms={}",
        options.max_pending_connections, options.max_pending_responses,
        options.max_pending_output_bytes, options.request_timeout.count());
  }
  return reactor;
}

SubReactor::SubReactor(std::unique_ptr<EventLoop> event_loop,
                       std::shared_ptr<platform::Wakeup> wake_fd,
                       SubReactorOptions options, ThreadPool& worker_pool,
                       ProtocolCodecFactory codec_factory,
                       ConnectionCallbacks callbacks, log::Logger* logger)
    : event_loop_(std::move(event_loop)),
      worker_pool_(worker_pool),
      codec_factory_(std::move(codec_factory)),
      callbacks_(std::move(callbacks)),
      wake_fd_(std::move(wake_fd)),
      response_queue_(std::make_shared<ResponseQueue>(
          options.max_pending_responses, wake_fd_)),
      options_(options),
      logger_(logger) {}

SubReactor::~SubReactor() {
  request_timer_.Stop();
  response_queue_->Shutdown();
}

bool SubReactor::AdoptConnection(UniqueFd& connection) {
  if (!connection) {
    return false;
  }

  std::lock_guard<std::mutex> lock(pending_mutex_);
  if (stopping_.load() ||
      pending_connections_.size() >= options_.max_pending_connections) {
    return false;
  }
  pending_connections_.push_back(std::move(connection));
  if (!Wake()) {
    connection = std::move(pending_connections_.back());
    pending_connections_.pop_back();
    return false;
  }
  return true;
}

void SubReactor::Run() { event_loop_->Run(); }

void SubReactor::Stop() {
  if (logger_ != nullptr) {
    NETLIB_LOG_INFO(*logger_, "SubReactor stopping");
  }
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    stopping_.store(true);
  }
  request_timer_.Stop();
  response_queue_->Shutdown();
  event_loop_->Stop();
}

void SubReactor::HandleWake() {
  if (!DrainWake()) {
    if (logger_ != nullptr) {
      NETLIB_LOG_ERROR(*logger_, "SubReactor wake notification drain failed");
    }
    Stop();
    return;
  }

  std::deque<UniqueFd> pending;
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending.swap(pending_connections_);
  }

  while (!pending.empty() && !stopping_.load()) {
    RegisterConnection(std::move(pending.front()));
    pending.pop_front();
  }

  ResponseMessage response;
  while (!stopping_.load() && response_queue_->TryDequeue(response)) {
    if (response.kind == ResponseMessageKind::kTimeout) {
      HandleRequestTimeout(response.connection_id, response.sequence);
      continue;
    }
    HandleApplicationResponse(response.connection_id, response.sequence,
                              std::move(response.response),
                              response.close_after_write);
  }
}

void SubReactor::HandleApplicationResponse(std::uint64_t connection_id,
                                           std::uint64_t sequence,
                                           ApplicationResponse response,
                                           bool close_after_write) {
  auto connection = connections_.find(connection_id);
  if (connection == connections_.end()) {
    return;
  }

  Connection& state = *connection->second;
  TimerScheduler::TimerId timer_id = state.TakeRequestTimer(sequence);
  Connection::ResponseStatus status =
      state.QueueResponse(sequence, std::move(response), close_after_write);
  if (timer_id != TimerScheduler::kInvalidTimer) {
    request_timer_.Cancel(timer_id);
  }
  if (status == Connection::ResponseStatus::kQueued) {
    FlushConnection(connection_id);
    return;
  }
  if (status == Connection::ResponseStatus::kOverflow) {
    if (logger_ != nullptr) {
      NETLIB_LOG_WARN(
          *logger_,
          "response buffer limit reached for connection id={} sequence={}",
          connection_id, sequence);
    }
    CloseConnection(connection_id);
    return;
  }
  if (logger_ != nullptr) {
    NETLIB_LOG_DEBUG(
        *logger_,
        "ignored response for closed or stale connection id={} sequence={}",
        connection_id, sequence);
  }
}

void SubReactor::HandleRequestTimeout(std::uint64_t connection_id,
                                      std::uint64_t sequence) {
  auto connection = connections_.find(connection_id);
  if (connection == connections_.end()) {
    return;
  }

  Connection& state = *connection->second;
  TimerScheduler::TimerId timer_id = state.TakeRequestTimer(sequence);
  if (timer_id == TimerScheduler::kInvalidTimer) {
    return;
  }
  request_timer_.Cancel(timer_id);

  std::optional<ApplicationResponse> timeout_response =
      state.codec().CreateTimeoutResponse();
  if (!timeout_response.has_value()) {
    if (logger_ != nullptr) {
      NETLIB_LOG_WARN(*logger_,
                      "request timed out without a protocol error response for "
                      "connection id={} sequence={}",
                      connection_id, sequence);
    }
    CloseConnection(connection_id);
    return;
  }

  Connection::ResponseStatus status =
      state.QueueResponse(sequence, std::move(*timeout_response), true);
  if (status == Connection::ResponseStatus::kQueued) {
    if (logger_ != nullptr) {
      NETLIB_LOG_WARN(*logger_,
                      "request timeout response queued for connection id={} "
                      "sequence={}",
                      connection_id, sequence);
    }
    FlushConnection(connection_id);
    return;
  }
  if (status == Connection::ResponseStatus::kOverflow) {
    if (logger_ != nullptr) {
      NETLIB_LOG_WARN(
          *logger_,
          "failed to encode request timeout for connection id={} sequence={}",
          connection_id, sequence);
    }
    CloseConnection(connection_id);
    return;
  }
  if (logger_ != nullptr) {
    NETLIB_LOG_DEBUG(
        *logger_,
        "request timeout became stale for connection id={} sequence={}",
        connection_id, sequence);
  }
}

bool SubReactor::DrainWake() { return wake_fd_->Drain(); }

bool SubReactor::Wake() {
  if (wake_fd_ != nullptr && wake_fd_->Notify()) {
    return true;
  }
  if (logger_ != nullptr) {
    NETLIB_LOG_ERROR(*logger_, "SubReactor notification write failed");
  }
  return false;
}

void SubReactor::RegisterConnection(UniqueFd connection) {
  SocketHandle fd = connection.get();
  if (!platform::SetNonBlocking(fd)) {
    if (logger_ != nullptr) {
      NETLIB_LOG_ERROR(*logger_, "failed to set nonblocking mode for fd={}: {}",
                       fd, platform::ErrorMessage());
    }
    return;
  }
  if (!platform::SetCloseOnExec(fd)) {
    if (logger_ != nullptr) {
      NETLIB_LOG_ERROR(*logger_,
                       "failed to set non-inheritable mode for fd={}: {}", fd,
                       platform::ErrorMessage());
    }
    return;
  }

  std::unique_ptr<ProtocolCodec> codec = codec_factory_();
  if (!codec || codec->MaxDecodeBufferSize() == 0) {
    if (logger_ != nullptr) {
      NETLIB_LOG_ERROR(*logger_, "ProtocolCodec factory failed for fd={}", fd);
    }
    return;
  }

  std::uint64_t connection_id = AllocateConnectionId();
  std::shared_ptr<std::atomic<bool>> alive =
      std::make_shared<std::atomic<bool>>(true);
  std::weak_ptr<ResponseQueue> weak_response_queue = response_queue_;
  ConnectionHandle handle(
      connection_id, alive,
      [weak_response_queue](std::uint64_t id, std::uint64_t sequence,
                            ApplicationResponse response,
                            bool close_after_write) {
        std::shared_ptr<ResponseQueue> response_queue =
            weak_response_queue.lock();
        if (!response_queue) {
          return false;
        }
        return response_queue->Enqueue(ResponseMessage{
            .connection_id = id,
            .sequence = sequence,
            .response = std::move(response),
            .close_after_write = close_after_write,
        });
      });

  ChannelCallbacks channel_callbacks{
      [this, connection_id] { HandleReadable(connection_id); },
      [this, connection_id] { HandleWritable(connection_id); },
      [this, connection_id] { CloseConnection(connection_id); },
  };
  std::unique_ptr<Channel> channel =
      Channel::Create(*event_loop_, fd, ChannelInterest{.read = true},
                      std::move(channel_callbacks));
  if (!channel) {
    return;
  }

  std::unique_ptr<Connection> state = std::make_unique<Connection>(
      std::move(connection), std::move(channel), std::move(codec),
      std::move(handle), std::move(alive), options_.max_pending_output_bytes,
      options_.max_pending_responses);
  auto inserted = connections_.emplace(connection_id, std::move(state));
  if (!inserted.second) {
    if (logger_ != nullptr) {
      NETLIB_LOG_ERROR(*logger_, "duplicate connection state for id={}",
                       connection_id);
    }
    return;
  }
  if (logger_ != nullptr) {
    NETLIB_LOG_DEBUG(*logger_, "registered connection fd={} id={}", fd,
                     connection_id);
  }
  if (callbacks_.on_connected) {
    callbacks_.on_connected(fd);
  }
}

void SubReactor::HandleReadable(std::uint64_t connection_id) {
  auto connection = connections_.find(connection_id);
  if (connection == connections_.end()) {
    return;
  }

  Connection& state = *connection->second;
  if (state.close_after_write()) {
    return;
  }
  SocketHandle fd = state.fd();
  while (true) {
    std::span<char> writable = state.WritableBytes();
    if (writable.empty()) {
      if (logger_ != nullptr) {
        NETLIB_LOG_WARN(*logger_,
                        "receive buffer limit reached for connection id={}",
                        connection_id);
      }
      CloseConnection(connection_id);
      return;
    }
    std::ptrdiff_t result =
        platform::Receive(fd, writable.data(), writable.size());
    if (result > 0) {
      state.CommitWrite(static_cast<std::size_t>(result));
      if (!ProcessFrames(state)) {
        CloseConnection(connection_id);
        return;
      }
      continue;
    }
    if (result == 0) {
      if (logger_ != nullptr) {
        NETLIB_LOG_DEBUG(*logger_, "peer closed fd={}", fd);
      }
      CloseConnection(connection_id);
      return;
    }
    if (platform::Interrupted(platform::LastError())) {
      continue;
    }
    if (platform::WouldBlock(platform::LastError())) {
      return;
    }
    if (logger_ != nullptr) {
      NETLIB_LOG_WARN(*logger_, "recv failed for fd={}: {}", fd,
                      platform::ErrorMessage());
    }
    CloseConnection(connection_id);
    return;
  }
}

bool SubReactor::ProcessFrames(Connection& connection) {
  while (true) {
    std::span<const char> readable = connection.ReadableBytes();
    DecodeResult decode_result = connection.codec().Decode(readable);
    if (decode_result.status == DecodeStatus::kNeedMore) {
      if (decode_result.required_bytes >
              connection.codec().MaxDecodeBufferSize() ||
          (decode_result.required_bytes != 0 &&
           decode_result.required_bytes <= readable.size())) {
        if (logger_ != nullptr) {
          NETLIB_LOG_WARN(*logger_,
                          "invalid decode target for connection id={}",
                          connection.connection_id());
        }
        return false;
      }
      connection.SetDecodeTarget(decode_result.required_bytes);
      return true;
    }
    if (decode_result.status == DecodeStatus::kProtocolError ||
        decode_result.consumed_bytes == 0 ||
        decode_result.consumed_bytes > readable.size() ||
        decode_result.payload_offset > decode_result.consumed_bytes ||
        decode_result.payload_size >
            decode_result.consumed_bytes - decode_result.payload_offset) {
      if (logger_ != nullptr) {
        NETLIB_LOG_WARN(*logger_, "protocol decode error for connection id={}",
                        connection.connection_id());
      }
      return false;
    }

    connection.ClearDecodeTarget();
    if (!connection.HasRequestTimerCapacity()) {
      if (logger_ != nullptr) {
        NETLIB_LOG_WARN(*logger_,
                        "in-flight request limit reached for connection id={}",
                        connection.connection_id());
      }
      return false;
    }
    std::uint64_t connection_id = connection.connection_id();
    std::uint64_t sequence = connection.NextSequence();
    std::span<const char> payload = readable.subspan(
        decode_result.payload_offset, decode_result.payload_size);
    FrameMessage message{
        .connection_id = connection_id,
        .sequence = sequence,
        .connection = connection.handle(),
        .metadata = std::move(decode_result.metadata),
        .payload = std::vector<char>(payload.begin(), payload.end()),
    };
    if (!worker_pool_.Submit(std::move(message))) {
      if (logger_ != nullptr) {
        NETLIB_LOG_WARN(*logger_,
                        "worker queue full for connection id={} sequence={}",
                        connection_id, sequence);
      }
      return false;
    }

    std::weak_ptr<ResponseQueue> weak_response_queue = response_queue_;
    TimerScheduler::TimerId timer_id = request_timer_.ScheduleAfter(
        options_.request_timeout,
        [weak_response_queue, connection_id, sequence] {
          std::shared_ptr<ResponseQueue> response_queue =
              weak_response_queue.lock();
          if (response_queue != nullptr) {
            response_queue->EnqueueTimeout(connection_id, sequence);
          }
        });
    if (timer_id == TimerScheduler::kInvalidTimer) {
      if (logger_ != nullptr) {
        NETLIB_LOG_ERROR(*logger_,
                         "failed to schedule request timeout for connection "
                         "id={} sequence={}",
                         connection_id, sequence);
      }
      return false;
    }
    if (!connection.TrackRequestTimer(sequence, timer_id)) {
      request_timer_.Cancel(timer_id);
      if (logger_ != nullptr) {
        NETLIB_LOG_ERROR(*logger_,
                         "duplicate request timer for connection id={} "
                         "sequence={}",
                         connection_id, sequence);
      }
      return false;
    }
    connection.Consume(decode_result.consumed_bytes);
  }
}

void SubReactor::HandleWritable(std::uint64_t connection_id) {
  FlushConnection(connection_id);
}

void SubReactor::FlushConnection(std::uint64_t connection_id) {
  auto connection = connections_.find(connection_id);
  if (connection == connections_.end()) {
    return;
  }

  Connection& state = *connection->second;
  SocketHandle fd = state.fd();
  while (state.HasOutput()) {
    std::span<const char> output = state.PendingOutput();
    std::ptrdiff_t result = platform::Send(fd, output.data(), output.size());
    if (result > 0) {
      state.ConsumeOutput(static_cast<std::size_t>(result));
      continue;
    }
    if (result < 0 && platform::Interrupted(platform::LastError())) {
      continue;
    }
    if (result < 0 && (platform::WouldBlock(platform::LastError()))) {
      if (!state.write_interest_enabled()) {
        ChannelInterest write_interest{
            .read = !state.close_after_write(),
            .write = true,
        };
        if (!state.SetInterest(write_interest)) {
          CloseConnection(connection_id);
          return;
        }
        state.set_write_interest_enabled(true);
      }
      return;
    }
    if (logger_ != nullptr) {
      NETLIB_LOG_WARN(*logger_, "send failed for connection id={}: {}",
                      connection_id, platform::ErrorMessage());
    }
    CloseConnection(connection_id);
    return;
  }

  if (state.close_after_write()) {
    CloseConnection(connection_id);
    return;
  }
  if (state.write_interest_enabled()) {
    if (!state.SetInterest(ChannelInterest{.read = true})) {
      CloseConnection(connection_id);
      return;
    }
    state.set_write_interest_enabled(false);
  }
}

void SubReactor::CloseConnection(std::uint64_t connection_id) {
  auto connection = connections_.find(connection_id);
  if (connection == connections_.end()) {
    return;
  }
  SocketHandle fd = connection->second->fd();
  std::vector<TimerScheduler::TimerId> timer_ids =
      connection->second->TakeRequestTimers();
  for (TimerScheduler::TimerId timer_id : timer_ids) {
    request_timer_.Cancel(timer_id);
  }
  connection->second->MarkClosed();
  if (logger_ != nullptr) {
    NETLIB_LOG_DEBUG(*logger_, "closing connection fd={} id={}", fd,
                     connection_id);
  }
  if (callbacks_.on_closed) {
    callbacks_.on_closed(fd);
  }
  connections_.erase(connection);
}

}  // namespace netlib
