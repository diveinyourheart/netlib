#include "netlib/sub_reactor.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <stop_token>
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
  kUnordered,
  kTimeout,
  kSessionDeadline,
  kDrainDeadline,
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
  ResponseQueue(std::size_t capacity, std::shared_ptr<platform::Wakeup> wake_fd,
                ThreadPool& worker_pool)
      : capacity_(capacity),
        wake_fd_(std::move(wake_fd)),
        worker_pool_(worker_pool.impl_),
        queue_(capacity),
        consumer_token_(queue_) {
    worker_producers_.reserve(worker_pool.worker_count());
    for (std::size_t i = 0; i < worker_pool.worker_count(); ++i) {
      worker_producers_.emplace_back(queue_);
    }
  }

  bool valid() const {
    return std::all_of(worker_producers_.begin(), worker_producers_.end(),
                       [](const moodycamel::ProducerToken& producer) {
                         return producer.valid();
                       });
  }

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

  bool EnqueueControl(ResponseMessageKind kind, std::uint64_t connection_id,
                      std::uint64_t generation) {
    return EnqueueMessage(
        {.kind = kind, .connection_id = connection_id, .sequence = generation},
        false);
  }

  bool EnqueueUnordered(std::uint64_t connection_id, std::uint64_t sequence,
                        ApplicationResponse response, bool close_after_write) {
    return EnqueueMessage(
        ResponseMessage{
            .kind = ResponseMessageKind::kUnordered,
            .connection_id = connection_id,
            .sequence = sequence,
            .response = std::move(response),
            .close_after_write = close_after_write,
        },
        true);
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
    // Each pool worker has exclusive use of its own token in this queue.
    // Timer callbacks and external async responders keep implicit producers.
    const std::optional<std::size_t> worker =
        ThreadPool::CurrentWorkerIndex(worker_pool_);
    const bool enqueued =
        worker.has_value()
            ? queue_.enqueue(worker_producers_[*worker], std::move(message))
            : queue_.enqueue(std::move(message));
    if (!enqueued) {
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
  // An in-flight external Respond may keep this queue alive after its reactor
  // closes. Retain pool identity state instead of borrowing a ThreadPool
  // object.
  const std::shared_ptr<ThreadPool::Impl> worker_pool_;
  moodycamel::ConcurrentQueue<ResponseMessage> queue_;
  std::vector<moodycamel::ProducerToken> worker_producers_;
  moodycamel::ConsumerToken consumer_token_;
  std::atomic<std::size_t> queued_count_{0};
  std::atomic<bool> accepting_{true};
};

class SubReactor::Connection {
 public:
  Connection(UniqueFd socket, std::unique_ptr<Channel> channel,
             std::unique_ptr<ProtocolSession> session, ConnectionHandle handle,
             std::shared_ptr<std::atomic<bool>> alive,
             std::size_t max_pending_responses)
      : socket_(std::move(socket)),
        channel_(std::move(channel)),
        session_(std::move(session)),
        handle_(std::move(handle)),
        connection_id_(handle_.id()),
        alive_(std::move(alive)),
        max_read_buffer_size_(session_->MaxInputBufferSize()),
        max_pending_responses_(max_pending_responses) {}

  SocketHandle fd() const { return socket_.get(); }
  std::uint64_t connection_id() const { return connection_id_; }
  const ConnectionHandle& handle() const { return handle_; }
  ProtocolSession& session() { return *session_; }
  bool AcceptRequestId(std::uint64_t id) {
    if (id != next_request_id_ ||
        id == std::numeric_limits<std::uint64_t>::max()) {
      return false;
    }
    ++next_request_id_;
    return true;
  }
  bool HasRequestTimerCapacity() const {
    return request_timers_.size() < max_pending_responses_;
  }
  bool TrackRequestTimer(RequestContext context, TimerScheduler::TimerId id,
                         std::stop_source source,
                         TimerScheduler::Clock::time_point deadline) {
    return request_timers_
        .emplace(context.request_id,
                 PendingRequest{context, id, std::move(source), deadline})
        .second;
  }
  std::optional<RequestContext> FindRequest(std::uint64_t id) const {
    auto request = request_timers_.find(id);
    if (request == request_timers_.end()) {
      return std::nullopt;
    }
    return request->second.context;
  }
  bool RequestExpired(std::uint64_t id) const {
    auto request = request_timers_.find(id);
    return request != request_timers_.end() &&
           TimerScheduler::Clock::now() >= request->second.deadline;
  }
  TimerScheduler::TimerId TakeRequestTimer(std::uint64_t id,
                                           bool cancel = false) {
    auto request = request_timers_.find(id);
    if (request == request_timers_.end()) {
      return TimerScheduler::kInvalidTimer;
    }
    if (cancel) request->second.source.request_stop();
    TimerScheduler::TimerId timer_id = request->second.timer_id;
    request_timers_.erase(request);
    return timer_id;
  }
  std::vector<TimerScheduler::TimerId> TakeRequestTimers() {
    std::vector<TimerScheduler::TimerId> ids;
    ids.reserve(request_timers_.size());
    for (auto& [id, request] : request_timers_) {
      request.source.request_stop();
      ids.push_back(request.timer_id);
    }
    request_timers_.clear();
    if (alarm_timer_ != TimerScheduler::kInvalidTimer)
      ids.push_back(alarm_timer_);
    alarm_timer_ = TimerScheduler::kInvalidTimer;
    alarm_deadline_.reset();
    ++alarm_generation_;
    return ids;
  }

  std::span<char> WritableBytes();
  void CommitWrite(std::size_t size);
  std::span<const char> ReadableBytes() const;
  void Consume(std::size_t size);
  bool read_interest_enabled() const { return read_interest_enabled_; }
  bool UpdateInterest(bool write) {
    bool read = close_deadline_.has_value() || session_->WantsInput();
    if (read == read_interest_enabled_ && write == write_interest_enabled_) {
      return true;
    }
    if (!channel_->SetInterest(ChannelInterest{.read = read, .write = write})) {
      return false;
    }
    read_interest_enabled_ = read;
    write_interest_enabled_ = write;
    return true;
  }
  void MarkClosed() { alive_->store(false, std::memory_order_release); }
  void SetDecodeTarget(std::size_t target) { decode_target_size_ = target; }
  void ClearDecodeTarget() { decode_target_size_ = 0; }

 private:
  friend class SubReactor;
  struct PendingRequest {
    RequestContext context;
    TimerScheduler::TimerId timer_id;
    std::stop_source source;
    TimerScheduler::Clock::time_point deadline;
  };
  void MakeWritable(std::size_t min_bytes);

  UniqueFd socket_;
  std::unique_ptr<Channel> channel_;
  const std::unique_ptr<ProtocolSession> session_;
  const ConnectionHandle handle_;
  const std::uint64_t connection_id_;
  const std::shared_ptr<std::atomic<bool>> alive_;
  std::vector<char> storage_;
  std::size_t read_offset_ = 0;
  std::size_t write_offset_ = 0;
  const std::size_t max_read_buffer_size_;
  const std::size_t max_pending_responses_;
  std::size_t decode_target_size_ = 0;
  std::uint64_t next_request_id_ = 0;
  std::map<std::uint64_t, PendingRequest> request_timers_;
  std::optional<TimerScheduler::Clock::time_point> alarm_deadline_;
  std::optional<TimerScheduler::Clock::time_point> write_deadline_;
  std::optional<TimerScheduler::Clock::time_point> close_deadline_;
  std::size_t close_bytes_ = 0;
  TimerScheduler::TimerId alarm_timer_ = TimerScheduler::kInvalidTimer;
  std::uint64_t alarm_generation_ = 0;
  bool read_interest_enabled_ = true;
  bool write_interest_enabled_ = false;
};

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
      std::min(max_read_buffer_size_,
               std::max(kInitialReadBufferSize, storage_.size() * 2));
  storage_.resize(std::max(required_size, grown_size));
}

std::unique_ptr<SubReactor> SubReactor::Create(
    std::size_t max_pending_connections, ThreadPool& worker_pool,
    ConnectionCallbacks callbacks) {
  SubReactorOptions options;
  options.max_pending_connections = max_pending_connections;
  options.max_pending_responses = max_pending_connections;
  return Create(options, worker_pool, std::move(callbacks));
}

std::unique_ptr<SubReactor> SubReactor::Create(
    std::size_t max_pending_connections, ThreadPool& worker_pool,
    ConnectionCallbacks callbacks, ProtocolCodecFactory codec_factory) {
  SubReactorOptions options;
  options.max_pending_connections = max_pending_connections;
  options.max_pending_responses = max_pending_connections;
  return Create(options, worker_pool, std::move(callbacks),
                std::move(codec_factory));
}

std::unique_ptr<SubReactor> SubReactor::Create(SubReactorOptions options,
                                               ThreadPool& worker_pool,
                                               ConnectionCallbacks callbacks) {
  return Create(options, worker_pool, std::move(callbacks),
                [] { return CreateLengthPrefixedCodec(); });
}

std::unique_ptr<SubReactor> SubReactor::Create(
    SubReactorOptions options, ThreadPool& worker_pool,
    ConnectionCallbacks callbacks, ProtocolCodecFactory codec_factory) {
  if (!codec_factory) {
    return nullptr;
  }
  return CreateWithSession(options, worker_pool, std::move(callbacks),
                           [codec_factory = std::move(codec_factory)](
                               ProtocolSessionOptions limits) {
                             return CreateCodecSession(codec_factory(), limits);
                           });
}

std::unique_ptr<SubReactor> SubReactor::CreateWithSession(
    SubReactorOptions options, ThreadPool& worker_pool,
    ConnectionCallbacks callbacks, ProtocolSessionFactory session_factory) {
  if (options.max_pending_connections == 0 ||
      options.max_pending_responses == 0 ||
      options.max_pending_output_bytes == 0 ||
      options.request_timeout <= std::chrono::milliseconds::zero() ||
      options.request_timeout > std::chrono::hours(24) ||
      options.write_timeout <= std::chrono::milliseconds::zero() ||
      options.write_timeout > std::chrono::hours(24) ||
      options.close_timeout <= std::chrono::milliseconds::zero() ||
      options.close_timeout > std::chrono::hours(24)) {
    NETLIB_LOG_ERROR("SubReactor option is outside its valid range");
    return nullptr;
  }
  if (!session_factory) {
    NETLIB_LOG_ERROR("SubReactor requires a ProtocolSession factory");
    return nullptr;
  }
  std::unique_ptr<ProtocolSession> session_probe = session_factory(
      {.max_pending_responses = options.max_pending_responses,
       .max_pending_output_bytes = options.max_pending_output_bytes});
  if (!session_probe || session_probe->MaxInputBufferSize() == 0) {
    NETLIB_LOG_ERROR(
        "SubReactor ProtocolSession factory returned invalid session");
    return nullptr;
  }

  std::unique_ptr<EventLoop> event_loop = EventLoop::Create();
  if (!event_loop) {
    return nullptr;
  }

  std::shared_ptr<platform::Wakeup> wake_fd = platform::Wakeup::Create();
  if (!wake_fd) {
    NETLIB_LOG_ERROR("SubReactor notification failed: {}",
                     platform::ErrorMessage());
    return nullptr;
  }

  auto reactor = std::unique_ptr<SubReactor>(new SubReactor(
      std::move(event_loop), std::move(wake_fd), options, worker_pool,
      std::move(session_factory), std::move(callbacks)));
  if (!reactor->request_submitter_.valid() ||
      !reactor->response_queue_->valid()) {
    return nullptr;
  }
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
  NETLIB_LOG_INFO(
      "SubReactor created with connection capacity={}, "
      "response capacity={}, output bytes={}, timeout_ms={}",
      options.max_pending_connections, options.max_pending_responses,
      options.max_pending_output_bytes, options.request_timeout.count());
  return reactor;
}

SubReactor::SubReactor(std::unique_ptr<EventLoop> event_loop,
                       std::shared_ptr<platform::Wakeup> wake_fd,
                       SubReactorOptions options, ThreadPool& worker_pool,
                       ProtocolSessionFactory session_factory,
                       ConnectionCallbacks callbacks)
    : event_loop_(std::move(event_loop)),
      request_submitter_(worker_pool.CreateSubmitter()),
      session_factory_(std::move(session_factory)),
      callbacks_(std::move(callbacks)),
      wake_fd_(std::move(wake_fd)),
      response_queue_(std::make_shared<ResponseQueue>(
          options.max_pending_responses, wake_fd_, worker_pool)),
      options_(options) {}

SubReactor::~SubReactor() {
  response_queue_->Shutdown();
  CancelRequestTimers();
  for (auto& [id, state] : connections_) state->MarkClosed();
}

bool SubReactor::AdoptConnection(UniqueFd& connection) {
  if (!connection) {
    return false;
  }

  std::lock_guard<std::mutex> lock(pending_mutex_);
  if (stopping_.load() || draining_.load() ||
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

void SubReactor::Run() {
  event_loop_->Run();
  stopping_.store(true);
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending_connections_.clear();
  }
  response_queue_->Shutdown();
  CancelRequestTimers();
  while (!connections_.empty()) CloseConnection(connections_.begin()->first);
}

void SubReactor::Stop() {
  NETLIB_LOG_INFO("SubReactor stopping");
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    stopping_.store(true);
  }
  response_queue_->Shutdown();
  event_loop_->Stop();
}

void SubReactor::HandleWake() {
  if (!DrainWake()) {
    NETLIB_LOG_ERROR("SubReactor wake notification drain failed");
    Stop();
    return;
  }

  ApplyDrain();
  if (stopping_.load()) return;
  std::deque<UniqueFd> pending;
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending.swap(pending_connections_);
  }

  while (!pending.empty() && !stopping_.load() && !draining_.load()) {
    RegisterConnection(std::move(pending.front()));
    pending.pop_front();
  }

  ResponseMessage response;
  while (!stopping_.load() && response_queue_->TryDequeue(response)) {
    if (response.kind == ResponseMessageKind::kSessionDeadline) {
      HandleSessionDeadline(response.connection_id, response.sequence);
      continue;
    }
    if (response.kind == ResponseMessageKind::kDrainDeadline) {
      HandleDrainDeadline(response.sequence);
      continue;
    }
    if (response.kind == ResponseMessageKind::kTimeout) {
      HandleRequestTimeout(response.connection_id, response.sequence);
      continue;
    }
    if (response.kind == ResponseMessageKind::kUnordered) {
      HandleUnorderedResponse(response.connection_id, response.sequence,
                              std::move(response.response),
                              response.close_after_write);
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
  HandleResponse(connection_id, sequence, std::move(response),
                 close_after_write, SessionResponseMode::kOrdered);
}

void SubReactor::HandleUnorderedResponse(std::uint64_t connection_id,
                                         std::uint64_t sequence,
                                         ApplicationResponse response,
                                         bool close_after_write) {
  HandleResponse(connection_id, sequence, std::move(response),
                 close_after_write, SessionResponseMode::kUnordered);
}

void SubReactor::HandleResponse(std::uint64_t connection_id,
                                std::uint64_t sequence,
                                ApplicationResponse response,
                                bool close_after_write,
                                SessionResponseMode mode) {
  auto connection = connections_.find(connection_id);
  if (connection == connections_.end()) {
    return;
  }
  Connection& state = *connection->second;
  std::optional<RequestContext> context = state.FindRequest(sequence);
  if (!context.has_value()) {
    return;
  }
  if (state.RequestExpired(sequence)) {
    HandleRequestTimeout(connection_id, sequence);
    return;
  }
  SessionResponseStatus status = state.session().OnResponse(
      *context, std::move(response), mode, close_after_write);
  TimerScheduler::TimerId timer_id = state.TakeRequestTimer(sequence);
  if (timer_id != TimerScheduler::kInvalidTimer) {
    TimerScheduler::Cancel(timer_id);
  }
  if (status == SessionResponseStatus::kError) {
    NETLIB_LOG_WARN("session response failed for connection id={} sequence={}",
                    connection_id, sequence);
    CloseConnection(connection_id);
    return;
  }
  // Even a rejected reply may cause protocol control output or closure.
  FlushConnection(connection_id);
}

void SubReactor::HandleRequestTimeout(std::uint64_t connection_id,
                                      std::uint64_t sequence) {
  auto connection = connections_.find(connection_id);
  if (connection == connections_.end()) {
    return;
  }
  Connection& state = *connection->second;
  std::optional<RequestContext> context = state.FindRequest(sequence);
  if (!context.has_value()) {
    return;
  }
  TimerScheduler::TimerId timer_id = state.TakeRequestTimer(sequence, true);
  TimerScheduler::Cancel(timer_id);
  SessionResponseStatus status = state.session().OnTimeout(*context);
  if (status == SessionResponseStatus::kError) {
    NETLIB_LOG_WARN("session timeout failed for connection id={} sequence={}",
                    connection_id, sequence);
    CloseConnection(connection_id);
    return;
  }
  FlushConnection(connection_id);
}

bool SubReactor::Drain(std::chrono::milliseconds timeout) {
  if (timeout <= std::chrono::milliseconds::zero() ||
      timeout > std::chrono::hours(24))
    return false;
  const TimerScheduler::Clock::time_point deadline =
      TimerScheduler::Clock::now() + timeout;
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    if (stopping_.load()) return false;
    draining_.store(true);
    if (!requested_drain_deadline_ || deadline < *requested_drain_deadline_) {
      requested_drain_deadline_ = deadline;
    }
  }
  return Wake();
}

void SubReactor::ApplyDrain() {
  std::optional<TimerScheduler::Clock::time_point> requested;
  std::deque<UniqueFd> discarded;
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    requested = requested_drain_deadline_;
    if (requested) discarded.swap(pending_connections_);
  }
  if (!requested || requested == drain_deadline_) return;
  bool begin = !drain_deadline_;
  drain_deadline_ = requested;
  TimerScheduler::Cancel(drain_timer_);
  std::uint64_t generation = ++drain_generation_;
  if (TimerScheduler::Clock::now() >= *requested || connections_.empty()) {
    HandleDrainDeadline(generation);
    if (connections_.empty()) Stop();
    return;
  }
  std::weak_ptr<ResponseQueue> queue = response_queue_;
  drain_timer_ = TimerScheduler::ScheduleAt(*requested, [queue, generation] {
    if (std::shared_ptr<ResponseQueue> target = queue.lock()) {
      target->EnqueueControl(ResponseMessageKind::kDrainDeadline, 0,
                             generation);
    }
  });
  if (drain_timer_ == TimerScheduler::kInvalidTimer) {
    Stop();
    return;
  }
  if (begin) {
    std::vector<std::uint64_t> ids;
    ids.reserve(connections_.size());
    for (const auto& [id, state] : connections_) ids.push_back(id);
    for (std::uint64_t id : ids) {
      auto connection = connections_.find(id);
      if (connection == connections_.end()) continue;
      connection->second->session().BeginDrain();
      FlushConnection(id);
    }
  }
}

void SubReactor::HandleDrainDeadline(std::uint64_t generation) {
  if (generation != drain_generation_ || !drain_deadline_ ||
      TimerScheduler::Clock::now() < *drain_deadline_)
    return;
  while (!connections_.empty()) CloseConnection(connections_.begin()->first);
  Stop();
}

void SubReactor::RefreshSessionTimer(std::uint64_t connection_id) {
  auto connection = connections_.find(connection_id);
  if (connection == connections_.end()) return;
  Connection& state = *connection->second;
  std::optional<TimerScheduler::Clock::time_point> deadline =
      state.close_deadline_ ? state.close_deadline_
                            : state.session().NextDeadline();
  if (state.write_deadline_ &&
      (!deadline || *state.write_deadline_ < *deadline)) {
    deadline = state.write_deadline_;
  }
  if (deadline == state.alarm_deadline_ &&
      (!deadline || state.alarm_timer_ != TimerScheduler::kInvalidTimer))
    return;
  TimerScheduler::Cancel(state.alarm_timer_);
  state.alarm_timer_ = TimerScheduler::kInvalidTimer;
  state.alarm_deadline_ = deadline;
  std::uint64_t generation = ++state.alarm_generation_;
  if (!deadline) return;
  std::weak_ptr<ResponseQueue> queue = response_queue_;
  state.alarm_timer_ =
      TimerScheduler::ScheduleAt(*deadline, [queue, connection_id, generation] {
        if (std::shared_ptr<ResponseQueue> target = queue.lock()) {
          target->EnqueueControl(ResponseMessageKind::kSessionDeadline,
                                 connection_id, generation);
        }
      });
  if (state.alarm_timer_ == TimerScheduler::kInvalidTimer)
    CloseConnection(connection_id);
}

void SubReactor::HandleSessionDeadline(std::uint64_t connection_id,
                                       std::uint64_t generation) {
  auto connection = connections_.find(connection_id);
  if (connection == connections_.end()) return;
  Connection& state = *connection->second;
  if (generation != state.alarm_generation_) return;
  state.alarm_timer_ = TimerScheduler::kInvalidTimer;
  TimerScheduler::Clock::time_point now = TimerScheduler::Clock::now();
  if (state.close_deadline_) {
    if (now >= *state.close_deadline_) {
      CloseConnection(connection_id);
    } else {
      RefreshSessionTimer(connection_id);
    }
    return;
  }
  if (state.write_deadline_ && now >= *state.write_deadline_) {
    CloseConnection(connection_id);
    return;
  }
  state.session().OnDeadline(now);
  // A broken Session must not reschedule the same expired deadline forever.
  std::optional<TimerScheduler::Clock::time_point> next =
      state.session().NextDeadline();
  if (next && *next <= now && !state.session().ShouldClose()) {
    CloseConnection(connection_id);
    return;
  }
  FlushConnection(connection_id);
}

bool SubReactor::DrainWake() { return wake_fd_->Drain(); }

bool SubReactor::Wake() {
  if (wake_fd_ != nullptr && wake_fd_->Notify()) {
    return true;
  }
  NETLIB_LOG_ERROR("SubReactor notification write failed");
  return false;
}

void SubReactor::RegisterConnection(UniqueFd connection) {
  SocketHandle fd = connection.get();
  if (!platform::SetNonBlocking(fd)) {
    NETLIB_LOG_ERROR("failed to set nonblocking mode for fd={}: {}", fd,
                     platform::ErrorMessage());
    return;
  }
  if (!platform::SetCloseOnExec(fd)) {
    NETLIB_LOG_ERROR("failed to set non-inheritable mode for fd={}: {}", fd,
                     platform::ErrorMessage());
    return;
  }

  std::unique_ptr<ProtocolSession> session = session_factory_(
      {.max_pending_responses = options_.max_pending_responses,
       .max_pending_output_bytes = options_.max_pending_output_bytes});
  if (!session || session->MaxInputBufferSize() == 0) {
    NETLIB_LOG_ERROR("ProtocolSession factory failed for fd={}", fd);
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
                            bool close_after_write,
                            ConnectionHandle::ResponseMode mode) {
        std::shared_ptr<ResponseQueue> response_queue =
            weak_response_queue.lock();
        if (!response_queue) {
          return false;
        }
        if (mode == ConnectionHandle::ResponseMode::kUnordered) {
          return response_queue->EnqueueUnordered(
              id, sequence, std::move(response), close_after_write);
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
      std::move(connection), std::move(channel), std::move(session),
      std::move(handle), std::move(alive), options_.max_pending_responses);
  auto inserted = connections_.emplace(connection_id, std::move(state));
  if (!inserted.second) {
    NETLIB_LOG_ERROR("duplicate connection state for id={}", connection_id);
    return;
  }
  NETLIB_LOG_DEBUG("registered connection fd={} id={}", fd, connection_id);
  if (callbacks_.on_connected) {
    callbacks_.on_connected(fd);
  }
  FlushConnection(connection_id);
}

void SubReactor::HandleReadable(std::uint64_t connection_id) {
  while (true) {
    auto connection = connections_.find(connection_id);
    if (connection == connections_.end()) {
      return;
    }

    Connection& state = *connection->second;
    if (state.close_deadline_) {
      DrainConnectionInput(connection_id);
      return;
    }
    if (!state.session().WantsInput()) {
      return;
    }
    SocketHandle fd = state.fd();
    std::span<char> writable = state.WritableBytes();
    if (writable.empty()) {
      NETLIB_LOG_WARN("receive buffer limit reached for connection id={}",
                      connection_id);
      CloseConnection(connection_id);
      return;
    }
    std::ptrdiff_t result =
        platform::Receive(fd, writable.data(), writable.size());
    if (result > 0) {
      state.CommitWrite(static_cast<std::size_t>(result));
      if (!ProcessInput(state)) {
        CloseConnection(connection_id);
        return;
      }
      // Input can generate control output and can close this connection. The
      // next iteration must look it up again before touching its state.
      FlushConnection(connection_id);
      continue;
    }
    if (result == 0) {
      NETLIB_LOG_DEBUG("peer closed fd={}", fd);
      CloseConnection(connection_id);
      return;
    }
    if (platform::Interrupted(platform::LastError())) {
      continue;
    }
    if (platform::WouldBlock(platform::LastError())) {
      return;
    }
    NETLIB_LOG_WARN("recv failed for fd={}: {}", fd, platform::ErrorMessage());
    CloseConnection(connection_id);
    return;
  }
}

bool SubReactor::ProcessInput(Connection& connection) {
  while (connection.session().WantsInput()) {
    std::span<const char> readable = connection.ReadableBytes();
    SessionInputResult result = connection.session().OnInput(readable);
    if (result.status == SessionInputStatus::kNeedMore) {
      if (result.consumed_bytes != 0 || !result.requests.empty() ||
          !result.cancelled_requests.empty() ||
          result.required_bytes > connection.session().MaxInputBufferSize() ||
          (result.required_bytes != 0 &&
           result.required_bytes <= readable.size())) {
        return false;
      }
      connection.SetDecodeTarget(result.required_bytes);
      return true;
    }
    if (result.status != SessionInputStatus::kProgress ||
        result.consumed_bytes == 0 || result.consumed_bytes > readable.size() ||
        result.required_bytes != 0) {
      return false;
    }
    connection.ClearDecodeTarget();
    connection.Consume(result.consumed_bytes);
    for (const RequestContext& cancelled : result.cancelled_requests) {
      std::optional<RequestContext> active =
          connection.FindRequest(cancelled.request_id);
      if (active.has_value() && *active == cancelled) {
        TimerScheduler::Cancel(
            connection.TakeRequestTimer(cancelled.request_id, true));
      }
    }
    for (SessionRequest& request : result.requests) {
      if (!connection.HasRequestTimerCapacity() ||
          !connection.AcceptRequestId(request.context.request_id)) {
        return false;
      }
      const RequestContext context = request.context;
      std::uint64_t connection_id = connection.connection_id();
      std::uint64_t sequence = context.request_id;
      std::stop_source source;
      FrameMessage message{
          .connection_id = connection_id,
          .sequence = sequence,
          .connection = connection.handle(),
          .metadata = std::move(request.metadata),
          .payload = std::move(request.payload),
          .request_context = context,
          .stop_token = source.get_token(),
      };
      std::weak_ptr<ResponseQueue> weak_response_queue = response_queue_;
      TimerScheduler::Clock::time_point deadline =
          TimerScheduler::Clock::now() + options_.request_timeout;
      TimerScheduler::TimerId timer_id = TimerScheduler::ScheduleAt(
          deadline, [weak_response_queue, connection_id, sequence] {
            std::shared_ptr<ResponseQueue> response_queue =
                weak_response_queue.lock();
            if (response_queue != nullptr) {
              response_queue->EnqueueTimeout(connection_id, sequence);
            }
          });
      if (timer_id == TimerScheduler::kInvalidTimer) {
        NETLIB_LOG_ERROR(
            "failed to schedule request timeout for connection id={} "
            "sequence={}",
            connection_id, sequence);
        return false;
      }
      if (!connection.TrackRequestTimer(context, timer_id, std::move(source),
                                        deadline)) {
        TimerScheduler::Cancel(timer_id);
        return false;
      }
      if (!request_submitter_.Submit(std::move(message))) {
        NETLIB_LOG_WARN("worker queue full for connection id={} sequence={}",
                        connection_id, sequence);
        return false;
      }
    }
  }
  return true;
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
  if (state.close_deadline_) return;
  ProtocolSession& session = state.session();
  // A Session can pause with a partial/coalesced request still in our buffer.
  // Resuming socket readiness alone cannot deliver those already-read bytes.
  if (session.WantsInput() && !state.read_interest_enabled() &&
      !state.ReadableBytes().empty() && !ProcessInput(state)) {
    CloseConnection(connection_id);
    return;
  }
  while (true) {
    if (!session.ProduceOutput(options_.max_pending_output_bytes)) {
      CloseConnection(connection_id);
      return;
    }
    for (const RequestContext& cancelled : session.TakeCancelledRequests()) {
      std::optional<RequestContext> active =
          state.FindRequest(cancelled.request_id);
      if (active.has_value() && *active == cancelled) {
        TimerScheduler::Cancel(
            state.TakeRequestTimer(cancelled.request_id, true));
      }
    }
    std::span<const char> output = session.PendingOutput();
    if (output.size() > options_.max_pending_output_bytes) {
      CloseConnection(connection_id);
      return;
    }
    if (output.empty()) {
      state.write_deadline_.reset();
      break;
    }
    if (!state.write_deadline_)
      state.write_deadline_ =
          TimerScheduler::Clock::now() + options_.write_timeout;
    std::ptrdiff_t result =
        platform::Send(state.fd(), output.data(), output.size());
    if (result > 0) {
      state.write_deadline_ =
          TimerScheduler::Clock::now() + options_.write_timeout;
      session.ConsumeOutput(static_cast<std::size_t>(result));
      continue;
    }
    if (result < 0 && platform::Interrupted(platform::LastError())) {
      continue;
    }
    if (result < 0 && platform::WouldBlock(platform::LastError())) {
      if (!state.UpdateInterest(true)) {
        CloseConnection(connection_id);
      } else {
        RefreshSessionTimer(connection_id);
      }
      return;
    }
    NETLIB_LOG_WARN("send failed for connection id={}: {}", connection_id,
                    platform::ErrorMessage());
    CloseConnection(connection_id);
    return;
  }
  if (session.ShouldClose()) {
    if (session.ShouldHalfClose()) {
      ShutdownConnection(connection_id);
    } else {
      CloseConnection(connection_id);
    }
  } else if (!state.UpdateInterest(false)) {
    CloseConnection(connection_id);
  } else {
    RefreshSessionTimer(connection_id);
  }
}

void SubReactor::ShutdownConnection(std::uint64_t connection_id) {
  auto connection = connections_.find(connection_id);
  if (connection == connections_.end()) return;
  Connection& state = *connection->second;
  if (state.close_deadline_) return;
  // A full close can reset TCP when a delayed SETTINGS ACK or other control
  // arrives, discarding the peer's unread response. Send FIN first and retain
  // read interest without parsing or dispatching any more protocol input.
  state.MarkClosed();
  if (!platform::ShutdownWrite(state.fd())) {
    CloseConnection(connection_id);
    return;
  }
  state.close_deadline_ = TimerScheduler::Clock::now() + options_.close_timeout;
  std::optional<TimerScheduler::Clock::time_point> protocol_deadline =
      state.session().NextDeadline();
  if (protocol_deadline && *protocol_deadline < *state.close_deadline_)
    state.close_deadline_ = protocol_deadline;
  state.close_bytes_ = state.ReadableBytes().size();
  state.Consume(state.close_bytes_);
  if (!state.UpdateInterest(false)) {
    CloseConnection(connection_id);
    return;
  }
  RefreshSessionTimer(connection_id);
  // Read immediately: epoll ET may already have reported these queued bytes.
  DrainConnectionInput(connection_id);
}

void SubReactor::DrainConnectionInput(std::uint64_t connection_id) {
  auto connection = connections_.find(connection_id);
  if (connection == connections_.end()) return;
  Connection& state = *connection->second;
  assert(state.close_deadline_.has_value());
  // Bound even a peer that continuously sends after FIN. This deadline never
  // extends with incoming bytes, and external Drain/Stop can still close
  // sooner.
  constexpr std::size_t kMaxCloseDrainBytes = 64 * 1024;
  std::array<char, 4096> discarded{};
  while (state.close_bytes_ < kMaxCloseDrainBytes &&
         TimerScheduler::Clock::now() < *state.close_deadline_) {
    std::ptrdiff_t count = platform::Receive(
        state.fd(), discarded.data(),
        std::min(discarded.size(), kMaxCloseDrainBytes - state.close_bytes_));
    if (count > 0) {
      state.close_bytes_ += static_cast<std::size_t>(count);
      continue;
    }
    if (count < 0 && platform::Interrupted(platform::LastError())) continue;
    if (count < 0 && platform::WouldBlock(platform::LastError())) return;
    CloseConnection(connection_id);
    return;
  }
  CloseConnection(connection_id);
}

void SubReactor::CancelRequestTimers() {
  TimerScheduler::Cancel(drain_timer_);
  drain_timer_ = TimerScheduler::kInvalidTimer;
  for (auto& entry : connections_) {
    std::vector<TimerScheduler::TimerId> timer_ids =
        entry.second->TakeRequestTimers();
    for (TimerScheduler::TimerId timer_id : timer_ids) {
      TimerScheduler::Cancel(timer_id);
    }
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
    TimerScheduler::Cancel(timer_id);
  }
  connection->second->MarkClosed();
  NETLIB_LOG_DEBUG("closing connection fd={} id={}", fd, connection_id);
  if (callbacks_.on_closed) {
    callbacks_.on_closed(fd);
  }
  connections_.erase(connection);
  if (drain_deadline_.has_value() && connections_.empty()) Stop();
}

}  // namespace netlib
