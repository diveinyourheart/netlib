#include "netlib/sub_reactor.h"

#include <fcntl.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
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

bool SetNonBlocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return false;
  }
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

bool SetCloseOnExec(int fd) {
  int flags = fcntl(fd, F_GETFD, 0);
  if (flags < 0) {
    return false;
  }
  return fcntl(fd, F_SETFD, flags | FD_CLOEXEC) == 0;
}

bool WakeEventFd(UniqueFd& wake_fd) {
  uint64_t one = 1;
  while (true) {
    ssize_t result = write(wake_fd.get(), &one, sizeof(one));
    if (result == sizeof(one)) {
      return true;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    return result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK);
  }
}

}  // namespace

struct ResponseMessage {
  std::uint64_t connection_id = 0;
  std::vector<char> payload;
  bool close_after_write = false;
};

class SubReactor::ResponseQueue {
 public:
  ResponseQueue(std::size_t capacity, std::shared_ptr<UniqueFd> wake_fd)
      : capacity_(capacity),
        wake_fd_(std::move(wake_fd)),
        queue_(capacity),
        consumer_token_(queue_) {}

  bool Enqueue(ResponseMessage message) {
    if (!accepting_.load(std::memory_order_acquire) || !ReserveQueueSlot()) {
      return false;
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

  bool TryDequeue(ResponseMessage& message) {
    if (!queue_.try_dequeue(consumer_token_, message)) {
      return false;
    }
    queued_count_.fetch_sub(1, std::memory_order_acq_rel);
    return true;
  }

  void Shutdown() { accepting_.store(false, std::memory_order_release); }

 private:
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

  bool Wake() { return wake_fd_ != nullptr && WakeEventFd(*wake_fd_); }

  const std::size_t capacity_;
  const std::shared_ptr<UniqueFd> wake_fd_;
  moodycamel::ConcurrentQueue<ResponseMessage> queue_;
  moodycamel::ConsumerToken consumer_token_;
  std::atomic<std::size_t> queued_count_{0};
  std::atomic<bool> accepting_{true};
};

class SubReactor::Connection {
 public:
  Connection(UniqueFd socket, std::unique_ptr<Channel> channel,
             std::unique_ptr<ProtocolCodec> codec, ConnectionHandle handle,
             std::shared_ptr<std::atomic<bool>> alive,
             std::size_t max_output_buffer_size);

  int fd() const;
  std::uint64_t connection_id() const;
  const ConnectionHandle& handle() const;
  ProtocolCodec& codec();
  std::uint64_t NextSequence();

  std::span<char> WritableBytes();
  void CommitWrite(std::size_t size);
  std::span<const char> ReadableBytes() const;
  void Consume(std::size_t size);

  bool AppendResponse(std::span<const char> payload, bool close_after_write);
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
  std::uint64_t next_sequence_ = 0;
  std::vector<char> output_buffer_;
  std::size_t output_offset_ = 0;
  bool close_after_write_ = false;
  bool write_interest_enabled_ = false;
};

SubReactor::Connection::Connection(UniqueFd socket,
                                   std::unique_ptr<Channel> channel,
                                   std::unique_ptr<ProtocolCodec> codec,
                                   ConnectionHandle handle,
                                   std::shared_ptr<std::atomic<bool>> alive,
                                   std::size_t max_output_buffer_size)
    : socket_(std::move(socket)),
      channel_(std::move(channel)),
      codec_(std::move(codec)),
      handle_(std::move(handle)),
      connection_id_(handle_.id()),
      alive_(std::move(alive)),
      max_read_buffer_size_(codec_->MaxDecodeBufferSize()),
      max_output_buffer_size_(max_output_buffer_size) {}

int SubReactor::Connection::fd() const { return socket_.get(); }

std::uint64_t SubReactor::Connection::connection_id() const {
  return connection_id_;
}

const ConnectionHandle& SubReactor::Connection::handle() const {
  return handle_;
}

ProtocolCodec& SubReactor::Connection::codec() { return *codec_; }

std::uint64_t SubReactor::Connection::NextSequence() {
  return next_sequence_++;
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

bool SubReactor::Connection::AppendResponse(std::span<const char> payload,
                                            bool close_after_write) {
  CompactOutput();
  if (output_buffer_.size() >= max_output_buffer_size_ ||
      payload.size() > max_output_buffer_size_ - output_buffer_.size()) {
    return false;
  }
  std::size_t old_size = output_buffer_.size();
  if (!codec_->Encode(payload, &output_buffer_)) {
    return false;
  }
  if (output_buffer_.size() > max_output_buffer_size_) {
    output_buffer_.resize(old_size);
    return false;
  }
  close_after_write_ = close_after_write_ || close_after_write;
  return true;
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
      options.max_pending_output_bytes == 0) {
    if (logger != nullptr) {
      NETLIB_LOG_ERROR(*logger, "SubReactor option capacity is zero");
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

  std::shared_ptr<UniqueFd> wake_fd =
      std::make_shared<UniqueFd>(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
  if (!*wake_fd) {
    if (logger != nullptr) {
      NETLIB_LOG_ERROR(*logger, "SubReactor eventfd failed: {}",
                       std::strerror(errno));
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
      Channel::Create(*reactor->event_loop_, reactor->wake_fd_->get(),
                      ChannelInterest{.read = true}, std::move(wake_callbacks));
  if (!reactor->wake_channel_) {
    return nullptr;
  }
  if (logger != nullptr) {
    NETLIB_LOG_INFO(*logger,
                    "SubReactor created with connection capacity={}, "
                    "response capacity={}, output bytes={}",
                    options.max_pending_connections,
                    options.max_pending_responses,
                    options.max_pending_output_bytes);
  }
  return reactor;
}

SubReactor::SubReactor(std::unique_ptr<EventLoop> event_loop,
                       std::shared_ptr<UniqueFd> wake_fd,
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

SubReactor::~SubReactor() { response_queue_->Shutdown(); }

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
  response_queue_->Shutdown();
  event_loop_->Stop();
}

void SubReactor::HandleWake() {
  if (!DrainWake()) {
    if (logger_ != nullptr) {
      NETLIB_LOG_ERROR(*logger_, "SubReactor wake eventfd drain failed");
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
    auto connection = connections_.find(response.connection_id);
    if (connection == connections_.end()) {
      continue;
    }
    Connection& state = *connection->second;
    if (!state.AppendResponse(response.payload, response.close_after_write)) {
      if (logger_ != nullptr) {
        NETLIB_LOG_WARN(*logger_,
                        "failed to encode response for connection id={}",
                        response.connection_id);
      }
      CloseConnection(response.connection_id);
      continue;
    }
    FlushConnection(response.connection_id);
  }
}

bool SubReactor::DrainWake() {
  uint64_t value = 0;
  while (true) {
    ssize_t result = read(wake_fd_->get(), &value, sizeof(value));
    if (result == sizeof(value)) {
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return true;
    }
    if (logger_ != nullptr) {
      NETLIB_LOG_ERROR(*logger_, "SubReactor eventfd read failed: {}",
                       std::strerror(errno));
    }
    return false;
  }
}

bool SubReactor::Wake() {
  if (wake_fd_ != nullptr && WakeEventFd(*wake_fd_)) {
    return true;
  }
  if (logger_ != nullptr) {
    NETLIB_LOG_ERROR(*logger_, "SubReactor eventfd write failed");
  }
  return false;
}

void SubReactor::RegisterConnection(UniqueFd connection) {
  int fd = connection.get();
  if (!SetNonBlocking(fd)) {
    if (logger_ != nullptr) {
      NETLIB_LOG_ERROR(*logger_, "failed to set O_NONBLOCK for fd={}: {}", fd,
                       std::strerror(errno));
    }
    return;
  }
  if (!SetCloseOnExec(fd)) {
    if (logger_ != nullptr) {
      NETLIB_LOG_ERROR(*logger_, "failed to set FD_CLOEXEC for fd={}: {}", fd,
                       std::strerror(errno));
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
      [weak_response_queue](std::uint64_t id, std::vector<char> payload,
                            bool close_after_write) {
        std::shared_ptr<ResponseQueue> response_queue =
            weak_response_queue.lock();
        if (!response_queue) {
          return false;
        }
        return response_queue->Enqueue(ResponseMessage{
            .connection_id = id,
            .payload = std::move(payload),
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
      std::move(handle), std::move(alive), options_.max_pending_output_bytes);
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
  int fd = state.fd();
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
    ssize_t result = recv(fd, writable.data(), writable.size(), 0);
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
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return;
    }
    if (logger_ != nullptr) {
      NETLIB_LOG_WARN(*logger_, "recv failed for fd={}: {}", fd,
                      std::strerror(errno));
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
    std::uint64_t connection_id = connection.connection_id();
    std::uint64_t sequence = connection.NextSequence();
    std::span<const char> payload = readable.subspan(
        decode_result.payload_offset, decode_result.payload_size);
    FrameMessage message{
        .connection_id = connection_id,
        .sequence = sequence,
        .connection = connection.handle(),
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
  int fd = state.fd();
  while (state.HasOutput()) {
    std::span<const char> output = state.PendingOutput();
    ssize_t result = send(fd, output.data(), output.size(), MSG_NOSIGNAL);
    if (result > 0) {
      state.ConsumeOutput(static_cast<std::size_t>(result));
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
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
                      connection_id, std::strerror(errno));
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
  int fd = connection->second->fd();
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
