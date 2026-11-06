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

#include "netlib/channel.h"
#include "netlib/event_loop.h"
#include "netlib/frame_message.h"
#include "netlib/frame_protocol.h"
#include "netlib/thread_pool.h"
#include "netlib/unique_fd.h"

namespace netlib {
namespace {

constexpr std::size_t kInitialReadBufferSize = 4 * 1024;
constexpr std::size_t kMaxReadBufferSize =
    kFrameLengthPrefixSize + kMaxFramePayloadSize;

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

std::uint32_t DecodePayloadSize(std::span<const char> header) {
  assert(header.size() >= kFrameLengthPrefixSize);
  return (static_cast<std::uint32_t>(static_cast<unsigned char>(header[0]))
          << 24U) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(header[1]))
          << 16U) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(header[2]))
          << 8U) |
         static_cast<std::uint32_t>(static_cast<unsigned char>(header[3]));
}

}  // namespace

class SubReactor::Connection {
 public:
  Connection(UniqueFd socket, std::unique_ptr<Channel> channel);

  int fd() const;
  std::span<char> WritableBytes();
  void CommitWrite(std::size_t size);
  std::span<const char> ReadableBytes() const;
  void Consume(std::size_t size);
  std::uint64_t connection_id() const;
  std::uint64_t NextSequence();
  std::size_t expected_frame_size() const;
  void set_expected_frame_size(std::size_t frame_size);

 private:
  void MakeWritable(std::size_t min_bytes);

  UniqueFd socket_;
  std::unique_ptr<Channel> channel_;
  std::vector<char> storage_;
  std::size_t read_offset_ = 0;
  std::size_t write_offset_ = 0;
  std::size_t expected_frame_size_ = 0;
  const std::uint64_t connection_id_;
  std::uint64_t next_sequence_ = 0;
};

SubReactor::Connection::Connection(UniqueFd socket,
                                   std::unique_ptr<Channel> channel)
    : socket_(std::move(socket)),
      channel_(std::move(channel)),
      connection_id_(AllocateConnectionId()) {}

int SubReactor::Connection::fd() const { return socket_.get(); }

std::span<char> SubReactor::Connection::WritableBytes() {
  std::size_t writable_size = kInitialReadBufferSize;
  if (expected_frame_size_ != 0) {
    std::size_t readable_size = write_offset_ - read_offset_;
    writable_size =
        std::min(writable_size, expected_frame_size_ - readable_size);
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

std::uint64_t SubReactor::Connection::connection_id() const {
  return connection_id_;
}

std::uint64_t SubReactor::Connection::NextSequence() {
  return next_sequence_++;
}

std::size_t SubReactor::Connection::expected_frame_size() const {
  return expected_frame_size_;
}

void SubReactor::Connection::set_expected_frame_size(std::size_t frame_size) {
  expected_frame_size_ = frame_size;
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
  assert(required_size <= kMaxReadBufferSize);
  std::size_t grown_size =
      std::max(kInitialReadBufferSize,
               std::min(kMaxReadBufferSize, storage_.size() * 2));
  storage_.resize(std::max(required_size, grown_size));
}

std::unique_ptr<SubReactor> SubReactor::Create(
    std::size_t max_pending_connections, ThreadPool& worker_pool,
    ConnectionCallbacks callbacks) {
  if (max_pending_connections == 0) {
    return nullptr;
  }

  std::unique_ptr<EventLoop> event_loop = EventLoop::Create();
  if (!event_loop) {
    return nullptr;
  }

  UniqueFd wake_fd(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
  if (!wake_fd) {
    return nullptr;
  }

  auto reactor = std::unique_ptr<SubReactor>(new SubReactor(
      std::move(event_loop), std::move(wake_fd), max_pending_connections,
      worker_pool, std::move(callbacks)));
  SubReactor* reactor_pointer = reactor.get();
  ChannelCallbacks wake_callbacks{
      [reactor_pointer] { reactor_pointer->HandleWake(); },
      {},
      [reactor_pointer] { reactor_pointer->Stop(); },
  };
  reactor->wake_channel_ =
      Channel::Create(*reactor->event_loop_, reactor->wake_fd_.get(),
                      ChannelInterest{.read = true}, std::move(wake_callbacks));
  if (!reactor->wake_channel_) {
    return nullptr;
  }
  return reactor;
}

SubReactor::SubReactor(std::unique_ptr<EventLoop> event_loop, UniqueFd wake_fd,
                       std::size_t max_pending_connections,
                       ThreadPool& worker_pool, ConnectionCallbacks callbacks)
    : event_loop_(std::move(event_loop)),
      worker_pool_(worker_pool),
      callbacks_(std::move(callbacks)),
      wake_fd_(std::move(wake_fd)),
      max_pending_connections_(max_pending_connections) {}

SubReactor::~SubReactor() = default;

bool SubReactor::AdoptConnection(UniqueFd& connection) {
  if (!connection) {
    return false;
  }

  std::lock_guard<std::mutex> lock(pending_mutex_);
  if (stopping_.load() ||
      pending_connections_.size() >= max_pending_connections_) {
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
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    stopping_.store(true);
  }
  event_loop_->Stop();
}

void SubReactor::HandleWake() {
  if (!DrainWake()) {
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
}

bool SubReactor::DrainWake() {
  uint64_t value = 0;
  while (true) {
    ssize_t result = read(wake_fd_.get(), &value, sizeof(value));
    if (result == sizeof(value)) {
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return true;
    }
    return false;
  }
}

bool SubReactor::Wake() {
  uint64_t one = 1;
  while (true) {
    ssize_t result = write(wake_fd_.get(), &one, sizeof(one));
    if (result == sizeof(one)) {
      return true;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return true;
    }
    return false;
  }
}

void SubReactor::RegisterConnection(UniqueFd connection) {
  int fd = connection.get();
  if (!SetNonBlocking(fd) || !SetCloseOnExec(fd)) {
    return;
  }

  ChannelCallbacks channel_callbacks{
      [this, fd] { HandleReadable(fd); },
      {},
      [this, fd] { CloseConnection(fd); },
  };
  std::unique_ptr<Channel> channel =
      Channel::Create(*event_loop_, fd, ChannelInterest{.read = true},
                      std::move(channel_callbacks));
  if (!channel) {
    return;
  }

  std::unique_ptr<Connection> state =
      std::make_unique<Connection>(std::move(connection), std::move(channel));
  auto inserted = connections_.emplace(fd, std::move(state));
  if (!inserted.second) {
    return;
  }
  if (callbacks_.on_connected) {
    callbacks_.on_connected(fd);
  }
}

void SubReactor::HandleReadable(int fd) {
  auto connection = connections_.find(fd);
  if (connection == connections_.end()) {
    return;
  }

  Connection& state = *connection->second;
  while (true) {
    std::span<char> writable = state.WritableBytes();
    ssize_t result = recv(fd, writable.data(), writable.size(), 0);
    if (result > 0) {
      state.CommitWrite(static_cast<std::size_t>(result));
      if (!ProcessFrames(state)) {
        CloseConnection(fd);
        return;
      }
      continue;
    }
    if (result == 0) {
      CloseConnection(fd);
      return;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return;
    }
    CloseConnection(fd);
    return;
  }
}

bool SubReactor::ProcessFrames(Connection& connection) {
  while (true) {
    std::span<const char> readable = connection.ReadableBytes();
    std::size_t expected_size = connection.expected_frame_size();

    if (expected_size == 0) {
      if (readable.size() < kFrameLengthPrefixSize) {
        return true;
      }
      std::uint32_t payload_size =
          DecodePayloadSize(readable.first(kFrameLengthPrefixSize));
      if (payload_size > kMaxFramePayloadSize) {
        return false;
      }
      expected_size =
          kFrameLengthPrefixSize + static_cast<std::size_t>(payload_size);
      connection.set_expected_frame_size(expected_size);
    }

    if (readable.size() < expected_size) {
      return true;
    }

    std::span<const char> payload = readable.subspan(
        kFrameLengthPrefixSize, expected_size - kFrameLengthPrefixSize);
    std::uint64_t connection_id = connection.connection_id();
    std::uint64_t sequence = connection.NextSequence();
    FrameMessage message{
        .connection_id = connection_id,
        .sequence = sequence,
        .payload = std::vector<char>(payload.begin(), payload.end()),
    };
    if (!worker_pool_.Submit(std::move(message))) {
      return false;
    }
    connection.Consume(expected_size);
    connection.set_expected_frame_size(0);
  }
}

void SubReactor::CloseConnection(int fd) {
  auto connection = connections_.find(fd);
  if (connection == connections_.end()) {
    return;
  }
  if (callbacks_.on_closed) {
    callbacks_.on_closed(fd);
  }
  connections_.erase(connection);
}

}  // namespace netlib
