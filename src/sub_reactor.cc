#include "netlib/sub_reactor.h"

#include <fcntl.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string_view>
#include <utility>

#include "netlib/channel.h"
#include "netlib/event_loop.h"
#include "netlib/unique_fd.h"

namespace netlib {
namespace {

constexpr std::size_t kReadBufferSize = 64 * 1024;

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

}  // namespace

std::unique_ptr<SubReactor> SubReactor::Create(
    std::size_t max_pending_connections, ConnectionCallbacks callbacks) {
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

  auto reactor = std::unique_ptr<SubReactor>(
      new SubReactor(std::move(event_loop), std::move(wake_fd),
                     max_pending_connections, std::move(callbacks)));
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
                       ConnectionCallbacks callbacks)
    : event_loop_(std::move(event_loop)),
      callbacks_(std::move(callbacks)),
      wake_fd_(std::move(wake_fd)),
      max_pending_connections_(max_pending_connections) {}

SubReactor::~SubReactor() = default;

bool SubReactor::AdoptConnection(UniqueFd connection) {
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

  auto inserted = connections_.emplace(
      fd, Connection{std::move(connection), std::move(channel)});
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

  std::array<char, kReadBufferSize> buffer{};
  while (true) {
    ssize_t result = recv(fd, buffer.data(), buffer.size(), 0);
    if (result > 0) {
      if (callbacks_.on_data) {
        callbacks_.on_data(
            fd,
            std::string_view(buffer.data(), static_cast<std::size_t>(result)));
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
