#include "netlib/event_loop.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "netlib/channel.h"
#include "netlib/log/logging.h"
#include "netlib/unique_fd.h"

namespace netlib {
namespace {

constexpr int kMaxEvents = 64;

uint32_t ToEpollEvents(ChannelInterest interest) {
  uint32_t events = EPOLLET;
  if (interest.read) {
    events |= EPOLLIN | EPOLLRDHUP;
  }
  if (interest.write) {
    events |= EPOLLOUT;
  }
  return events;
}

}  // namespace

std::unique_ptr<EventLoop> EventLoop::Create(log::Logger* logger) {
  UniqueFd epoll_fd(epoll_create1(EPOLL_CLOEXEC));
  if (!epoll_fd) {
    if (logger != nullptr) {
      NETLIB_LOG_ERROR(*logger,
                       std::string("event loop epoll_create1 failed: ") +
                           std::strerror(errno));
    }
    return nullptr;
  }

  UniqueFd stop_event_fd(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
  if (!stop_event_fd) {
    if (logger != nullptr) {
      NETLIB_LOG_ERROR(*logger, std::string("event loop eventfd failed: ") +
                                    std::strerror(errno));
    }
    return nullptr;
  }

  std::unique_ptr<EventLoop> loop = std::unique_ptr<EventLoop>(
      new EventLoop(std::move(epoll_fd), std::move(stop_event_fd), logger));
  EventLoop* loop_pointer = loop.get();
  ChannelCallbacks stop_callbacks{
      [loop_pointer] { loop_pointer->DrainStopEvent(); },
      {},
      [loop_pointer] { loop_pointer->Stop(); },
  };
  loop->stop_channel_ =
      Channel::Create(*loop, loop->stop_event_fd_.get(),
                      ChannelInterest{.read = true}, std::move(stop_callbacks));
  if (!loop->stop_channel_) {
    if (logger != nullptr) {
      NETLIB_LOG_ERROR(
          *logger,
          std::string("event loop stop Channel registration failed: ") +
              std::strerror(errno));
    }
    return nullptr;
  }
  if (logger != nullptr) {
    NETLIB_LOG_DEBUG(*logger, "event loop created");
  }
  return loop;
}

EventLoop::EventLoop(UniqueFd epoll_fd, UniqueFd stop_event_fd,
                     log::Logger* logger)
    : epoll_fd_(std::move(epoll_fd)),
      stop_event_fd_(std::move(stop_event_fd)),
      logger_(logger) {}

EventLoop::~EventLoop() = default;

void EventLoop::Run() {
  if (logger_ != nullptr) {
    NETLIB_LOG_DEBUG(*logger_, "event loop started");
  }
  std::array<epoll_event, kMaxEvents> events{};
  while (!stopping_.load()) {
    int ready_count =
        epoll_wait(epoll_fd_.get(), events.data(), kMaxEvents, -1);
    if (ready_count < 0) {
      if (errno == EINTR) {
        continue;
      }
      if (logger_ != nullptr) {
        NETLIB_LOG_ERROR(*logger_, std::string("epoll_wait failed: ") +
                                       std::strerror(errno));
      }
      break;
    }

    for (int i = 0; i < ready_count; ++i) {
      Dispatch(events[i].data.fd, events[i].events);
    }
  }
  if (logger_ != nullptr) {
    NETLIB_LOG_DEBUG(*logger_, "event loop stopped");
  }
}

void EventLoop::Stop() {
  if (!stopping_.exchange(true)) {
    if (logger_ != nullptr) {
      NETLIB_LOG_DEBUG(*logger_, "event loop stop requested");
    }
    WakeForStop();
  }
}

bool EventLoop::AddChannel(Channel& channel) {
  epoll_event event{};
  event.events = ToEpollEvents(channel.interest_);
  event.data.fd = channel.fd_;
  if (epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, channel.fd_, &event) != 0) {
    if (logger_ != nullptr) {
      NETLIB_LOG_ERROR(*logger_, "epoll_ctl ADD failed for fd=" +
                                     std::to_string(channel.fd_) + ": " +
                                     std::strerror(errno));
    }
    return false;
  }

  channels_.emplace(channel.fd_, &channel);
  return true;
}

void EventLoop::RemoveChannel(Channel& channel) {
  channels_.erase(channel.fd_);
  if (epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, channel.fd_, nullptr) != 0 &&
      errno != ENOENT && errno != EBADF && logger_ != nullptr) {
    NETLIB_LOG_ERROR(
        *logger_, "epoll_ctl DEL failed for fd=" + std::to_string(channel.fd_) +
                      ": " + std::strerror(errno));
  }
}

bool EventLoop::ModifyChannel(Channel& channel) {
  epoll_event event{};
  event.events = ToEpollEvents(channel.interest_);
  event.data.fd = channel.fd_;
  if (epoll_ctl(epoll_fd_.get(), EPOLL_CTL_MOD, channel.fd_, &event) != 0) {
    if (logger_ != nullptr) {
      NETLIB_LOG_ERROR(*logger_, "epoll_ctl MOD failed for fd=" +
                                     std::to_string(channel.fd_) + ": " +
                                     std::strerror(errno));
    }
    return false;
  }
  return true;
}

void EventLoop::Dispatch(int fd, uint32_t events) {
  auto channel = channels_.find(fd);
  if (channel != channels_.end()) {
    channel->second->Dispatch(events);
  }
}

void EventLoop::DrainStopEvent() {
  uint64_t value = 0;
  while (true) {
    ssize_t result = read(stop_event_fd_.get(), &value, sizeof(value));
    if (result == sizeof(value)) {
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return;
    }
    if (logger_ != nullptr) {
      NETLIB_LOG_ERROR(*logger_, std::string("stop eventfd read failed: ") +
                                     std::strerror(errno));
    }
    return;
  }
}

void EventLoop::WakeForStop() {
  uint64_t one = 1;
  while (write(stop_event_fd_.get(), &one, sizeof(one)) < 0) {
    if (errno == EINTR) {
      continue;
    }
    if (logger_ != nullptr) {
      NETLIB_LOG_ERROR(*logger_, std::string("stop eventfd write failed: ") +
                                     std::strerror(errno));
    }
    break;
  }
}

}  // namespace netlib
