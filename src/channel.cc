#include "netlib/channel.h"

#include <sys/epoll.h>

#include <cstdint>
#include <string>
#include <utility>

#include "netlib/event_loop.h"
#include "netlib/log/logging.h"

namespace netlib {

std::unique_ptr<Channel> Channel::Create(EventLoop& loop, int fd,
                                         ChannelInterest interest,
                                         ChannelCallbacks callbacks) {
  if (fd < 0 || (!interest.read && !interest.write)) {
    if (loop.logger_ != nullptr) {
      NETLIB_LOG_WARN(*loop.logger_,
                      "channel creation rejected for fd=" + std::to_string(fd) +
                          " without valid interest");
    }
    return nullptr;
  }

  auto channel = std::unique_ptr<Channel>(
      new Channel(loop, fd, interest, std::move(callbacks)));
  if (!loop.AddChannel(*channel)) {
    return nullptr;
  }
  channel->registered_ = true;
  if (loop.logger_ != nullptr) {
    NETLIB_LOG_DEBUG(*loop.logger_,
                     "channel registered fd=" + std::to_string(fd));
  }
  return channel;
}

Channel::Channel(EventLoop& loop, int fd, ChannelInterest interest,
                 ChannelCallbacks callbacks)
    : loop_(loop),
      fd_(fd),
      interest_(interest),
      callbacks_(std::move(callbacks)) {}

Channel::~Channel() {
  if (registered_) {
    loop_.RemoveChannel(*this);
  }
}

bool Channel::SetInterest(ChannelInterest interest) {
  if (!interest.read && !interest.write) {
    if (loop_.logger_ != nullptr) {
      NETLIB_LOG_WARN(*loop_.logger_, "channel interest rejected for fd=" +
                                          std::to_string(fd_));
    }
    return false;
  }
  ChannelInterest previous_interest = interest_;
  interest_ = interest;
  if (!loop_.ModifyChannel(*this)) {
    interest_ = previous_interest;
    return false;
  }
  return true;
}

int Channel::fd() const { return fd_; }

void Channel::Dispatch(uint32_t events) {
  // Keep callbacks alive even if the callback removes this Channel.
  ChannelCallbacks callbacks = callbacks_;
  if ((events & (EPOLLIN | EPOLLRDHUP)) != 0 && callbacks.on_readable) {
    callbacks.on_readable();
    return;
  }
  if ((events & EPOLLOUT) != 0 && callbacks.on_writable) {
    callbacks.on_writable();
    return;
  }
  if ((events & (EPOLLERR | EPOLLHUP)) != 0 && callbacks.on_error) {
    callbacks.on_error();
  }
}

}  // namespace netlib
