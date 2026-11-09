#include "netlib/channel.h"

#include <cstdint>
#include <utility>

#include "netlib/event_loop.h"
#include "netlib/log/logging.h"

namespace netlib {

std::unique_ptr<Channel> Channel::Create(EventLoop& loop, SocketHandle fd,
                                         ChannelInterest interest,
                                         ChannelCallbacks callbacks) {
  if (fd == kInvalidSocket || (!interest.read && !interest.write)) {
    NETLIB_LOG_WARN(
        "channel creation rejected for fd={} without valid interest", fd);
    return nullptr;
  }

  auto channel = std::unique_ptr<Channel>(
      new Channel(loop, fd, interest, std::move(callbacks)));
  if (!loop.AddChannel(*channel)) {
    return nullptr;
  }
  channel->registered_ = true;
  NETLIB_LOG_DEBUG("channel registered fd={}", fd);
  return channel;
}

Channel::Channel(EventLoop& loop, SocketHandle fd, ChannelInterest interest,
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
    NETLIB_LOG_WARN("channel interest rejected for fd={}", fd_);
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

SocketHandle Channel::fd() const { return fd_; }

}  // namespace netlib
