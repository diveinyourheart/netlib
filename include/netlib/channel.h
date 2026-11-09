#ifndef NETLIB_INCLUDE_NETLIB_CHANNEL_H_
#define NETLIB_INCLUDE_NETLIB_CHANNEL_H_

#include <cstdint>
#include <functional>
#include <memory>

#include "netlib/platform.h"

namespace netlib {

class EventLoop;

// Describes the events a Channel wants from its EventLoop.
struct ChannelInterest {
  bool read = false;
  bool write = false;
};

// Callbacks run on the EventLoop thread. EventLoop checks registration
// before each callback; removal cancels remaining readiness flags.
struct ChannelCallbacks {
  std::function<void()> on_readable;
  std::function<void()> on_writable;
  std::function<void()> on_error;
};

// Registers a file descriptor with an EventLoop. The descriptor and EventLoop
// are non-owning dependencies and must both outlive the Channel.
class Channel {
 public:
  static std::unique_ptr<Channel> Create(EventLoop& loop, SocketHandle fd,
                                         ChannelInterest interest,
                                         ChannelCallbacks callbacks);

  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;
  Channel(Channel&&) = delete;
  Channel& operator=(Channel&&) = delete;
  ~Channel();

  // Empty interests pause registration; a later nonempty update registers
  // again with a fresh event token. Call only from the owning loop thread.
  bool SetInterest(ChannelInterest interest);
  SocketHandle fd() const;

 private:
  friend class EventLoop;

  Channel(EventLoop& loop, SocketHandle fd, ChannelInterest interest,
          ChannelCallbacks callbacks);

  std::uint64_t token_ = 0;

  EventLoop& loop_;
  SocketHandle fd_;
  ChannelInterest interest_;
  ChannelCallbacks callbacks_;
  bool registered_ = false;
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_CHANNEL_H_
