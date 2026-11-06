#ifndef NETLIB_INCLUDE_NETLIB_CHANNEL_H_
#define NETLIB_INCLUDE_NETLIB_CHANNEL_H_

#include <cstdint>
#include <functional>
#include <memory>

namespace netlib {

class EventLoop;

// Describes the events a Channel wants from its EventLoop.
struct ChannelInterest {
  bool read = false;
  bool write = false;
};

// Callbacks run on the EventLoop thread. A Channel invokes at most one callback
// for an epoll event, so a readable callback may safely close the connection.
struct ChannelCallbacks {
  std::function<void()> on_readable;
  std::function<void()> on_writable;
  std::function<void()> on_error;
};

// Registers a file descriptor with an EventLoop. The descriptor and EventLoop
// are non-owning dependencies and must both outlive the Channel.
class Channel {
 public:
  static std::unique_ptr<Channel> Create(EventLoop& loop, int fd,
                                         ChannelInterest interest,
                                         ChannelCallbacks callbacks);

  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;
  Channel(Channel&&) = delete;
  Channel& operator=(Channel&&) = delete;
  ~Channel();

  bool SetInterest(ChannelInterest interest);
  int fd() const;

 private:
  friend class EventLoop;

  Channel(EventLoop& loop, int fd, ChannelInterest interest,
          ChannelCallbacks callbacks);

  void Dispatch(uint32_t events);

  EventLoop& loop_;
  int fd_;
  ChannelInterest interest_;
  ChannelCallbacks callbacks_;
  bool registered_ = false;
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_CHANNEL_H_
