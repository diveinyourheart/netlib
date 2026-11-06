#ifndef NETLIB_INCLUDE_NETLIB_EVENT_LOOP_H_
#define NETLIB_INCLUDE_NETLIB_EVENT_LOOP_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <unordered_map>

#include "netlib/unique_fd.h"

namespace netlib {

class Channel;

// Runs epoll events on the calling thread. Run() must be called by one thread;
// Stop() is thread-safe.
class EventLoop {
 public:
  static std::unique_ptr<EventLoop> Create();

  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;
  EventLoop(EventLoop&&) = delete;
  EventLoop& operator=(EventLoop&&) = delete;
  ~EventLoop();

  void Run();
  void Stop();

 private:
  friend class Channel;

  EventLoop(UniqueFd epoll_fd, UniqueFd wake_fd);

  bool AddChannel(Channel& channel);
  void RemoveChannel(Channel& channel);
  bool ModifyChannel(Channel& channel);
  void Dispatch(int fd, uint32_t events);
  void DrainWake();
  void Wake();

  UniqueFd epoll_fd_;
  UniqueFd wake_fd_;
  std::unordered_map<int, Channel*> channels_;
  std::atomic<bool> stopping_{false};
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_EVENT_LOOP_H_
