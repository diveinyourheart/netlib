#ifndef NETLIB_INCLUDE_NETLIB_EVENT_LOOP_H_
#define NETLIB_INCLUDE_NETLIB_EVENT_LOOP_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <unordered_map>

#include "netlib/log/logger_fwd.h"
#include "netlib/unique_fd.h"

namespace netlib {

class Channel;

// Runs epoll events on the calling thread. Run() must be called by one thread;
// Stop() is thread-safe.
class EventLoop {
 public:
  // logger is optional and must outlive the EventLoop when non-null.
  static std::unique_ptr<EventLoop> Create(log::Logger* logger = nullptr);

  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;
  EventLoop(EventLoop&&) = delete;
  EventLoop& operator=(EventLoop&&) = delete;
  ~EventLoop();

  void Run();
  void Stop();

 private:
  friend class Channel;

  EventLoop(UniqueFd epoll_fd, UniqueFd stop_event_fd, log::Logger* logger);

  bool AddChannel(Channel& channel);
  void RemoveChannel(Channel& channel);
  bool ModifyChannel(Channel& channel);
  void Dispatch(int fd, uint32_t events);
  void DrainStopEvent();
  void WakeForStop();

  UniqueFd epoll_fd_;
  UniqueFd stop_event_fd_;
  std::unique_ptr<Channel> stop_channel_;
  std::unordered_map<int, Channel*> channels_;
  std::atomic<bool> stopping_{false};
  log::Logger* logger_;
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_EVENT_LOOP_H_
