#ifndef NETLIB_INCLUDE_NETLIB_SUB_REACTOR_H_
#define NETLIB_INCLUDE_NETLIB_SUB_REACTOR_H_

#include <atomic>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string_view>
#include <unordered_map>

#include "netlib/channel.h"
#include "netlib/event_loop.h"
#include "netlib/unique_fd.h"

namespace netlib {

// Owns accepted connections and all subsequent nonblocking reads.
class SubReactor {
 public:
  // The data view is valid only for the duration of on_data.
  struct ConnectionCallbacks {
    std::function<void(int)> on_connected;
    std::function<void(int, std::string_view)> on_data;
    std::function<void(int)> on_closed;
  };

  // Pending connections are bounded; a capacity of zero is invalid.
  static std::unique_ptr<SubReactor> Create(std::size_t max_pending_connections,
                                            ConnectionCallbacks callbacks);

  SubReactor(const SubReactor&) = delete;
  SubReactor& operator=(const SubReactor&) = delete;
  SubReactor(SubReactor&&) = delete;
  SubReactor& operator=(SubReactor&&) = delete;
  ~SubReactor();

  // Thread-safe. The descriptor must otherwise be exclusively owned by the
  // caller. False means it was not queued and will be closed on return.
  bool AdoptConnection(UniqueFd connection);

  void Run();
  void Stop();

 private:
  struct Connection {
    UniqueFd socket;
    std::unique_ptr<Channel> channel;
  };

  SubReactor(std::unique_ptr<EventLoop> event_loop, UniqueFd wake_fd,
             std::size_t max_pending_connections,
             ConnectionCallbacks callbacks);

  void HandleWake();
  bool DrainWake();
  bool Wake();
  void RegisterConnection(UniqueFd connection);
  void HandleReadable(int fd);
  void CloseConnection(int fd);

  std::unique_ptr<EventLoop> event_loop_;
  ConnectionCallbacks callbacks_;
  std::unordered_map<int, Connection> connections_;
  UniqueFd wake_fd_;
  std::unique_ptr<Channel> wake_channel_;
  const std::size_t max_pending_connections_;
  std::mutex pending_mutex_;
  std::deque<UniqueFd> pending_connections_;
  std::atomic<bool> stopping_{false};
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_SUB_REACTOR_H_
