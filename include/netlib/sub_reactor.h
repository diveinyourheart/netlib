#ifndef NETLIB_INCLUDE_NETLIB_SUB_REACTOR_H_
#define NETLIB_INCLUDE_NETLIB_SUB_REACTOR_H_

#include <atomic>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "netlib/channel.h"
#include "netlib/event_loop.h"
#include "netlib/thread_pool.h"
#include "netlib/unique_fd.h"

namespace netlib {

// Owns accepted connections and all subsequent nonblocking reads.
class SubReactor {
 public:
  struct ConnectionCallbacks {
    std::function<void(int)> on_connected;
    std::function<void(int)> on_closed;
  };

  // Pending connections are bounded; a capacity of zero is invalid. The
  // ThreadPool must outlive this SubReactor.
  static std::unique_ptr<SubReactor> Create(std::size_t max_pending_connections,
                                            ThreadPool& worker_pool,
                                            ConnectionCallbacks callbacks);

  SubReactor(const SubReactor&) = delete;
  SubReactor& operator=(const SubReactor&) = delete;
  SubReactor(SubReactor&&) = delete;
  SubReactor& operator=(SubReactor&&) = delete;
  ~SubReactor();

  // Thread-safe. Ownership moves only when true is returned. On false, the
  // caller retains the descriptor and may try another SubReactor.
  bool AdoptConnection(UniqueFd& connection);

  void Run();
  void Stop();

 private:
  class Connection;

  SubReactor(std::unique_ptr<EventLoop> event_loop, UniqueFd wake_fd,
             std::size_t max_pending_connections, ThreadPool& worker_pool,
             ConnectionCallbacks callbacks);

  void HandleWake();
  bool DrainWake();
  bool Wake();
  void RegisterConnection(UniqueFd connection);
  void HandleReadable(int fd);
  bool ProcessFrames(Connection& connection);
  void CloseConnection(int fd);

  std::unique_ptr<EventLoop> event_loop_;
  ThreadPool& worker_pool_;
  ConnectionCallbacks callbacks_;
  std::unordered_map<int, std::unique_ptr<Connection>> connections_;
  UniqueFd wake_fd_;
  std::unique_ptr<Channel> wake_channel_;
  const std::size_t max_pending_connections_;
  std::mutex pending_mutex_;
  std::deque<UniqueFd> pending_connections_;
  std::atomic<bool> stopping_{false};
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_SUB_REACTOR_H_
