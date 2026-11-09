#ifndef NETLIB_INCLUDE_NETLIB_SUB_REACTOR_H_
#define NETLIB_INCLUDE_NETLIB_SUB_REACTOR_H_

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "netlib/channel.h"
#include "netlib/event_loop.h"
#include "netlib/protocol_codec.h"
#include "netlib/thread_pool.h"
#include "netlib/timer.h"
#include "netlib/unique_fd.h"

namespace netlib {

struct SubReactorOptions {
  // Maximum accepted descriptors waiting for registration.
  std::size_t max_pending_connections = 1024;
  // Bound for the global worker-response queue, each connection's reorder
  // buffer, and its in-flight request timers.
  std::size_t max_pending_responses = 4096;
  // Maximum encoded bytes buffered for one slow connection.
  std::size_t max_pending_output_bytes =
      kFrameLengthPrefixSize + kMaxFramePayloadSize;
  // Maximum time from Worker submission to an accepted response.
  std::chrono::milliseconds request_timeout = std::chrono::minutes(10);
};

// Owns accepted connections, protocol codecs, and nonblocking read/write state.
class SubReactor {
 public:
  struct ConnectionCallbacks {
    std::function<void(SocketHandle)> on_connected;
    std::function<void(SocketHandle)> on_closed;
  };

  // Compatibility overload with the same capacity for connections and
  // responses. The ThreadPool must outlive SubReactor.
  static std::unique_ptr<SubReactor> Create(std::size_t max_pending_connections,
                                            ThreadPool& worker_pool,
                                            ConnectionCallbacks callbacks);
  static std::unique_ptr<SubReactor> Create(std::size_t max_pending_connections,
                                            ThreadPool& worker_pool,
                                            ConnectionCallbacks callbacks,
                                            ProtocolCodecFactory codec_factory);
  static std::unique_ptr<SubReactor> Create(SubReactorOptions options,
                                            ThreadPool& worker_pool,
                                            ConnectionCallbacks callbacks);
  static std::unique_ptr<SubReactor> Create(SubReactorOptions options,
                                            ThreadPool& worker_pool,
                                            ConnectionCallbacks callbacks,
                                            ProtocolCodecFactory codec_factory);

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
  class ResponseQueue;

  SubReactor(std::unique_ptr<EventLoop> event_loop,
             std::shared_ptr<platform::Wakeup> wake_fd,
             SubReactorOptions options, ThreadPool& worker_pool,
             ProtocolCodecFactory codec_factory, ConnectionCallbacks callbacks);

  void HandleWake();
  bool DrainWake();
  bool Wake();
  void RegisterConnection(UniqueFd connection);
  void HandleReadable(std::uint64_t connection_id);
  void HandleWritable(std::uint64_t connection_id);
  bool ProcessFrames(Connection& connection);
  void HandleApplicationResponse(std::uint64_t connection_id,
                                 std::uint64_t sequence,
                                 ApplicationResponse response,
                                 bool close_after_write);
  void HandleRequestTimeout(std::uint64_t connection_id,
                            std::uint64_t sequence);
  void FlushConnection(std::uint64_t connection_id);
  void CloseConnection(std::uint64_t connection_id);

  std::unique_ptr<EventLoop> event_loop_;
  ThreadPool::Submitter request_submitter_;
  ProtocolCodecFactory codec_factory_;
  ConnectionCallbacks callbacks_;
  std::unordered_map<std::uint64_t, std::unique_ptr<Connection>> connections_;
  std::shared_ptr<platform::Wakeup> wake_fd_;
  std::unique_ptr<Channel> wake_channel_;
  std::shared_ptr<ResponseQueue> response_queue_;
  const SubReactorOptions options_;
  TimerScheduler request_timer_;
  std::mutex pending_mutex_;
  std::deque<UniqueFd> pending_connections_;
  std::atomic<bool> stopping_{false};
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_SUB_REACTOR_H_
