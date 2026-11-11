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
#include <optional>
#include <unordered_map>
#include <vector>

#include "netlib/channel.h"
#include "netlib/event_loop.h"
#include "netlib/protocol_codec.h"
#include "netlib/protocol_session.h"
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
  // Idle time while encoded socket output remains unsent (0 < timeout <= 24h).
  std::chrono::milliseconds write_timeout = std::chrono::seconds(30);
  // Fixed wait for peer EOF after graceful TCP write shutdown (0 < time <=
  // 24h).
  std::chrono::milliseconds close_timeout = std::chrono::seconds(5);
  std::size_t max_queued_response_bytes = 64 * 1024 * 1024;
  std::size_t max_read_bytes_per_turn = 64 * 1024;
  std::size_t max_write_bytes_per_turn = 64 * 1024;
  std::size_t max_decode_calls_per_turn = 64;
  std::size_t max_response_commands_per_turn = 64;
};

// Owns accepted connections, protocol sessions, and nonblocking read/write
// state.
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

  // Native stateful protocols can emit control output independently of Workers.
  // The factory is called during validation and once per accepted connection.
  static std::unique_ptr<SubReactor> CreateWithSession(
      SubReactorOptions options, ThreadPool& worker_pool,
      ConnectionCallbacks callbacks, ProtocolSessionFactory session_factory);

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
  // Thread-safe: stop adoption, drain accepted connections, then stop the loop.
  // Repeated calls may shorten but never extend the deadline (0 < timeout <=
  // 24h).
  bool Drain(std::chrono::milliseconds timeout = std::chrono::seconds(30));

 private:
  class Connection;
  class ResponseQueue;

  SubReactor(std::unique_ptr<EventLoop> event_loop,
             std::shared_ptr<platform::Wakeup> wake_fd,
             SubReactorOptions options, ThreadPool& worker_pool,
             ProtocolSessionFactory session_factory,
             ConnectionCallbacks callbacks);

  void HandleWake();
  bool DrainWake();
  bool Wake();
  void RegisterConnection(UniqueFd connection);
  void HandleReadable(std::uint64_t connection_id);
  void HandleWritable(std::uint64_t connection_id);
  bool ProcessInput(Connection& connection, std::size_t& calls);
  void ContinueConnection(std::uint64_t connection_id, bool read, bool write);
  void HandleApplicationResponse(std::uint64_t connection_id,
                                 std::uint64_t sequence,
                                 ApplicationResponse response,
                                 bool close_after_write);
  void HandleUnorderedResponse(std::uint64_t connection_id,
                               std::uint64_t sequence,
                               ApplicationResponse response,
                               bool close_after_write);
  void HandleResponse(std::uint64_t connection_id, std::uint64_t sequence,
                      ApplicationResponse response, bool close_after_write,
                      SessionResponseMode mode);
  void HandleRequestTimeout(std::uint64_t connection_id,
                            std::uint64_t sequence);
  void FlushConnection(std::uint64_t connection_id);
  void ShutdownConnection(std::uint64_t connection_id);
  void DrainConnectionInput(std::uint64_t connection_id);
  void CancelRequestTimers();
  void CloseConnection(std::uint64_t connection_id);
  void ApplyDrain();
  void RefreshSessionTimer(std::uint64_t connection_id);
  void HandleSessionDeadline(std::uint64_t connection_id,
                             std::uint64_t generation);
  void HandleDrainDeadline(std::uint64_t generation);

  std::unique_ptr<EventLoop> event_loop_;
  ThreadPool::Submitter request_submitter_;
  ProtocolSessionFactory session_factory_;
  ConnectionCallbacks callbacks_;
  std::unordered_map<std::uint64_t, std::unique_ptr<Connection>> connections_;
  std::shared_ptr<platform::Wakeup> wake_fd_;
  std::unique_ptr<Channel> wake_channel_;
  std::shared_ptr<ResponseQueue> response_queue_;
  const SubReactorOptions options_;
  std::mutex pending_mutex_;
  std::deque<UniqueFd> pending_connections_;
  std::atomic<bool> stopping_{false};
  std::atomic<bool> draining_{false};
  std::optional<TimerScheduler::Clock::time_point> requested_drain_deadline_;
  std::optional<TimerScheduler::Clock::time_point> drain_deadline_;
  TimerScheduler::TimerId drain_timer_ = TimerScheduler::kInvalidTimer;
  std::uint64_t drain_generation_ = 0;
  bool response_continuation_ = false;
  std::uint64_t response_turn_ = 0;
  std::size_t response_commands_ = 0;
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_SUB_REACTOR_H_
