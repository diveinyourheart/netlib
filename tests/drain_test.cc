#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <stop_token>
#include <thread>
#include <utility>
#include <vector>

#include "netlib/frame_message.h"
#include "netlib/main_reactor.h"
#include "netlib/platform.h"
#include "netlib/sub_reactor.h"
#include "netlib/thread_pool.h"
#include "netlib/timer.h"
#include "netlib/unique_fd.h"

namespace {

// A real Codec Session with small output slices. This exercises socket stalls
// without depending on Winsock accepting one oversized send into kernel memory.
class ChunkedSession final : public netlib::ProtocolSession {
 public:
  explicit ChunkedSession(netlib::ProtocolSessionOptions limits,
                          bool half_close)
      : codec_(netlib::CreateCodecSession(netlib::CreateLengthPrefixedCodec(),
                                          limits)),
        half_close_(half_close) {}
  netlib::SessionInputResult OnInput(std::span<const char> input) override {
    return codec_->OnInput(input);
  }
  netlib::SessionResponseStatus OnResponse(
      const netlib::RequestContext& context,
      netlib::ApplicationResponse response, netlib::SessionResponseMode mode,
      bool close) override {
    return codec_->OnResponse(context, std::move(response), mode, close);
  }
  netlib::SessionResponseStatus OnTimeout(
      const netlib::RequestContext& context) override {
    return codec_->OnTimeout(context);
  }
  void BeginDrain() override { codec_->BeginDrain(); }
  bool ProduceOutput(std::size_t budget) override {
    return codec_->ProduceOutput(budget);
  }
  std::span<const char> PendingOutput() const override {
    std::span<const char> output = codec_->PendingOutput();
    return output.first(std::min(output.size(), std::size_t{1024}));
  }
  void ConsumeOutput(std::size_t size) override { codec_->ConsumeOutput(size); }
  bool WantsInput() const override { return codec_->WantsInput(); }
  bool ShouldClose() const override { return codec_->ShouldClose(); }
  bool ShouldHalfClose() const override {
    return half_close_ && codec_->ShouldClose();
  }
  std::size_t MaxInputBufferSize() const override {
    return codec_->MaxInputBufferSize();
  }

 private:
  std::unique_ptr<netlib::ProtocolSession> codec_;
  const bool half_close_;
};

class Server {
 public:
  ~Server() {
    if (main) main->Stop();
    if (sub) sub->Stop();
    if (main_thread.joinable()) main_thread.join();
    if (sub_thread.joinable()) sub_thread.join();
    if (workers) workers->Stop();
  }
  bool Start(netlib::SubReactorOptions options = {}, int send_buffer = 0,
             bool half_close = false) {
    workers =
        netlib::ThreadPool::Create(1, 8, [this](netlib::FrameMessage message) {
          std::lock_guard<std::mutex> lock(mutex);
          requests.push_back(std::move(message));
          condition.notify_all();
        });
    if (!workers) return false;
    netlib::SubReactor::ConnectionCallbacks callbacks;
    callbacks.on_connected = [this, send_buffer](netlib::SocketHandle socket) {
      if (send_buffer != 0)
        buffer_configured.store(
            netlib::platform::SetSendBuffer(socket, send_buffer));
    };
    callbacks.on_closed = [this](netlib::SocketHandle) {
      std::lock_guard<std::mutex> lock(mutex);
      closed = true;
      condition.notify_all();
    };
    sub = netlib::SubReactor::CreateWithSession(
        options, *workers, std::move(callbacks),
        [half_close](netlib::ProtocolSessionOptions limits) {
          return std::make_unique<ChunkedSession>(limits, half_close);
        });
    if (!sub) return false;
    main = netlib::MainReactor::Create(0, {sub.get()});
    if (!main) return false;
    sub_thread = std::thread([this] {
      sub->Run();
      std::lock_guard<std::mutex> lock(mutex);
      stopped = true;
      condition.notify_all();
    });
    main_thread = std::thread([this] { main->Run(); });
    socket.Reset(netlib::platform::CreateTcpSocket(false));
    if (!socket || !netlib::platform::SetReceiveBuffer(socket.get(), 1024) ||
        !netlib::platform::SetReceiveTimeout(socket.get(), 2000) ||
        !netlib::platform::ConnectLoopback(socket.get(), main->port()))
      return false;
    const std::array<char, 5> frame{0, 0, 0, 1, 'q'};
    if (netlib::platform::Send(socket.get(), frame.data(), frame.size()) != 5)
      return false;
    std::unique_lock<std::mutex> lock(mutex);
    return condition.wait_for(lock, std::chrono::seconds(2), [this] {
      return requests.size() == 1;
    }) && buffer_configured.load();
  }
  netlib::FrameMessage Request() {
    std::lock_guard<std::mutex> lock(mutex);
    return requests[0];
  }
  bool WaitStopped() {
    std::unique_lock<std::mutex> lock(mutex);
    return condition.wait_for(lock, std::chrono::seconds(2),
                              [this] { return stopped; });
  }
  bool WaitClosed() {
    std::unique_lock<std::mutex> lock(mutex);
    return condition.wait_for(lock, std::chrono::seconds(2),
                              [this] { return closed; });
  }

  std::unique_ptr<netlib::ThreadPool> workers;
  std::unique_ptr<netlib::SubReactor> sub;
  std::unique_ptr<netlib::MainReactor> main;
  std::thread main_thread;
  std::thread sub_thread;
  netlib::UniqueFd socket;

 private:
  std::mutex mutex;
  std::condition_variable condition;
  std::vector<netlib::FrameMessage> requests;
  bool stopped = false;
  bool closed = false;
  std::atomic<bool> buffer_configured{true};
};

bool TestGracefulDrain() {
  Server server;
  if (!server.Start()) return false;
  netlib::FrameMessage request = server.Request();
  if (server.main->Drain(std::chrono::milliseconds::zero()) ||
      !server.main->Drain(std::chrono::seconds(2)))
    return false;
  netlib::UniqueFd candidate(netlib::platform::CreateTcpSocket());
  if (server.sub->AdoptConnection(candidate) || !candidate ||
      !request.Respond(std::vector<char>{'o', 'k'}))
    return false;
  std::array<char, 6> response{};
  std::size_t offset = 0;
  while (offset < response.size()) {
    std::ptrdiff_t count =
        netlib::platform::Receive(server.socket.get(), response.data() + offset,
                                  response.size() - offset);
    if (count <= 0) return false;
    offset += static_cast<std::size_t>(count);
  }
  return response == std::array<char, 6>{0, 0, 0, 2, 'o', 'k'} &&
         server.WaitStopped() && !request.IsCancelled() &&
         !request.connection.valid();
}

enum class HalfCloseFinish {
  kPeerEof,
  kTimeout,
  kDrainDeadline,
  kStop,
  kByteLimit
};

bool TestHalfClose(HalfCloseFinish finish) {
  Server server;
  netlib::SubReactorOptions options{.close_timeout =
                                        finish == HalfCloseFinish::kTimeout
                                            ? std::chrono::milliseconds(80)
                                            : std::chrono::milliseconds(5000)};
  if (!server.Start(options, 0, true)) return false;
  netlib::FrameMessage request = server.Request();
  std::chrono::milliseconds drain_timeout =
      finish == HalfCloseFinish::kDrainDeadline
          ? std::chrono::milliseconds(80)
          : std::chrono::milliseconds(5000);
  if (!server.main->Drain(drain_timeout) ||
      !request.Respond(std::vector<char>{'o', 'k'}))
    return false;
  std::array<char, 7> response{};
  std::size_t size = 0;
  while (size != response.size()) {
    std::ptrdiff_t count = netlib::platform::Receive(
        server.socket.get(), response.data() + size, response.size() - size);
    if (count < 0) return false;
    if (count == 0) break;
    size += static_cast<std::size_t>(count);
  }
  // The final recv must observe FIN, with every response byte before it.
  if (size != 6 || response != std::array<char, 7>{0, 0, 0, 2, 'o', 'k', 0} ||
      request.connection.valid())
    return false;
  if (finish == HalfCloseFinish::kPeerEof ||
      finish == HalfCloseFinish::kByteLimit) {
    // Trailing bytes must be discarded rather than decoded as new requests.
    std::vector<char> trailing(
        finish == HalfCloseFinish::kByteLimit ? 64 * 1024 : 37, 'x');
    std::size_t offset = 0;
    while (offset < trailing.size()) {
      std::ptrdiff_t count =
          netlib::platform::Send(server.socket.get(), trailing.data() + offset,
                                 trailing.size() - offset);
      if (count <= 0) return false;
      offset += static_cast<std::size_t>(count);
    }
    if (finish == HalfCloseFinish::kPeerEof &&
        !netlib::platform::ShutdownWrite(server.socket.get()))
      return false;
  } else if (finish == HalfCloseFinish::kStop) {
    server.sub->Stop();
  }
  return server.WaitStopped() && !request.IsCancelled();
}

bool TestForcedDrainAndStop(bool drain) {
  Server server;
  if (!server.Start()) return false;
  netlib::FrameMessage request = server.Request();
  std::atomic<int> notifications{0};
  const std::thread::id owner = server.sub_thread.get_id();
  std::atomic<bool> correct_thread{false};
  std::stop_callback callback(request.stop_token, [&] {
    correct_thread.store(std::this_thread::get_id() == owner);
    notifications.fetch_add(1);
  });
  if (drain) {
    if (!server.main->Drain(std::chrono::milliseconds(80)) ||
        !server.sub->Drain(std::chrono::seconds(4)))
      return false;
  } else {
    server.sub->Stop();
    server.main->Stop();
  }
  return server.WaitStopped() && request.IsCancelled() &&
         !request.Respond(std::vector<char>{'l'}) &&
         !request.connection.valid() && notifications.load() == 1 &&
         correct_thread.load();
}

bool TestSocketWriteDeadline() {
  Server server;
  netlib::SubReactorOptions options{.write_timeout =
                                        std::chrono::milliseconds(100)};
  if (!server.Start(options, 1024)) return false;
  netlib::FrameMessage request = server.Request();
  if (!request.Respond(std::vector<char>(netlib::kMaxFramePayloadSize, 'w')))
    return false;
  return server.WaitClosed() && !request.connection.valid();
}

bool TestLateResponseBeforeDelayedTimeoutEvent() {
  struct Gate {
    std::mutex mutex;
    std::condition_variable condition;
    bool entered = false;
    bool release = false;
  };
  struct ReleaseGate {
    std::shared_ptr<Gate> gate;
    ~ReleaseGate() {
      std::lock_guard<std::mutex> lock(gate->mutex);
      gate->release = true;
      gate->condition.notify_all();
    }
  };
  std::shared_ptr<Gate> gate = std::make_shared<Gate>();
  ReleaseGate release{gate};
  // Deliberately delay delivery of timer controls. An expired business reply
  // arriving from a different producer must not win just because it dequeues
  // before the timer event. The owner checks its stored absolute deadline.
  netlib::TimerScheduler::TimerId blocker =
      netlib::TimerScheduler::ScheduleAfter(
          std::chrono::milliseconds(1), [gate] {
            std::unique_lock<std::mutex> lock(gate->mutex);
            gate->entered = true;
            gate->condition.notify_all();
            gate->condition.wait(lock, [&] { return gate->release; });
          });
  if (blocker == netlib::TimerScheduler::kInvalidTimer) return false;
  {
    std::unique_lock<std::mutex> lock(gate->mutex);
    if (!gate->condition.wait_for(lock, std::chrono::seconds(2),
                                  [&] { return gate->entered; }))
      return false;
  }
  Server server;
  if (!server.Start({.request_timeout = std::chrono::milliseconds(30)}))
    return false;
  netlib::FrameMessage request = server.Request();
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  bool enqueued =
      request.connection.Respond(request.sequence, std::vector<char>{'l'});
  return enqueued && server.WaitClosed() && request.IsCancelled();
}

}  // namespace

int main() {
  if (!TestGracefulDrain()) {
    std::fprintf(stderr, "graceful drain failed\n");
    return 1;
  }
  for (HalfCloseFinish finish :
       {HalfCloseFinish::kPeerEof, HalfCloseFinish::kTimeout,
        HalfCloseFinish::kDrainDeadline, HalfCloseFinish::kStop,
        HalfCloseFinish::kByteLimit}) {
    if (!TestHalfClose(finish)) {
      std::fprintf(stderr, "TCP half-close failed: finish=%d\n",
                   static_cast<int>(finish));
      return 1;
    }
  }
  if (!TestForcedDrainAndStop(true)) {
    std::fprintf(stderr, "forced drain/cancellation failed\n");
    return 1;
  }
  if (!TestForcedDrainAndStop(false)) {
    std::fprintf(stderr, "immediate stop/cancellation failed\n");
    return 1;
  }
  if (!TestSocketWriteDeadline()) {
    std::fprintf(stderr, "socket write deadline failed\n");
    return 1;
  }
  if (!TestLateResponseBeforeDelayedTimeoutEvent()) {
    std::fprintf(stderr, "late response revived an expired request\n");
    return 1;
  }
  return 0;
}
