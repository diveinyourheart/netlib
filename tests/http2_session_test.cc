#include "netlib/http2_session.h"

#include <nghttp2/nghttp2.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "netlib/frame_message.h"
#include "netlib/main_reactor.h"
#include "netlib/platform.h"
#include "netlib/sub_reactor.h"
#include "netlib/thread_pool.h"
#include "netlib/unique_fd.h"

namespace {

bool Check(bool condition, const char* message) {
  if (!condition) std::fprintf(stderr, "%s\n", message);
  return condition;
}

class Client {
 public:
  struct Reply {
    std::string status;
    std::string body;
    bool complete = false;
    std::uint32_t reset = 0;
  };
  struct Body {
    std::string bytes;
    std::size_t offset = 0;
    std::size_t allowance = 0;
  };
  ~Client() { nghttp2_session_del(session); }
  bool Initialize(std::uint32_t window = 65535) {
    nghttp2_session_callbacks* callbacks = nullptr;
    if (nghttp2_session_callbacks_new(&callbacks) != 0) return false;
    nghttp2_session_callbacks_set_on_header_callback(callbacks, Header);
    nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, Frame);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks, Data);
    nghttp2_option* option = nullptr;
    if (nghttp2_option_new(&option) != 0) {
      nghttp2_session_callbacks_del(callbacks);
      return false;
    }
    nghttp2_option_set_no_auto_window_update(option, 1);
    int result = nghttp2_session_client_new2(&session, callbacks, this, option);
    nghttp2_option_del(option);
    nghttp2_session_callbacks_del(callbacks);
    nghttp2_settings_entry settings_entry{NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE,
                                          window};
    return result == 0 && nghttp2_submit_settings(session, NGHTTP2_FLAG_NONE,
                                                  &settings_entry, 1) == 0;
  }
  std::int32_t Request(std::string path, std::string body = {},
                       bool defer = false, std::string large_header = {},
                       std::string method = "POST") {
    std::vector<std::pair<std::string, std::string>> fields{
        {":method", std::move(method)},
        {":scheme", "http"},
        {":authority", "localhost"},
        {":path", std::move(path)}};
    if (!large_header.empty())
      fields.emplace_back("x-large", std::move(large_header));
    std::vector<nghttp2_nv> headers;
    for (std::pair<std::string, std::string>& field : fields) {
      headers.push_back({reinterpret_cast<std::uint8_t*>(field.first.data()),
                         reinterpret_cast<std::uint8_t*>(field.second.data()),
                         field.first.size(), field.second.size(),
                         NGHTTP2_NV_FLAG_NONE});
    }
    std::unique_ptr<Body> source = std::make_unique<Body>();
    source->bytes = std::move(body);
    source->allowance = defer ? 0 : source->bytes.size();
    nghttp2_data_provider2 provider{.source = {.ptr = source.get()},
                                    .read_callback = Read};
    std::int32_t id = nghttp2_submit_request2(
        session, nullptr, headers.data(), headers.size(),
        source->bytes.empty() ? nullptr : &provider, nullptr);
    if (id > 0) bodies.emplace(id, std::move(source));
    return id;
  }
  bool Allow(std::int32_t id, std::size_t count) {
    bodies.at(id)->allowance += count;
    return nghttp2_session_resume_data(session, id) == 0;
  }
  bool TakeOutput(std::vector<char>* output) {
    output->clear();
    for (int calls = 0; calls != 1024; ++calls) {
      const std::uint8_t* bytes = nullptr;
      nghttp2_ssize size = nghttp2_session_mem_send2(session, &bytes);
      if (size < 0) return false;
      if (size == 0) return true;
      output->insert(output->end(), bytes, bytes + size);
    }
    return false;
  }
  bool Receive(std::span<const char> bytes) {
    nghttp2_ssize consumed = nghttp2_session_mem_recv2(
        session, reinterpret_cast<const std::uint8_t*>(bytes.data()),
        bytes.size());
    if (consumed != static_cast<nghttp2_ssize>(bytes.size())) {
      std::fprintf(stderr, "client decode consumed=%td expected=%zu\n",
                   static_cast<std::ptrdiff_t>(consumed), bytes.size());
      return false;
    }
    return true;
  }

  nghttp2_session* session = nullptr;
  std::map<std::int32_t, Reply> replies;
  std::vector<std::int32_t> completion_order;
  bool auto_credit = true;
  int ping_acks = 0;
  int settings = 0;
  int goaways = 0;
  std::int32_t last_goaway_stream = -1;

 private:
  static nghttp2_ssize Read(nghttp2_session*, std::int32_t, std::uint8_t* bytes,
                            std::size_t size, std::uint32_t* flags,
                            nghttp2_data_source* source, void*) {
    Body& body = *static_cast<Body*>(source->ptr);
    if (body.allowance == 0 && body.offset != body.bytes.size()) {
      return NGHTTP2_ERR_DEFERRED;
    }
    std::size_t count =
        std::min({size, body.allowance, body.bytes.size() - body.offset});
    if (count != 0) std::memcpy(bytes, body.bytes.data() + body.offset, count);
    body.offset += count;
    body.allowance -= count;
    if (body.offset == body.bytes.size()) *flags |= NGHTTP2_DATA_FLAG_EOF;
    return static_cast<nghttp2_ssize>(count);
  }
  static int Header(nghttp2_session*, const nghttp2_frame* frame,
                    const std::uint8_t* name, std::size_t name_size,
                    const std::uint8_t* value, std::size_t value_size,
                    std::uint8_t, void* data) {
    Client& self = *static_cast<Client*>(data);
    if (std::string_view(reinterpret_cast<const char*>(name), name_size) ==
        ":status") {
      self.replies[frame->hd.stream_id].status.assign(
          reinterpret_cast<const char*>(value), value_size);
    }
    return 0;
  }
  static int Data(nghttp2_session* session, std::uint8_t, std::int32_t id,
                  const std::uint8_t* bytes, std::size_t size, void* data) {
    Client& self = *static_cast<Client*>(data);
    self.replies[id].body.append(reinterpret_cast<const char*>(bytes), size);
    return !self.auto_credit || nghttp2_session_consume(session, id, size) == 0
               ? 0
               : NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  static int Frame(nghttp2_session*, const nghttp2_frame* frame, void* data) {
    Client& self = *static_cast<Client*>(data);
    if (frame->hd.type == NGHTTP2_RST_STREAM) {
      self.replies[frame->hd.stream_id].reset = frame->rst_stream.error_code;
    }
    if (frame->hd.type == NGHTTP2_PING && (frame->hd.flags & NGHTTP2_FLAG_ACK))
      ++self.ping_acks;
    if (frame->hd.type == NGHTTP2_SETTINGS) ++self.settings;
    if (frame->hd.type == NGHTTP2_GOAWAY) {
      ++self.goaways;
      self.last_goaway_stream = frame->goaway.last_stream_id;
    }
    if ((frame->hd.type == NGHTTP2_HEADERS || frame->hd.type == NGHTTP2_DATA) &&
        (frame->hd.flags & NGHTTP2_FLAG_END_STREAM)) {
      self.replies[frame->hd.stream_id].complete = true;
      self.completion_order.push_back(frame->hd.stream_id);
    }
    return 0;
  }
  std::map<std::int32_t, std::unique_ptr<Body>> bodies;
};

struct Harness {
  Client client;
  std::unique_ptr<netlib::ProtocolSession> server;
  std::vector<netlib::SessionRequest> requests;
  std::vector<netlib::RequestContext> cancelled;
  std::size_t fragment = 17;
  std::size_t output_budget = 4096;
  bool Pump() {
    for (int round = 0; round != 1024; ++round) {
      bool progress = false;
      std::vector<char> outgoing;
      if (!client.TakeOutput(&outgoing)) return false;
      std::size_t offset = 0;
      while (offset < outgoing.size()) {
        netlib::SessionInputResult result =
            server->OnInput(std::span<const char>(outgoing).subspan(
                offset, std::min(fragment, outgoing.size() - offset)));
        if (result.status != netlib::SessionInputStatus::kProgress ||
            result.consumed_bytes == 0) {
          return Check(false, "server rejected client input");
        }
        offset += result.consumed_bytes;
        for (netlib::SessionRequest& request : result.requests)
          requests.push_back(std::move(request));
        cancelled.insert(cancelled.end(), result.cancelled_requests.begin(),
                         result.cancelled_requests.end());
        progress = true;
      }
      for (int writes = 0; writes != 200000; ++writes) {
        if (!server->ProduceOutput(output_budget))
          return Check(false, "server output failed");
        std::vector<netlib::RequestContext> resets =
            server->TakeCancelledRequests();
        cancelled.insert(cancelled.end(), resets.begin(), resets.end());
        std::span<const char> bytes = server->PendingOutput();
        if (bytes.empty()) break;
        if (!Check(bytes.size() <= output_budget, "output budget exceeded"))
          return false;
        // Simulate a socket that accepts only part of each encoded chunk.
        std::size_t count = std::min(bytes.size(), fragment);
        if (!client.Receive(bytes.first(count))) return false;
        server->ConsumeOutput(count);
        progress = true;
        if (writes == 199999) return false;
      }
      if (!progress) return true;
    }
    return false;
  }
};

netlib::HttpResponse Reply(std::string body) {
  return {.headers = {}, .body = std::vector<char>(body.begin(), body.end())};
}

bool TestMultiplexingAndControls() {
  Harness h;
  h.server = netlib::CreateHttp2Session();
  h.fragment = 1;
  h.output_budget = 7;
  if (!h.server || !h.client.Initialize() || !h.Pump() || h.client.settings < 2)
    return false;
  std::int32_t first = h.client.Request("/one", "abc", true);
  std::int32_t second = h.client.Request("/two", "def", true);
  if (first <= 0 || second <= 0 || !h.Pump() || !h.requests.empty() ||
      !h.client.Allow(second, 3) || !h.Pump() || h.requests.size() != 1 ||
      !h.client.Allow(first, 1) || !h.Pump() || h.requests.size() != 1 ||
      !h.client.Allow(first, 2) || !h.Pump() || h.requests.size() != 2)
    return false;
  const netlib::HttpRequestMetadata& metadata =
      std::get<netlib::HttpRequestMetadata>(h.requests[0].metadata);
  if (!Check(h.requests[0].context.request_id == 0 &&
                 h.requests[0].context.stream_id ==
                     static_cast<std::uint32_t>(second) &&
                 h.requests[1].context.request_id == 1 &&
                 h.requests[1].context.stream_id ==
                     static_cast<std::uint32_t>(first) &&
                 metadata.version == "HTTP/2" && metadata.target == "/two" &&
                 metadata.scheme == "http" &&
                 metadata.authority == "localhost" &&
                 h.requests[1].payload == std::vector<char>{'a', 'b', 'c'},
             "completed requests lost stream identities or owned data"))
    return false;
  if (h.server->OnResponse(h.requests[1].context, Reply("first"),
                           netlib::SessionResponseMode::kOrdered,
                           false) != netlib::SessionResponseStatus::kQueued ||
      !h.Pump() || !h.client.replies[first].complete ||
      h.client.replies[second].complete)
    return false;
  if (h.server->OnResponse(h.requests[0].context, Reply("second"),
                           netlib::SessionResponseMode::kOrdered,
                           false) != netlib::SessionResponseStatus::kQueued ||
      !h.Pump())
    return false;
  const std::array<std::uint8_t, 8> ping{1, 2, 3, 4, 5, 6, 7, 8};
  return nghttp2_submit_ping(h.client.session, NGHTTP2_FLAG_NONE,
                             ping.data()) == 0 &&
         h.Pump() && h.client.ping_acks == 1 && h.requests.size() == 2 &&
         h.client.replies[first].body == "first" &&
         h.client.replies[second].body == "second";
}

bool TestFlowControlAndLargeHeaders() {
  Harness h;
  h.server = netlib::CreateHttp2Session();
  h.fragment = 8192;
  if (!h.client.Initialize(7) || !h.Pump()) return false;
  h.client.auto_credit = false;
  std::int32_t first = h.client.Request("/large", std::string(130000, 'q'),
                                        false, std::string(40000, 'z'));
  if (first <= 0 || !h.Pump() || h.requests.size() != 1 ||
      h.requests[0].payload.size() != 130000)
    return Check(false,
                 "large incoming body deadlocked or CONTINUATION failed");
  h.server->OnResponse(h.requests[0].context, Reply(std::string(100, 'r')),
                       netlib::SessionResponseMode::kOrdered, false);
  if (!h.Pump() || h.client.replies[first].body.size() != 7 ||
      h.client.replies[first].complete || !h.server->WantsInput())
    return false;
  std::int32_t second = h.client.Request("/small");
  if (!h.Pump() || h.requests.size() != 2) return false;
  h.server->OnResponse(h.requests[1].context, Reply("ok"),
                       netlib::SessionResponseMode::kOrdered, false);
  if (!h.Pump() || !h.client.replies[second].complete) return false;
  for (int i = 0; i != 20 && !h.client.replies[first].complete; ++i) {
    if (nghttp2_session_consume(h.client.session, first, 7) != 0 || !h.Pump())
      return false;
  }
  return Check(h.client.replies[first].complete &&
                   h.client.replies[first].body == std::string(100, 'r'),
               "stream window did not resume exactly once per returned credit");
}

bool TestResetLimitsTimeoutAndDrain() {
  Harness h;
  h.server = netlib::CreateHttp2Session(
      {}, {.max_header_bytes = 512, .max_body_bytes = 32});
  if (!h.client.Initialize() || !h.Pump()) return false;
  std::int32_t large =
      h.client.Request("/headers", {}, false, std::string(1024, 'h'));
  std::int32_t body = h.client.Request("/body", std::string(33, 'b'));
  std::int32_t good = h.client.Request("/ok");
  if (!h.Pump() || h.requests.size() != 1 || !h.client.replies[large].reset ||
      !h.client.replies[body].reset)
    return Check(false, "stream limits did not isolate rejected requests");
  netlib::RequestContext context = h.requests[0].context;
  if (nghttp2_submit_rst_stream(h.client.session, NGHTTP2_FLAG_NONE, good,
                                NGHTTP2_CANCEL) != 0 ||
      !h.Pump() || h.cancelled.size() != 1 || h.cancelled[0] != context ||
      h.server->OnResponse(context, Reply("late"),
                           netlib::SessionResponseMode::kOrdered,
                           false) != netlib::SessionResponseStatus::kStale)
    return false;
  std::int32_t timeout = h.client.Request("/timeout");
  std::int32_t other = h.client.Request("/other");
  if (!h.Pump() || h.requests.size() != 3 ||
      h.server->OnTimeout(h.requests[1].context) !=
          netlib::SessionResponseStatus::kQueued ||
      !h.Pump() || h.client.replies[timeout].status != "504" ||
      h.client.replies[other].complete)
    return false;
  h.server->BeginDrain();
  h.server->OnResponse(h.requests[2].context, Reply("alive"),
                       netlib::SessionResponseMode::kOrdered, false);
  return h.Pump() && h.client.goaways == 1 &&
         h.client.replies[other].body == "alive" && h.server->ShouldClose();
}

bool SendClient(Client& client, netlib::SocketHandle socket) {
  std::vector<char> bytes;
  if (!client.TakeOutput(&bytes)) return false;
  std::size_t offset = 0;
  while (offset < bytes.size()) {
    std::ptrdiff_t size = netlib::platform::Send(socket, bytes.data() + offset,
                                                 bytes.size() - offset);
    if (size <= 0) {
      std::fprintf(stderr, "client send failed: error=%d pending=%zu\n",
                   netlib::platform::LastError(), bytes.size() - offset);
      return false;
    }
    offset += static_cast<std::size_t>(size);
  }
  return true;
}

bool RunTcpTest(bool cancel, bool timeout, bool drain = false,
                std::size_t read_chunk = 4096, std::size_t ping_count = 0) {
  std::mutex mutex;
  std::condition_variable condition;
  std::vector<netlib::FrameMessage> requests;
  bool closed = false;
  bool stopped = false;
  std::unique_ptr<netlib::ThreadPool> workers =
      netlib::ThreadPool::Create(1, 8, [&](netlib::FrameMessage message) {
        std::lock_guard<std::mutex> lock(mutex);
        requests.push_back(std::move(message));
        condition.notify_all();
      });
  if (!workers) return false;
  // One in-flight slot makes a leaked cancellation timer observable
  // immediately.
  netlib::SubReactorOptions options{
      .max_pending_responses = cancel ? 1U : 2U,
      .request_timeout = timeout ? std::chrono::milliseconds(150)
                                 : std::chrono::milliseconds(5000),
      .max_read_bytes_per_turn = 17,
      .max_write_bytes_per_turn = 7,
      .max_decode_calls_per_turn = 1,
      .max_response_commands_per_turn = 1};
  netlib::SubReactor::ConnectionCallbacks callbacks;
  callbacks.on_closed = [&](netlib::SocketHandle) {
    std::lock_guard<std::mutex> lock(mutex);
    closed = true;
    condition.notify_all();
  };
  std::unique_ptr<netlib::SubReactor> reactor =
      netlib::SubReactor::CreateWithSession(
          options, *workers, std::move(callbacks),
          [](netlib::ProtocolSessionOptions limits) {
            return netlib::CreateHttp2Session(limits);
          });
  if (!reactor) return false;
  std::unique_ptr<netlib::MainReactor> main =
      netlib::MainReactor::Create(0, {reactor.get()});
  if (!main) return false;
  std::thread reactor_thread([&] {
    reactor->Run();
    std::lock_guard<std::mutex> lock(mutex);
    stopped = true;
    condition.notify_all();
  });
  std::thread main_thread([&] { main->Run(); });
  netlib::UniqueFd socket(netlib::platform::CreateTcpSocket(false));
  Client client;
  bool success =
      socket && netlib::platform::SetReceiveTimeout(socket.get(), 2000) &&
      netlib::platform::ConnectLoopback(socket.get(), main->port()) &&
      client.Initialize();
  const std::array<std::uint8_t, 8> ping{};
  for (std::size_t i = 0; success && i < ping_count; ++i)
    success = nghttp2_submit_ping(client.session, NGHTTP2_FLAG_NONE,
                                  ping.data()) == 0;
  std::int32_t first = success ? client.Request("/cancel") : -1;
  if (!cancel && !timeout && success) success = client.Request("/two") == 3;
  success = success && first > 0 && SendClient(client, socket.get());
  netlib::FrameMessage expired;
  if (success) {
    std::unique_lock<std::mutex> lock(mutex);
    const std::size_t expected = cancel || timeout ? 1 : 2;
    success = condition.wait_for(lock, std::chrono::seconds(2),
                                 [&] { return requests.size() == expected; });
    if (success) expired = requests[0];
  }
  if (success && timeout) {
    std::array<char, 4096> received{};
    for (int i = 0; success && i != 100 && !client.replies[first].complete;
         ++i) {
      std::ptrdiff_t size = netlib::platform::Receive(
          socket.get(), received.data(), received.size());
      success = size > 0 &&
                client.Receive(std::span<const char>(received).first(
                    static_cast<std::size_t>(size))) &&
                SendClient(client, socket.get());
    }
    success = success && client.replies[first].status == "504" &&
              client.Request("/after-timeout") == 3 &&
              SendClient(client, socket.get());
  }
  // Reset and a replacement request share one TCP write. The Reactor must
  // remove the old context/timer before dispatching the new request.
  if (success && cancel) {
    success = nghttp2_submit_rst_stream(client.session, NGHTTP2_FLAG_NONE,
                                        first, NGHTTP2_CANCEL) == 0 &&
              client.Request("/replacement") == 3;
    success = success && SendClient(client, socket.get());
  }
  netlib::FrameMessage replacement;
  if (success) {
    std::unique_lock<std::mutex> lock(mutex);
    success = condition.wait_for(lock, std::chrono::seconds(2),
                                 [&] { return requests.size() == 2; });
    if (success) replacement = requests[1];
  }
  if (success && drain) success = main->Drain(std::chrono::seconds(2));
  if (success) {
    success = expired.sequence == 0 && replacement.Respond(Reply("done"));
    if (timeout)
      success =
          success && expired.IsCancelled() && !expired.Respond(Reply("late"));
    if (cancel) success = success && expired.IsCancelled();
  }
  // Hold the SETTINGS ACK until after FIN, and fragment every frame. A full
  // server close would reset TCP when this delayed control finally arrives.
  const bool delayed_ack = drain && read_chunk == 1;
  std::array<char, 4096> bytes{};
  for (int i = 0; success && i != 4096 && !client.replies[3].complete; ++i) {
    std::ptrdiff_t count =
        netlib::platform::Receive(socket.get(), bytes.data(), read_chunk);
    if (count <= 0)
      std::fprintf(stderr, "client receive failed: result=%td error=%d\n",
                   count, netlib::platform::LastError());
    success = count > 0 &&
              client.Receive(std::span<const char>(bytes).first(
                  static_cast<std::size_t>(count))) &&
              (delayed_ack || SendClient(client, socket.get()));
  }
  if (success && !cancel && !timeout) {
    // Prove stream 3 can complete while stream 1 still awaits its application.
    // Once both are ready, nghttp2 may interleave their DATA under small
    // slices.
    success = expired.Respond(Reply("first"));
    for (int i = 0; success && i != 4096 && !client.replies[first].complete;
         ++i) {
      std::ptrdiff_t count =
          netlib::platform::Receive(socket.get(), bytes.data(), read_chunk);
      if (count <= 0)
        std::fprintf(stderr, "client receive failed: result=%td error=%d\n",
                     count, netlib::platform::LastError());
      success = count > 0 &&
                client.Receive(std::span<const char>(bytes).first(
                    static_cast<std::size_t>(count))) &&
                (delayed_ack || SendClient(client, socket.get()));
    }
    success = success && client.replies[first].body == "first" &&
              !client.completion_order.empty() &&
              client.completion_order.front() == 3;
  }
  success = success && client.replies[3].complete &&
            client.replies[3].body == "done" && replacement.sequence == 1 &&
            replacement.request_context.stream_id == 3;
  success = success && client.ping_acks == static_cast<int>(ping_count);
  if (success && drain) {
    std::ptrdiff_t count =
        netlib::platform::Receive(socket.get(), bytes.data(), bytes.size());
    success = count == 0;
    if (!success)
      std::fprintf(stderr, "expected server FIN: result=%td error=%d\n", count,
                   netlib::platform::LastError());
    {
      std::lock_guard<std::mutex> lock(mutex);
      // FIN is a half-close: the descriptor remains registered for late input.
      success = success && !closed;
    }
    if (delayed_ack)
      success = success && nghttp2_session_want_write(client.session) != 0;
    success = success && SendClient(client, socket.get()) &&
              netlib::platform::ShutdownWrite(socket.get());
    std::unique_lock<std::mutex> lock(mutex);
    success = success &&
              condition.wait_for(lock, std::chrono::seconds(2),
                                 [&] { return stopped; }) &&
              closed && client.goaways == 1 && client.last_goaway_stream == 3 &&
              !expired.IsCancelled() && !replacement.IsCancelled();
  }
  if (!success) {
    std::lock_guard<std::mutex> lock(mutex);
    std::fprintf(
        stderr,
        "TCP state: cancel=%d timeout=%d drain=%d chunk=%zu "
        "requests=%zu stopped=%d goaways=%d last_stream=%d "
        "first_complete=%d second_complete=%d cancelled=%d/%d "
        "first_completed_stream=%d closed=%d\n",
        cancel, timeout, drain, read_chunk, requests.size(), stopped,
        client.goaways, client.last_goaway_stream,
        client.replies[first].complete, client.replies[3].complete,
        expired.IsCancelled(), replacement.IsCancelled(),
        client.completion_order.empty() ? -1 : client.completion_order.front(),
        closed);
  }
  socket.Reset();
  main->Stop();
  reactor->Stop();
  main_thread.join();
  reactor_thread.join();
  workers->Stop();
  return success;
}

bool TestTcpRouting() { return RunTcpTest(false, false); }
bool TestTcpCancellation() { return RunTcpTest(true, false); }
bool TestTcpTimeout() { return RunTcpTest(false, true); }
bool TestTcpDrain() { return RunTcpTest(false, false, true); }
bool TestTcpDrainDelayedAck() { return RunTcpTest(false, false, true, 1); }
bool TestTcpControlFlood() { return RunTcpTest(false, false, false, 1, 200); }

bool TestPhaseDeadlines() {
  std::unique_ptr<netlib::ProtocolSession> preface =
      netlib::CreateHttp2Session();
  std::optional<netlib::ProtocolSession::Clock::time_point> deadline =
      preface->NextDeadline();
  if (!deadline) return false;
  preface->OnDeadline(*deadline - std::chrono::nanoseconds(1));
  if (preface->ShouldClose()) return false;
  preface->OnDeadline(*deadline);
  if (!preface->ShouldClose()) return false;

  Harness partial;
  partial.server = netlib::CreateHttp2Session();
  if (!partial.client.Initialize() || !partial.Pump()) return false;
  partial.client.Request("/partial");
  std::vector<char> bytes;
  if (!partial.client.TakeOutput(&bytes) || bytes.size() < 5) return false;
  partial.server->OnInput(std::span<const char>(bytes).first(5));
  deadline = partial.server->NextDeadline();
  if (!deadline) return false;
  partial.server->OnDeadline(*deadline);
  if (!partial.server->ShouldClose())
    return Check(false, "partial frame header did not expire");

  Harness headers;
  headers.server = netlib::CreateHttp2Session();
  if (!headers.client.Initialize() || !headers.Pump()) return false;
  headers.client.Request("/continuation", {}, false, std::string(40000, 'z'));
  if (!headers.client.TakeOutput(&bytes) || bytes.size() < 9) return false;
  std::size_t first_frame_size =
      9 + (std::size_t{static_cast<unsigned char>(bytes[0])} << 16) +
      (std::size_t{static_cast<unsigned char>(bytes[1])} << 8) +
      static_cast<unsigned char>(bytes[2]);
  std::size_t offset = 0;
  while (offset < first_frame_size) {
    netlib::SessionInputResult result =
        headers.server->OnInput(std::span<const char>(bytes).subspan(
            offset, first_frame_size - offset));
    if (result.consumed_bytes == 0) return false;
    offset += result.consumed_bytes;
  }
  deadline = headers.server->NextDeadline();
  if (!deadline) return false;
  headers.server->OnDeadline(*deadline);
  if (!headers.server->ShouldClose())
    return Check(false, "unfinished HPACK block did not expire");

  Harness body;
  body.server = netlib::CreateHttp2Session();
  if (!body.client.Initialize() || !body.Pump()) return false;
  std::int32_t first = body.client.Request("/idle", "ab", true);
  if (!body.Pump()) return false;
  deadline = body.server->NextDeadline();
  if (!deadline || !body.client.Allow(first, 1) || !body.Pump()) return false;
  body.server->OnDeadline(*deadline);
  if (!body.Pump() || body.client.replies[first].reset != 0)
    return Check(false, "stale body idle alarm reset an active stream");
  deadline = body.server->NextDeadline();
  if (!deadline) return false;
  body.server->OnDeadline(*deadline);
  if (!body.Pump() || body.client.replies[first].reset == 0 ||
      body.server->ShouldClose())
    return false;

  Harness response;
  response.server = netlib::CreateHttp2Session();
  if (!response.client.Initialize(0) || !response.Pump()) return false;
  std::int32_t stream = response.client.Request("/zero-window");
  if (!response.Pump() || response.requests.size() != 1 ||
      response.server->NextDeadline())
    return false;
  response.server->OnResponse(response.requests[0].context, Reply("pending"),
                              netlib::SessionResponseMode::kOrdered, false);
  if (!response.Pump() || response.client.replies[stream].complete)
    return false;
  deadline = response.server->NextDeadline();
  if (!deadline) return false;
  response.server->OnDeadline(*deadline);
  if (!response.Pump() || response.client.replies[stream].reset == 0 ||
      response.server->ShouldClose())
    return false;
  return true;
}

bool TestDrainBoundaryAndExpiry() {
  Harness h;
  h.server = netlib::CreateHttp2Session();
  if (!h.client.Initialize() || !h.Pump()) return false;
  std::int32_t existing = h.client.Request("/accepted", "x", true);
  if (!h.Pump()) return false;
  h.server->BeginDrain();
  h.client.Request("/new-after-drain");
  if (!h.Pump() || h.client.last_goaway_stream != existing ||
      !h.requests.empty() || !h.client.Allow(existing, 1) || !h.Pump() ||
      h.requests.size() != 1)
    return false;
  h.server->OnResponse(h.requests[0].context, Reply("ok"),
                       netlib::SessionResponseMode::kOrdered, false);
  if (!h.Pump() || !h.server->ShouldClose()) return false;
  Harness pending;
  pending.server = netlib::CreateHttp2Session();
  if (!pending.client.Initialize() || !pending.Pump()) return false;
  pending.client.Request("/unfinished-work");
  if (!pending.Pump() || pending.requests.size() != 1) return false;
  pending.server->BeginDrain();
  std::optional<netlib::ProtocolSession::Clock::time_point> deadline =
      pending.server->NextDeadline();
  if (!deadline) return false;
  pending.server->OnDeadline(*deadline);
  return pending.server->ShouldClose() && !pending.server->ProduceOutput(4096);
}

bool TestTcpHandshakeDeadline() {
  std::mutex mutex;
  std::condition_variable condition;
  bool closed = false;
  std::unique_ptr<netlib::ThreadPool> workers =
      netlib::ThreadPool::Create(1, 8, [](netlib::FrameMessage) {});
  if (!workers) return false;
  netlib::SubReactor::ConnectionCallbacks callbacks;
  callbacks.on_closed = [&](netlib::SocketHandle) {
    std::lock_guard<std::mutex> lock(mutex);
    closed = true;
    condition.notify_all();
  };
  std::unique_ptr<netlib::SubReactor> sub =
      netlib::SubReactor::CreateWithSession(
          {}, *workers, std::move(callbacks),
          [](netlib::ProtocolSessionOptions limits) {
            return netlib::CreateHttp2Session(
                limits, {.header_timeout = std::chrono::milliseconds(80)});
          });
  if (!sub) return false;
  std::unique_ptr<netlib::MainReactor> main =
      netlib::MainReactor::Create(0, {sub.get()});
  if (!main) return false;
  std::thread sub_thread([&] { sub->Run(); });
  std::thread main_thread([&] { main->Run(); });
  netlib::UniqueFd socket(netlib::platform::CreateTcpSocket(false));
  bool success =
      socket && netlib::platform::ConnectLoopback(socket.get(), main->port());
  if (success) {
    std::unique_lock<std::mutex> lock(mutex);
    success = condition.wait_for(lock, std::chrono::seconds(2),
                                 [&] { return closed; });
  }
  socket.Reset();
  main->Stop();
  sub->Stop();
  main_thread.join();
  sub_thread.join();
  workers->Stop();
  return success;
}

bool TestInvalidPrefaceAndResponse() {
  std::unique_ptr<netlib::ProtocolSession> server =
      netlib::CreateHttp2Session();
  const std::string invalid = "GET / HTTP/1.1\r\n\r\n";
  if (server->OnInput(invalid).status !=
      netlib::SessionInputStatus::kProtocolError)
    return false;
  Harness h;
  h.server = netlib::CreateHttp2Session();
  if (!h.client.Initialize() || !h.Pump()) return false;
  std::int32_t first = h.client.Request("/invalid");
  std::int32_t second = h.client.Request("/head", {}, false, {}, "HEAD");
  if (!h.Pump() || h.requests.size() != 2) return false;
  netlib::HttpResponse bad = Reply("bad");
  bad.headers.push_back({"Connection", "close"});
  h.server->OnResponse(h.requests[0].context, std::move(bad),
                       netlib::SessionResponseMode::kOrdered, false);
  h.server->OnResponse(h.requests[1].context, Reply("representation"),
                       netlib::SessionResponseMode::kOrdered, false);
  return h.Pump() && h.client.replies[first].reset != 0 &&
         h.client.replies[second].status == "200" &&
         h.client.replies[second].body.empty() &&
         h.client.replies[second].complete;
}

bool TestConnectionBudgetAndProtocolReset() {
  Harness h;
  h.server = netlib::CreateHttp2Session(
      {}, {.max_body_bytes = 64, .max_buffered_body_bytes = 32});
  if (!h.client.Initialize() || !h.Pump()) return false;
  std::int32_t first = h.client.Request("/first", std::string(30, 'a'), true);
  std::int32_t second = h.client.Request("/second", std::string(20, 'b'), true);
  if (!h.Pump() || !h.client.Allow(first, 20) || !h.Pump() ||
      !h.client.Allow(second, 20) || !h.Pump() ||
      h.client.replies[second].reset == 0 || !h.requests.empty() ||
      !h.client.Allow(first, 10) || !h.Pump() || h.requests.size() != 1)
    return Check(
        false,
        "connection budget rejected the wrong stream or did not release bytes");
  // A response of the wrong application type generates RST. Its close callback
  // runs during ProduceOutput, with no new peer input to deliver cancellation.
  netlib::SessionResponseStatus status =
      h.server->OnResponse(h.requests[0].context, netlib::BinaryResponse{},
                           netlib::SessionResponseMode::kOrdered, false);
  if (status != netlib::SessionResponseStatus::kQueued || !h.Pump() ||
      h.cancelled.size() != 1 || h.cancelled[0] != h.requests[0].context) {
    std::fprintf(stderr, "reset cancellation count=%zu status=%d reset=%u\n",
                 h.cancelled.size(), static_cast<int>(status),
                 h.client.replies[first].reset);
    return false;
  }
  std::int32_t next = h.client.Request("/survivor");
  if (!h.Pump() || h.requests.size() != 2)
    return Check(false, "replacement stream refused after protocol reset");
  h.server->OnResponse(h.requests[1].context, Reply("ok"),
                       netlib::SessionResponseMode::kOrdered, false);
  return h.Pump() && h.client.replies[next].body == "ok";
}

bool TestRetainedAllocationBudget() {
  if (netlib::CreateHttp2Session({}, {.max_memory_bytes = 1})) return false;
  Harness h;
  h.server = netlib::CreateHttp2Session({}, {.max_memory_bytes = 256 * 1024});
  if (!h.server || !h.client.Initialize() || !h.Pump()) return false;
  std::shared_ptr<netlib::MemoryBudget> budget = h.server->memory_budget();
  const std::int32_t first =
      h.client.Request("/retained", std::string(20000, 'a'));
  if (!h.Pump() || h.requests.size() != 1 || !h.requests[0].memory ||
      h.requests[0].memory->size() < h.requests[0].payload.capacity())
    return false;
  const std::size_t request_charge = h.requests[0].memory->size();
  if (nghttp2_submit_rst_stream(h.client.session, NGHTTP2_FLAG_NONE, first,
                                NGHTTP2_CANCEL) != 0 ||
      !h.Pump())
    return false;
  // Reset and even destruction of the Session must not return Worker-held data.
  h.server.reset();
  if (budget->used() != request_charge) return false;
  h.requests.clear();
  if (budget->used() != 0) return false;

  Harness response;
  response.server =
      netlib::CreateHttp2Session({}, {.max_memory_bytes = 256 * 1024});
  if (!response.client.Initialize() || !response.Pump()) return false;
  const std::int32_t rejected = response.client.Request("/oversized-capacity");
  const std::int32_t survivor = response.client.Request("/survivor");
  if (!response.Pump() || response.requests.size() != 2) return false;
  netlib::HttpResponse large = Reply("x");
  large.body.reserve(256 * 1024);
  response.server->OnResponse(response.requests[0].context, std::move(large),
                              netlib::SessionResponseMode::kOrdered, false);
  response.server->OnResponse(response.requests[1].context, Reply("ok"),
                              netlib::SessionResponseMode::kOrdered, false);
  return response.Pump() && response.client.replies[rejected].reset != 0 &&
         response.client.replies[survivor].body == "ok" &&
         !response.server->ShouldClose();
}

bool TestTcpQueueSaturation() {
  std::mutex mutex;
  std::condition_variable condition;
  bool release = false;
  int entered = 0;
  std::atomic<bool> large_rejected{false};
  std::unique_ptr<netlib::ThreadPool> workers =
      netlib::ThreadPool::Create(1, 1, [&](netlib::FrameMessage message) {
        {
          std::unique_lock<std::mutex> lock(mutex);
          ++entered;
          condition.notify_all();
          condition.wait(lock, [&] { return release; });
        }
        netlib::HttpResponse oversized = Reply("x");
        oversized.body.reserve(4096);
        large_rejected.store(!message.Respond(std::move(oversized)));
        message.Respond(Reply("ok"));
      });
  if (!workers) return false;
  netlib::SubReactorOptions options{.max_pending_responses = 8,
                                    .max_queued_response_bytes = 1024,
                                    .max_read_bytes_per_turn = 17,
                                    .max_write_bytes_per_turn = 7,
                                    .max_decode_calls_per_turn = 1,
                                    .max_response_commands_per_turn = 1};
  std::unique_ptr<netlib::SubReactor> sub =
      netlib::SubReactor::CreateWithSession(
          options, *workers, {}, [](netlib::ProtocolSessionOptions limits) {
            return netlib::CreateHttp2Session(limits);
          });
  if (!sub) return false;
  std::unique_ptr<netlib::MainReactor> main =
      netlib::MainReactor::Create(0, {sub.get()});
  if (!main) return false;
  std::thread sub_thread([&] { sub->Run(); });
  std::thread main_thread([&] { main->Run(); });
  netlib::UniqueFd socket(netlib::platform::CreateTcpSocket(false));
  Client client;
  bool success =
      socket && netlib::platform::SetReceiveTimeout(socket.get(), 2000) &&
      netlib::platform::ConnectLoopback(socket.get(), main->port()) &&
      client.Initialize() && client.Request("/active") == 1 &&
      SendClient(client, socket.get());
  if (success) {
    std::unique_lock<std::mutex> lock(mutex);
    success = condition.wait_for(lock, std::chrono::seconds(2),
                                 [&] { return entered == 1; });
  }
  const std::array<std::uint8_t, 8> ping{};
  success = success && client.Request("/queued") == 3 &&
            client.Request("/refused") == 5 &&
            nghttp2_submit_ping(client.session, NGHTTP2_FLAG_NONE,
                                ping.data()) == 0 &&
            SendClient(client, socket.get());
  std::array<char, 4096> bytes{};
  for (int i = 0; success && i < 1000 &&
                  (client.replies[5].reset == 0 || client.ping_acks == 0);
       ++i) {
    std::ptrdiff_t count =
        netlib::platform::Receive(socket.get(), bytes.data(), bytes.size());
    success = count > 0 &&
              client.Receive(std::span<const char>(bytes).first(
                  static_cast<std::size_t>(count))) &&
              SendClient(client, socket.get());
  }
  success = success && client.replies[5].reset == NGHTTP2_REFUSED_STREAM &&
            client.ping_acks == 1 && !client.replies[1].complete &&
            !client.replies[3].complete;
  {
    std::lock_guard<std::mutex> lock(mutex);
    release = true;
    condition.notify_all();
  }
  for (int i = 0; success && i < 1000 && !client.replies[3].complete; ++i) {
    std::ptrdiff_t count =
        netlib::platform::Receive(socket.get(), bytes.data(), bytes.size());
    success = count > 0 &&
              client.Receive(std::span<const char>(bytes).first(
                  static_cast<std::size_t>(count))) &&
              SendClient(client, socket.get());
  }
  success = success && client.replies[1].body == "ok" &&
            client.replies[3].body == "ok" && large_rejected.load();
  success = success && client.Request("/after-saturation") == 7 &&
            SendClient(client, socket.get());
  for (int i = 0; success && i < 1000 && !client.replies[7].complete; ++i) {
    std::ptrdiff_t count =
        netlib::platform::Receive(socket.get(), bytes.data(), bytes.size());
    success = count > 0 &&
              client.Receive(std::span<const char>(bytes).first(
                  static_cast<std::size_t>(count))) &&
              SendClient(client, socket.get());
  }
  success = success && client.replies[7].body == "ok";
  socket.Reset();
  main->Stop();
  sub->Stop();
  main_thread.join();
  sub_thread.join();
  workers->Stop();
  return success;
}

bool TestWorkerHeldConnectionBudget() {
  Harness h;
  h.server = netlib::CreateHttp2Session({}, {.max_memory_bytes = 256 * 1024});
  if (!h.server || !h.client.Initialize() || !h.Pump()) return false;
  h.client.Request("/worker-held", std::string(60000, 'a'));
  if (!h.Pump() || h.requests.size() != 1) return false;
  const std::int32_t refused =
      h.client.Request("/over-budget", std::string(60000, 'b'));
  if (!h.Pump() || h.requests.size() != 1 ||
      h.client.replies[refused].reset == 0 || h.server->ShouldClose())
    return false;
  // Logical Session body size is zero after dispatch, but its allocation is
  // still owned by a Worker. Only releasing that request admits another body.
  h.requests.clear();
  const std::int32_t next =
      h.client.Request("/after-release", std::string(60000, 'c'));
  return h.Pump() && h.requests.size() == 1 &&
         h.requests[0].context.stream_id == static_cast<std::uint32_t>(next) &&
         h.requests[0].payload.size() == 60000 &&
         h.server->memory_budget()->used() <=
             h.server->memory_budget()->limit();
}

}  // namespace

int main() {
  struct Test {
    const char* name;
    bool (*run)();
  };
  const Test tests[]{
      {"multiplexing/controls", TestMultiplexingAndControls},
      {"flow control/CONTINUATION", TestFlowControlAndLargeHeaders},
      {"reset/limits/timeout/drain", TestResetLimitsTimeoutAndDrain},
      {"invalid preface/response and HEAD", TestInvalidPrefaceAndResponse},
      {"connection budget/protocol reset",
       TestConnectionBudgetAndProtocolReset},
      {"retained allocation budget", TestRetainedAllocationBudget},
      {"TCP queue saturation and recovery", TestTcpQueueSaturation},
      {"Worker-held connection budget and recovery",
       TestWorkerHeldConnectionBudget},
      {"TCP control flood with tiny scheduling slices", TestTcpControlFlood},
      {"TCP multiplexing", TestTcpRouting},
      {"TCP cancellation", TestTcpCancellation},
      {"TCP timeout", TestTcpTimeout},
      {"phase deadlines", TestPhaseDeadlines},
      {"drain boundary/expiry", TestDrainBoundaryAndExpiry},
      {"TCP graceful drain", TestTcpDrain},
      {"TCP graceful drain with delayed SETTINGS ACK", TestTcpDrainDelayedAck},
      {"TCP handshake deadline", TestTcpHandshakeDeadline},
  };
  for (const Test& test : tests) {
    if (!test.run()) {
      std::fprintf(stderr, "HTTP/2 test failed: %s\n", test.name);
      return 1;
    }
  }
  return 0;
}
