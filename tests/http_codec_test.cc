#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdio>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "netlib/frame_message.h"
#include "netlib/http1_codec.h"
#include "netlib/main_reactor.h"
#include "netlib/platform.h"
#include "netlib/protocol_codec.h"
#include "netlib/protocol_message.h"
#include "netlib/sub_reactor.h"
#include "netlib/thread_pool.h"
#include "netlib/unique_fd.h"

namespace {

void ReportFailure(const char* message) {
  std::fprintf(stderr, "%s\n", message);
}

std::span<const char> Bytes(std::string_view value) {
  return std::span<const char>(value.data(), value.size());
}

bool ConnectTo(uint16_t port, netlib::UniqueFd* connection) {
  connection->Reset(netlib::platform::CreateTcpSocket(false));
  return *connection &&
         netlib::platform::SetReceiveTimeout(connection->get(), 2000) &&
         netlib::platform::SetReceiveBuffer(connection->get(), 8 * 1024) &&
         netlib::platform::ConnectLoopback(connection->get(), port);
}

bool SendAll(netlib::SocketHandle fd, std::string_view data) {
  std::size_t sent = 0;
  while (sent < data.size()) {
    std::ptrdiff_t result =
        netlib::platform::Send(fd, data.data() + sent, data.size() - sent);
    if (result > 0) {
      sent += static_cast<std::size_t>(result);
      continue;
    }
    if (result < 0 &&
        netlib::platform::Interrupted(netlib::platform::LastError())) {
      continue;
    }
    return false;
  }
  return true;
}

bool ReadUntilClosed(netlib::SocketHandle fd, std::string* output) {
  char buffer[4096];
  while (true) {
    std::ptrdiff_t result =
        netlib::platform::Receive(fd, buffer, sizeof(buffer));
    if (result > 0) {
      output->append(buffer, static_cast<std::size_t>(result));
      continue;
    }
    if (result == 0) {
      return true;
    }
    if (netlib::platform::Interrupted(netlib::platform::LastError())) {
      continue;
    }
    return false;
  }
}

int RunRequestParsingTest() {
  std::unique_ptr<netlib::ProtocolCodec> codec =
      netlib::CreateHttp1Codec(netlib::Http1CodecOptions{
          .max_header_bytes = 128,
          .max_body_bytes = 32,
      });
  if (!codec) {
    ReportFailure("failed to create HTTP codec");
    return 1;
  }

  const std::string request =
      "POST /submit?q=1 HTTP/1.1\r\n"
      "Host: example.com\r\n"
      "content-length: 5\r\n"
      "Connection: keep-alive\r\n"
      "\r\n"
      "hello";
  netlib::DecodeResult result = codec->Decode(Bytes(request).first(10));
  if (result.status != netlib::DecodeStatus::kNeedMore ||
      result.required_bytes != 0) {
    ReportFailure("HTTP header fragment was decoded incorrectly");
    return 1;
  }

  const std::size_t header_size = request.find("\r\n\r\n") + 4;
  result = codec->Decode(Bytes(request).first(header_size));
  if (result.status != netlib::DecodeStatus::kNeedMore ||
      result.required_bytes != request.size()) {
    ReportFailure("HTTP body target was not reported");
    return 1;
  }

  result = codec->Decode(Bytes(request));
  const netlib::HttpRequestMetadata* metadata =
      std::get_if<netlib::HttpRequestMetadata>(&result.metadata);
  if (result.status != netlib::DecodeStatus::kFrameReady ||
      result.consumed_bytes != request.size() ||
      result.payload_offset != header_size || result.payload_size != 5 ||
      metadata == nullptr || metadata->method != "POST" ||
      metadata->target != "/submit?q=1" || metadata->version != "HTTP/1.1" ||
      metadata->content_length != 5 || !metadata->keep_alive ||
      metadata->headers.size() != 3 ||
      std::string_view(request.data() + result.payload_offset,
                       result.payload_size) != "hello") {
    ReportFailure("HTTP request context or body is incorrect");
    return 1;
  }
  return 0;
}

int RunCoalescedRequestTest() {
  std::unique_ptr<netlib::ProtocolCodec> codec = netlib::CreateHttp1Codec();
  if (!codec) {
    ReportFailure("failed to create default HTTP codec");
    return 1;
  }

  const std::string first =
      "GET /first HTTP/1.1\r\n"
      "Host: example.com\r\n"
      "\r\n";
  const std::string second =
      "GET /second HTTP/1.1\r\n"
      "Host: example.com\r\n"
      "Connection: close\r\n"
      "\r\n";
  const std::string requests = first + second;

  netlib::DecodeResult result = codec->Decode(Bytes(requests));
  if (result.status != netlib::DecodeStatus::kFrameReady ||
      result.consumed_bytes != first.size()) {
    ReportFailure("first coalesced HTTP request failed");
    return 1;
  }
  result = codec->Decode(Bytes(requests).subspan(first.size()));
  const netlib::HttpRequestMetadata* metadata =
      std::get_if<netlib::HttpRequestMetadata>(&result.metadata);
  if (result.status != netlib::DecodeStatus::kFrameReady ||
      result.consumed_bytes != second.size() || metadata == nullptr ||
      metadata->target != "/second" || metadata->keep_alive) {
    ReportFailure("second coalesced HTTP request failed");
    return 1;
  }
  return 0;
}

int RunResponseEncodingTest() {
  std::unique_ptr<netlib::ProtocolCodec> codec = netlib::CreateHttp1Codec();
  if (!codec) {
    ReportFailure("failed to create response HTTP codec");
    return 1;
  }

  netlib::HttpResponse response{
      .status_code = 201,
      .reason_phrase = "Created",
      .headers =
          {
              netlib::HttpHeader{.name = "Content-Type", .value = "text/plain"},
              netlib::HttpHeader{.name = "Content-Length", .value = "999"},
              netlib::HttpHeader{.name = "Connection", .value = "keep-alive"},
          },
      .body = {'o', 'k'},
      .keep_alive = false,
  };
  std::vector<char> output;
  if (!codec->Encode(netlib::ApplicationResponse{std::move(response)},
                     &output)) {
    ReportFailure("failed to encode HTTP response");
    return 1;
  }

  std::string wire(output.begin(), output.end());
  if (wire.find("HTTP/1.1 201 Created\r\n") != 0 ||
      wire.find("Content-Type: text/plain\r\n") == std::string::npos ||
      wire.find("Content-Length: 999") != std::string::npos ||
      wire.find("Content-Length: 2\r\n") == std::string::npos ||
      wire.find("Connection: close\r\n") == std::string::npos ||
      wire.find("\r\n\r\nok") == std::string::npos) {
    ReportFailure("HTTP response wire format is incorrect");
    return 1;
  }
  return 0;
}

int RunInvalidRequestTest() {
  std::unique_ptr<netlib::ProtocolCodec> codec = netlib::CreateHttp1Codec();
  if (!codec) {
    ReportFailure("failed to create invalid-request HTTP codec");
    return 1;
  }

  const std::string chunked =
      "POST / HTTP/1.1\r\n"
      "Transfer-Encoding: chunked\r\n"
      "\r\n";
  netlib::DecodeResult result = codec->Decode(Bytes(chunked));
  if (result.status != netlib::DecodeStatus::kProtocolError) {
    ReportFailure("HTTP codec accepted unsupported chunked framing");
    return 1;
  }
  return 0;
}

int RunHttpPipelineTest() {
  std::mutex handler_mutex;
  std::condition_variable handler_condition;
  std::size_t completed_requests = 0;
  bool first_request_started = false;
  bool second_response_queued = false;
  bool handler_ok = true;

  netlib::ThreadPool::Handler handler = [&handler_mutex, &handler_condition,
                                         &completed_requests,
                                         &first_request_started,
                                         &second_response_queued, &handler_ok](
                                            netlib::FrameMessage message) {
    const netlib::HttpRequestMetadata* metadata =
        std::get_if<netlib::HttpRequestMetadata>(&message.metadata);
    bool first = message.sequence == 0 && metadata != nullptr &&
                 metadata->method == "POST" && metadata->target == "/first" &&
                 message.payload == std::vector<char>{'o', 'n', 'e'};
    bool second = message.sequence == 1 && metadata != nullptr &&
                  metadata->method == "POST" && metadata->target == "/second" &&
                  message.payload == std::vector<char>{'t', 'w', 'o'};
    bool accepted = false;

    if (first) {
      std::unique_lock<std::mutex> lock(handler_mutex);
      first_request_started = true;
      handler_condition.notify_all();
      if (handler_condition.wait_for(
              lock, std::chrono::seconds(2),
              [&second_response_queued] { return second_response_queued; })) {
        lock.unlock();
        accepted = message.connection.Respond(
            message.sequence, netlib::HttpResponse{
                                  .status_code = 200,
                                  .reason_phrase = "OK",
                                  .headers =
                                      {
                                          netlib::HttpHeader{
                                              .name = "Content-Type",
                                              .value = "text/plain",
                                          },
                                      },
                                  .body = {'f', 'i', 'r', 's', 't'},
                                  .keep_alive = true,
                              });
      }
    } else if (second) {
      accepted = message.connection.Respond(
          message.sequence, netlib::HttpResponse{
                                .status_code = 200,
                                .reason_phrase = "OK",
                                .headers =
                                    {
                                        netlib::HttpHeader{
                                            .name = "Content-Type",
                                            .value = "text/plain",
                                        },
                                    },
                                .body = {'s', 'e', 'c', 'o', 'n', 'd'},
                                .keep_alive = false,
                            });
    }

    {
      std::lock_guard<std::mutex> lock(handler_mutex);
      ++completed_requests;
      handler_ok = handler_ok && accepted;
      if (second && accepted) {
        second_response_queued = true;
      }
      handler_condition.notify_all();
    }
  };

  std::unique_ptr<netlib::ThreadPool> worker_pool =
      netlib::ThreadPool::Create(2, 16, std::move(handler));
  if (!worker_pool) {
    ReportFailure("failed to create HTTP ThreadPool");
    return 1;
  }
  netlib::SubReactorOptions sub_options{
      .max_pending_connections = 8,
      .max_pending_responses = 8,
      .max_pending_output_bytes = 64 * 1024,
  };
  std::unique_ptr<netlib::SubReactor> sub_reactor = netlib::SubReactor::Create(
      sub_options, *worker_pool, {}, [] { return netlib::CreateHttp1Codec(); });
  if (!sub_reactor) {
    ReportFailure("failed to create HTTP SubReactor");
    return 1;
  }
  std::unique_ptr<netlib::MainReactor> main_reactor =
      netlib::MainReactor::Create(0, {sub_reactor.get()});
  if (!main_reactor) {
    ReportFailure("failed to create HTTP MainReactor");
    return 1;
  }

  std::thread sub_thread([&sub_reactor] { sub_reactor->Run(); });
  std::thread main_thread([&main_reactor] { main_reactor->Run(); });

  bool success = true;
  netlib::UniqueFd client;
  constexpr std::string_view kFirstRequest =
      "POST /first HTTP/1.1\r\n"
      "Host: localhost\r\n"
      "Content-Length: 3\r\n"
      "\r\n"
      "one";
  constexpr std::string_view kSecondRequest =
      "POST /second HTTP/1.1\r\n"
      "Host: localhost\r\n"
      "Content-Length: 3\r\n"
      "Connection: close\r\n"
      "\r\n"
      "two";
  std::string requests(kFirstRequest);
  requests.append(kSecondRequest);
  if (!ConnectTo(main_reactor->port(), &client) ||
      !SendAll(client.get(), requests)) {
    ReportFailure("failed to send pipelined HTTP requests");
    success = false;
  }

  {
    std::unique_lock<std::mutex> lock(handler_mutex);
    if (success && !handler_condition.wait_for(
                       lock, std::chrono::seconds(2),
                       [&completed_requests, &first_request_started] {
                         return completed_requests == 2 &&
                                first_request_started;
                       })) {
      ReportFailure("HTTP Workers did not process both pipelined requests");
      success = false;
    }
    if (success && !handler_ok) {
      ReportFailure("pipelined HTTP request context was incorrect");
      success = false;
    }
  }

  std::string response;
  if (success && !ReadUntilClosed(client.get(), &response)) {
    ReportFailure("failed to read HTTP response");
    success = false;
  }
  if (success &&
      (response.find("HTTP/1.1 200 OK\r\n") != 0 ||
       response.find("Content-Length: 5\r\n") == std::string::npos ||
       response.find("Connection: close\r\n") == std::string::npos ||
       response.find("\r\n\r\nfirst") == std::string::npos ||
       response.find("\r\n\r\nsecond") == std::string::npos ||
       response.find("\r\n\r\nfirst") > response.find("\r\n\r\nsecond"))) {
    ReportFailure("pipelined HTTP responses were out of order or malformed");
    success = false;
  }

  client.Reset();
  main_reactor->Stop();
  sub_reactor->Stop();
  main_thread.join();
  sub_thread.join();
  worker_pool->Stop();
  return success ? 0 : 1;
}

int RunHttpTimeoutTest() {
  std::mutex handler_mutex;
  std::condition_variable handler_condition;
  std::size_t completed_requests = 0;
  bool handler_ok = true;

  netlib::ThreadPool::Handler handler =
      [&handler_mutex, &handler_condition, &completed_requests,
       &handler_ok](netlib::FrameMessage message) {
        const netlib::HttpRequestMetadata* metadata =
            std::get_if<netlib::HttpRequestMetadata>(&message.metadata);
        bool slow = message.sequence == 0 && metadata != nullptr &&
                    metadata->target == "/slow";
        bool fast = message.sequence == 1 && metadata != nullptr &&
                    metadata->target == "/fast";
        bool accepted = slow;
        if (fast) {
          accepted = message.connection.Respond(
              message.sequence, netlib::HttpResponse{
                                    .status_code = 200,
                                    .reason_phrase = "OK",
                                    .body = {'f', 'a', 's', 't'},
                                    .keep_alive = true,
                                });
        }

        {
          std::lock_guard<std::mutex> lock(handler_mutex);
          ++completed_requests;
          handler_ok = handler_ok && accepted;
          handler_condition.notify_all();
        }
      };

  std::unique_ptr<netlib::ThreadPool> worker_pool =
      netlib::ThreadPool::Create(2, 16, std::move(handler));
  if (!worker_pool) {
    ReportFailure("failed to create HTTP timeout ThreadPool");
    return 1;
  }
  netlib::SubReactorOptions sub_options{
      .max_pending_connections = 8,
      .max_pending_responses = 8,
      .max_pending_output_bytes = 64 * 1024,
      .request_timeout = std::chrono::milliseconds(100),
  };
  std::unique_ptr<netlib::SubReactor> sub_reactor = netlib::SubReactor::Create(
      sub_options, *worker_pool, {}, [] { return netlib::CreateHttp1Codec(); });
  if (!sub_reactor) {
    ReportFailure("failed to create HTTP timeout SubReactor");
    return 1;
  }
  std::unique_ptr<netlib::MainReactor> main_reactor =
      netlib::MainReactor::Create(0, {sub_reactor.get()});
  if (!main_reactor) {
    ReportFailure("failed to create HTTP timeout MainReactor");
    return 1;
  }

  std::thread sub_thread([&sub_reactor] { sub_reactor->Run(); });
  std::thread main_thread([&main_reactor] { main_reactor->Run(); });

  bool success = true;
  netlib::UniqueFd client;
  constexpr std::string_view kRequests =
      "GET /slow HTTP/1.1\r\n"
      "Host: localhost\r\n"
      "\r\n"
      "GET /fast HTTP/1.1\r\n"
      "Host: localhost\r\n"
      "\r\n";
  if (!ConnectTo(main_reactor->port(), &client) ||
      !SendAll(client.get(), kRequests)) {
    ReportFailure("failed to send requests for HTTP timeout test");
    success = false;
  }

  {
    std::unique_lock<std::mutex> lock(handler_mutex);
    if (success && !handler_condition.wait_for(lock, std::chrono::seconds(2),
                                               [&completed_requests] {
                                                 return completed_requests == 2;
                                               })) {
      ReportFailure("HTTP timeout test requests were not dispatched");
      success = false;
    }
    if (success && !handler_ok) {
      ReportFailure("HTTP timeout test request context was incorrect");
      success = false;
    }
  }

  std::string response;
  if (success && !ReadUntilClosed(client.get(), &response)) {
    ReportFailure("timed-out HTTP connection did not close");
    success = false;
  }
  if (success &&
      (response.find("HTTP/1.1 504 Gateway Timeout\r\n") != 0 ||
       response.find("Content-Length: 18\r\n") == std::string::npos ||
       response.find("Connection: close\r\n") == std::string::npos ||
       response.find("\r\n\r\nrequest timed out\n") == std::string::npos ||
       response.find("fast") != std::string::npos)) {
    ReportFailure("HTTP timeout response was missing or malformed");
    success = false;
  }

  client.Reset();
  main_reactor->Stop();
  sub_reactor->Stop();
  main_thread.join();
  sub_thread.join();
  worker_pool->Stop();
  return success ? 0 : 1;
}

int RunHttpResponseCancelsTimeoutTest() {
  std::mutex handler_mutex;
  std::condition_variable handler_condition;
  std::size_t completed_requests = 0;
  bool handler_ok = true;

  netlib::ThreadPool::Handler handler = [&handler_mutex, &handler_condition,
                                         &completed_requests, &handler_ok](
                                            netlib::FrameMessage message) {
    const netlib::HttpRequestMetadata* metadata =
        std::get_if<netlib::HttpRequestMetadata>(&message.metadata);
    bool first = metadata != nullptr && metadata->target == "/cancel-first";
    bool second = metadata != nullptr && metadata->target == "/cancel-second";
    bool accepted = false;
    if (first || second) {
      accepted = message.connection.Respond(
          message.sequence,
          netlib::HttpResponse{
              .status_code = 200,
              .reason_phrase = "OK",
              .body = first ? std::vector<char>{'f', 'i', 'r', 's', 't'}
                            : std::vector<char>{'s', 'e', 'c', 'o', 'n', 'd'},
              .keep_alive = first,
          });
    }

    {
      std::lock_guard<std::mutex> lock(handler_mutex);
      ++completed_requests;
      handler_ok = handler_ok && accepted;
      handler_condition.notify_all();
    }
  };

  std::unique_ptr<netlib::ThreadPool> worker_pool =
      netlib::ThreadPool::Create(1, 16, std::move(handler));
  if (!worker_pool) {
    ReportFailure("failed to create timeout cancellation ThreadPool");
    return 1;
  }
  netlib::SubReactorOptions sub_options{
      .max_pending_connections = 8,
      .max_pending_responses = 8,
      .max_pending_output_bytes = 64 * 1024,
      .request_timeout = std::chrono::milliseconds(300),
  };
  std::unique_ptr<netlib::SubReactor> sub_reactor = netlib::SubReactor::Create(
      sub_options, *worker_pool, {}, [] { return netlib::CreateHttp1Codec(); });
  if (!sub_reactor) {
    ReportFailure("failed to create timeout cancellation SubReactor");
    return 1;
  }
  std::unique_ptr<netlib::MainReactor> main_reactor =
      netlib::MainReactor::Create(0, {sub_reactor.get()});
  if (!main_reactor) {
    ReportFailure("failed to create timeout cancellation MainReactor");
    return 1;
  }

  std::thread sub_thread([&sub_reactor] { sub_reactor->Run(); });
  std::thread main_thread([&main_reactor] { main_reactor->Run(); });

  bool success = true;
  netlib::UniqueFd client;
  constexpr std::string_view kFirstRequest =
      "POST /cancel-first HTTP/1.1\r\n"
      "Host: localhost\r\n"
      "Content-Length: 0\r\n"
      "\r\n";
  constexpr std::string_view kSecondRequest =
      "POST /cancel-second HTTP/1.1\r\n"
      "Host: localhost\r\n"
      "Content-Length: 0\r\n"
      "Connection: close\r\n"
      "\r\n";
  if (!ConnectTo(main_reactor->port(), &client) ||
      !SendAll(client.get(), kFirstRequest)) {
    ReportFailure("failed to send first cancellation-test request");
    success = false;
  }

  {
    std::unique_lock<std::mutex> lock(handler_mutex);
    if (success && !handler_condition.wait_for(lock, std::chrono::seconds(2),
                                               [&completed_requests] {
                                                 return completed_requests >= 1;
                                               })) {
      ReportFailure("first cancellation-test request was not processed");
      success = false;
    }
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(400));
  if (success && !SendAll(client.get(), kSecondRequest)) {
    ReportFailure("successful response did not cancel the request timeout");
    success = false;
  }

  {
    std::unique_lock<std::mutex> lock(handler_mutex);
    if (success && !handler_condition.wait_for(lock, std::chrono::seconds(2),
                                               [&completed_requests] {
                                                 return completed_requests >= 2;
                                               })) {
      ReportFailure("timed-out connection rejected the second request");
      success = false;
    }
    if (success && !handler_ok) {
      ReportFailure("timeout cancellation-test request context was incorrect");
      success = false;
    }
  }

  std::string response;
  if (success && !ReadUntilClosed(client.get(), &response)) {
    ReportFailure("failed to read timeout cancellation-test responses");
    success = false;
  }
  if (success &&
      (response.find("\r\n\r\nfirst") == std::string::npos ||
       response.find("\r\n\r\nsecond") == std::string::npos ||
       response.find("HTTP/1.1 504 Gateway Timeout") != std::string::npos)) {
    ReportFailure("timeout fired after the business response was accepted");
    success = false;
  }

  client.Reset();
  main_reactor->Stop();
  sub_reactor->Stop();
  main_thread.join();
  sub_thread.join();
  worker_pool->Stop();
  return success ? 0 : 1;
}

}  // namespace

int main() {
  int parse_result = RunRequestParsingTest();
  if (parse_result != 0) {
    return parse_result;
  }
  int coalesced_result = RunCoalescedRequestTest();
  if (coalesced_result != 0) {
    return coalesced_result;
  }
  int encode_result = RunResponseEncodingTest();
  if (encode_result != 0) {
    return encode_result;
  }
  int invalid_result = RunInvalidRequestTest();
  if (invalid_result != 0) {
    return invalid_result;
  }
  int pipeline_result = RunHttpPipelineTest();
  if (pipeline_result != 0) {
    return pipeline_result;
  }
  int cancellation_result = RunHttpResponseCancelsTimeoutTest();
  if (cancellation_result != 0) {
    return cancellation_result;
  }
  return RunHttpTimeoutTest();
}
