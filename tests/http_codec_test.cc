#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

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
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

  connection->Reset(socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
  if (!*connection) {
    return false;
  }
  timeval timeout{};
  timeout.tv_sec = 2;
  setsockopt(connection->get(), SOL_SOCKET, SO_RCVTIMEO, &timeout,
             sizeof(timeout));
  return connect(connection->get(), reinterpret_cast<const sockaddr*>(&address),
                 sizeof(address)) == 0;
}

bool ReadUntilClosed(int fd, std::string* output) {
  char buffer[4096];
  while (true) {
    ssize_t result = recv(fd, buffer, sizeof(buffer), 0);
    if (result > 0) {
      output->append(buffer, static_cast<std::size_t>(result));
      continue;
    }
    if (result == 0) {
      return true;
    }
    if (errno == EINTR) {
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

int RunHttpServerTest() {
  std::mutex handler_mutex;
  std::condition_variable handler_condition;
  bool handler_completed = false;
  bool handler_ok = false;

  netlib::ThreadPool::Handler handler =
      [&handler_mutex, &handler_condition, &handler_completed,
       &handler_ok](netlib::FrameMessage message) {
        const netlib::HttpRequestMetadata* metadata =
            std::get_if<netlib::HttpRequestMetadata>(&message.metadata);
        bool valid = metadata != nullptr && metadata->method == "POST" &&
                     metadata->target == "/rpc" &&
                     message.payload == std::vector<char>{'p', 'i', 'n', 'g'};
        if (valid) {
          valid = message.connection.Respond(netlib::HttpResponse{
              .status_code = 200,
              .reason_phrase = "OK",
              .headers =
                  {
                      netlib::HttpHeader{
                          .name = "Content-Type",
                          .value = "text/plain",
                      },
                  },
              .body = {'p', 'o', 'n', 'g'},
              .keep_alive = false,
          });
        }
        {
          std::lock_guard<std::mutex> lock(handler_mutex);
          handler_completed = true;
          handler_ok = valid;
          handler_condition.notify_all();
        }
      };

  std::unique_ptr<netlib::ThreadPool> worker_pool =
      netlib::ThreadPool::Create(1, 16, std::move(handler));
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
  constexpr std::string_view kRequest =
      "POST /rpc HTTP/1.1\r\n"
      "Host: localhost\r\n"
      "Content-Length: 4\r\n"
      "\r\n"
      "ping";
  if (!ConnectTo(main_reactor->port(), &client) ||
      send(client.get(), kRequest.data(), kRequest.size(), MSG_NOSIGNAL) !=
          static_cast<ssize_t>(kRequest.size())) {
    ReportFailure("failed to send HTTP request");
    success = false;
  }

  {
    std::unique_lock<std::mutex> lock(handler_mutex);
    if (success && !handler_condition.wait_for(
                       lock, std::chrono::seconds(2),
                       [&handler_completed] { return handler_completed; })) {
      ReportFailure("HTTP Worker did not process the request");
      success = false;
    }
    if (success && !handler_ok) {
      ReportFailure("HTTP request context was incorrect");
      success = false;
    }
  }

  std::string response;
  if (success && !ReadUntilClosed(client.get(), &response)) {
    ReportFailure("failed to read HTTP response");
    success = false;
  }
  if (success && (response.find("HTTP/1.1 200 OK\r\n") != 0 ||
                  response.find("Content-Length: 4\r\n") == std::string::npos ||
                  response.find("Connection: close\r\n") == std::string::npos ||
                  response.find("\r\n\r\npong") == std::string::npos)) {
    ReportFailure("HTTP response wire format is incorrect");
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
  return RunHttpServerTest();
}
