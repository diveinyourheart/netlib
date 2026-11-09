#include "netlib/protocol_session.h"

#include <cstdio>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "netlib/http1_codec.h"
#include "netlib/rpc_codec.h"

namespace {

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::fprintf(stderr, "%s\n", message);
  }
  return condition;
}

std::string Wire(char payload) { return std::string("\0\0\0\1", 4) + payload; }

std::string Output(const netlib::ProtocolSession& session) {
  std::span<const char> output = session.PendingOutput();
  return std::string(output.begin(), output.end());
}

netlib::ApplicationResponse Reply(char payload) {
  return netlib::BinaryResponse{.payload = {payload}};
}

bool TestOrderingAndDrain() {
  std::unique_ptr<netlib::ProtocolSession> session =
      netlib::CreateCodecSession(netlib::CreateLengthPrefixedCodec(32));
  std::string input = Wire('a');
  netlib::SessionInputResult partial =
      session->OnInput(std::span(input).first(2));
  if (!Check(partial.status == netlib::SessionInputStatus::kNeedMore &&
                 partial.required_bytes == 4 && partial.requests.empty(),
             "partial frame lost its read target")) {
    return false;
  }
  std::vector<netlib::RequestContext> contexts;
  for (int i = 0; i != 3; ++i) {
    netlib::SessionInputResult decoded = session->OnInput(input);
    if (!Check(decoded.status == netlib::SessionInputStatus::kProgress &&
                   decoded.consumed_bytes == input.size() &&
                   decoded.requests.size() == 1 &&
                   decoded.requests[0].context.request_id ==
                       static_cast<std::uint64_t>(i) &&
                   !decoded.requests[0].context.stream_id.has_value(),
               "Codec adapter did not assign local request identities")) {
      return false;
    }
    contexts.push_back(decoded.requests[0].context);
    input.back() = 'z';
    if (!Check(decoded.requests[0].payload ==
                   std::vector<char>{i == 0 ? 'a' : 'z'},
               "Session request aliases the receive buffer")) {
      return false;
    }
  }
  session->BeginDrain();
  if (!Check(!session->WantsInput() && !session->ShouldClose(),
             "drain must wait for accepted requests")) {
    return false;
  }
  if (!Check(
          session->OnResponse(contexts[2], Reply('c'),
                              netlib::SessionResponseMode::kUnordered, false) ==
                  netlib::SessionResponseStatus::kQueued &&
              Output(*session) == Wire('c') &&
              session->OnResponse(contexts[2], Reply('x'),
                                  netlib::SessionResponseMode::kOrdered,
                                  false) ==
                  netlib::SessionResponseStatus::kStale,
          "unordered completion did not retain a duplicate placeholder")) {
    return false;
  }
  session->ConsumeOutput(5);
  if (!Check(session->OnResponse(
                 contexts[1], Reply('b'), netlib::SessionResponseMode::kOrdered,
                 false) == netlib::SessionResponseStatus::kQueued &&
                 session->PendingOutput().empty(),
             "later ordered response escaped the pipeline")) {
    return false;
  }
  return Check(
      session->OnResponse(contexts[0], Reply('a'),
                          netlib::SessionResponseMode::kOrdered,
                          false) == netlib::SessionResponseStatus::kQueued &&
          Output(*session) == Wire('a') + Wire('b') && session->ShouldClose(),
      "drain did not finish after the contiguous completion chain");
}

bool TestBoundsAndPartialOutput() {
  std::unique_ptr<netlib::ProtocolSession> session = netlib::CreateCodecSession(
      netlib::CreateLengthPrefixedCodec(32),
      {.max_pending_responses = 1, .max_pending_output_bytes = 8});
  const std::string input = Wire('a');
  for (int i = 0; i != 3; ++i) {
    session->OnInput(input);
  }
  const netlib::RequestContext first{.request_id = 0,
                                     .stream_id = std::nullopt};
  const netlib::RequestContext second{.request_id = 1,
                                      .stream_id = std::nullopt};
  const netlib::RequestContext third{.request_id = 2,
                                     .stream_id = std::nullopt};
  if (!Check(session->OnResponse(
                 third, Reply('c'), netlib::SessionResponseMode::kOrdered,
                 false) == netlib::SessionResponseStatus::kQueued &&
                 session->OnResponse(
                     second, Reply('b'), netlib::SessionResponseMode::kOrdered,
                     false) == netlib::SessionResponseStatus::kError,
             "pending response count was not bounded")) {
    return false;
  }
  session->OnResponse(first, Reply('a'), netlib::SessionResponseMode::kOrdered,
                      false);
  session->ConsumeOutput(2);
  // Three unsent bytes plus two five-byte responses exceed the eight-byte cap.
  return Check(
      session->OnResponse(second, Reply('b'),
                          netlib::SessionResponseMode::kOrdered,
                          false) == netlib::SessionResponseStatus::kError &&
          Output(*session) == Wire('a').substr(2) + Wire('b') &&
          session->ProduceOutput(8) && !session->ProduceOutput(7) &&
          !netlib::CreateCodecSession(nullptr) &&
          !netlib::CreateCodecSession(netlib::CreateLengthPrefixedCodec(),
                                      {.max_pending_responses = 0}),
      "partial output, encoding rollback or option bounds failed");
}

bool TestHttpTimeoutAndClose() {
  std::unique_ptr<netlib::ProtocolSession> session =
      netlib::CreateCodecSession(netlib::CreateHttp1Codec());
  const std::string input = "GET / HTTP/1.1\r\nHost: test\r\n\r\n";
  netlib::SessionInputResult first = session->OnInput(input);
  netlib::SessionInputResult second = session->OnInput(input);
  if (!Check(
          first.requests.size() == 1 && second.requests.size() == 1 &&
              std::get<netlib::HttpRequestMetadata>(first.requests[0].metadata)
                      .target == "/",
          "HTTP metadata was not owned by Session requests")) {
    return false;
  }
  netlib::HttpResponse later{.headers = {}, .body = {'b'}};
  session->OnResponse(second.requests[0].context, std::move(later),
                      netlib::SessionResponseMode::kOrdered, false);
  return Check(
      session->PendingOutput().empty() &&
          session->OnTimeout(first.requests[0].context) ==
              netlib::SessionResponseStatus::kQueued &&
          Output(*session).starts_with("HTTP/1.1 504 ") &&
          Output(*session).find("HTTP/1.1 200 ") == std::string::npos &&
          !session->WantsInput() && session->ShouldClose(),
      "HTTP timeout must discard replies after its terminal response");
}

bool TestRpcIdentity() {
  std::unique_ptr<netlib::ProtocolSession> session =
      netlib::CreateCodecSession(netlib::CreateRpcCodec());
  netlib::RpcFrameMetadata metadata{
      .request_id = 123456, .service_name = "S", .method_name = "M"};
  const std::vector<char> payload{'r'};
  std::vector<char> wire;
  if (!netlib::EncodeRpcFrame(metadata, payload, &wire)) {
    return false;
  }
  netlib::SessionInputResult request = session->OnInput(wire);
  if (!Check(
          request.requests.size() == 1 &&
              request.requests[0].context.request_id == 0 &&
              std::get<netlib::RpcFrameMetadata>(request.requests[0].metadata)
                      .request_id == 123456,
          "RPC wire identity was conflated with local request identity")) {
    return false;
  }
  netlib::RpcResponse response{.metadata = metadata, .payload = {'s'}};
  response.metadata.message_type = netlib::RpcMessageType::kResponse;
  if (session->OnResponse(request.requests[0].context, std::move(response),
                          netlib::SessionResponseMode::kOrdered,
                          false) != netlib::SessionResponseStatus::kQueued) {
    return false;
  }
  std::unique_ptr<netlib::ProtocolCodec> decoder = netlib::CreateRpcCodec();
  netlib::DecodeResult decoded = decoder->Decode(session->PendingOutput());
  return Check(
      decoded.status == netlib::DecodeStatus::kFrameReady &&
          std::get<netlib::RpcFrameMetadata>(decoded.metadata).request_id ==
              123456 &&
          session->OnTimeout(request.requests[0].context) ==
              netlib::SessionResponseStatus::kError,
      "RPC response encoding or unsupported timeout policy changed");
}

}  // namespace

int main() {
  return TestOrderingAndDrain() && TestBoundsAndPartialOutput() &&
                 TestHttpTimeoutAndClose() && TestRpcIdentity()
             ? 0
             : 1;
}
