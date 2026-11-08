#include "netlib/rpc_codec.h"

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "netlib/connection_handle.h"
#include "netlib/frame_message.h"
#include "netlib/main_reactor.h"
#include "netlib/platform.h"
#include "netlib/sub_reactor.h"
#include "netlib/thread_pool.h"
#include "netlib/unique_fd.h"

namespace {

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::fprintf(stderr, "%s\n", message);
  }
  return condition;
}

// Independent wire fixture: length=34, header=31, payload=3. Protobuf bytes
// contain a high bit, so parsing cannot accidentally depend on signed char.
std::vector<char> GoldenRequest() {
  return {0,
          0,
          0,
          34,
          'N',
          'R',
          'P',
          'C',
          1,
          0,
          0,
          31,
          1,
          2,
          3,
          4,
          5,
          6,
          7,
          8,
          1,
          0,
          0,
          0,
          0,
          1,
          0,
          1,
          'S',
          'M',
          0,
          7,
          0,
          1,
          'x',
          8,
          static_cast<char>(0x96),
          1};
}

netlib::RpcFrameMetadata RequestMetadata() {
  netlib::RpcFrameMetadata metadata;
  metadata.request_id = 0x0102030405060708;
  metadata.service_name = "S";
  metadata.method_name = "M";
  metadata.extensions = {{.type = 7, .value = {'x'}}};
  return metadata;
}

bool IsGoldenMetadata(const netlib::FrameMetadata& decoded) {
  const netlib::RpcFrameMetadata* metadata =
      std::get_if<netlib::RpcFrameMetadata>(&decoded);
  return metadata != nullptr &&
         metadata->version == netlib::kRpcProtocolVersion &&
         metadata->message_type == netlib::RpcMessageType::kRequest &&
         metadata->request_id == 0x0102030405060708 &&
         metadata->serialization == netlib::RpcSerialization::kProtobuf &&
         metadata->compression == netlib::RpcCompression::kNone &&
         metadata->error_code == 0 && metadata->service_name == "S" &&
         metadata->method_name == "M" && metadata->extensions.size() == 1 &&
         metadata->extensions[0].type == 7 &&
         metadata->extensions[0].value == std::vector<char>{'x'};
}

bool TestWireFormatAndFragments() {
  const std::vector<char> wire = GoldenRequest();
  const std::span<const char> payload(wire.data() + 35, 3);
  std::vector<char> encoded;
  if (!Check(netlib::EncodeRpcFrame(RequestMetadata(), payload, &encoded) &&
                 encoded == wire,
             "RPC encoder disagrees with independent big-endian fixture")) {
    return false;
  }

  // Test every split, including splits inside the fixed header, names and TLV.
  for (std::size_t split = 0; split < wire.size(); ++split) {
    std::unique_ptr<netlib::ProtocolCodec> codec = netlib::CreateRpcCodec();
    netlib::DecodeResult result =
        codec->Decode(std::span<const char>(wire).first(split));
    if (!Check(result.status == netlib::DecodeStatus::kNeedMore &&
                   result.required_bytes > split &&
                   result.required_bytes <= wire.size(),
               "valid partial RPC frame rejected or invalid read target")) {
      return false;
    }
    result = codec->Decode(wire);
    if (!Check(result.status == netlib::DecodeStatus::kFrameReady &&
                   result.consumed_bytes == wire.size() &&
                   result.payload_offset == 35 && result.payload_size == 3 &&
                   IsGoldenMetadata(result.metadata),
               "fragmented RPC frame decoded incorrectly")) {
      return false;
    }
  }
  std::unique_ptr<netlib::ProtocolCodec> codec = netlib::CreateRpcCodec();
  for (std::size_t size = 0; size < wire.size(); ++size) {
    if (!Check(codec->Decode(std::span<const char>(wire).first(size)).status ==
                   netlib::DecodeStatus::kNeedMore,
               "byte-at-a-time RPC decode failed")) {
      return false;
    }
  }
  return Check(codec->Decode(wire).status == netlib::DecodeStatus::kFrameReady,
               "byte-at-a-time RPC decode did not complete");
}

bool TestCoalescingAndOwnership() {
  std::vector<char> wire = GoldenRequest();
  const std::size_t first_size = wire.size();
  netlib::RpcFrameMetadata second = RequestMetadata();
  second.request_id = 42;
  second.extensions = {{.type = 8, .value = {}},
                       {.type = 9, .value = {0, 'y'}},
                       {.type = 8, .value = {'z'}}};
  if (!netlib::EncodeRpcFrame(second, {}, &wire)) {
    return false;
  }
  std::unique_ptr<netlib::ProtocolCodec> codec = netlib::CreateRpcCodec();
  const netlib::DecodeResult first = codec->Decode(wire);
  const netlib::DecodeResult next =
      codec->Decode(std::span<const char>(wire).subspan(first_size));
  const netlib::RpcFrameMetadata* metadata =
      std::get_if<netlib::RpcFrameMetadata>(&next.metadata);
  // Destroy the input and decode again: both returned headers must still own
  // their names/extensions, and the decoder must have reset between frames.
  wire.clear();
  wire.shrink_to_fit();
  return Check(
      first.status == netlib::DecodeStatus::kFrameReady &&
          first.consumed_bytes == first_size &&
          IsGoldenMetadata(first.metadata) &&
          next.status == netlib::DecodeStatus::kFrameReady &&
          next.payload_size == 0 && metadata != nullptr &&
          metadata->request_id == 42 && metadata->extensions.size() == 3 &&
          metadata->extensions[0].value.empty() &&
          metadata->extensions[1].value == std::vector<char>({0, 'y'}) &&
          metadata->extensions[2].type == 8 &&
          metadata->extensions[2].value == std::vector<char>{'z'} &&
          codec->Decode(GoldenRequest()).status ==
              netlib::DecodeStatus::kFrameReady,
      "coalesced frames, optional TLVs, or metadata ownership failed");
}

bool Rejects(const std::vector<char>& wire) {
  std::unique_ptr<netlib::ProtocolCodec> codec = netlib::CreateRpcCodec();
  return codec->Decode(wire).status == netlib::DecodeStatus::kProtocolError &&
         codec->Decode(GoldenRequest()).status ==
             netlib::DecodeStatus::kProtocolError;
}

bool TestInvalidFrames() {
  // Byte offsets are deliberately independent of the decoder implementation.
  struct Mutation {
    std::size_t offset = 0;
    unsigned char value = 0;
  };
  const std::array<Mutation, 17> mutations{{
      {3, 23},     // Total shorter than fixed header.
      {4, 'X'},    // Magic.
      {8, 2},      // Unsupported version.
      {9, 0x80},   // Reserved flags.
      {10, 0xff},  // Header longer than frame / header limit.
      {11, 23},    // Header shorter than fixed fields.
      {11, 35},    // Header longer than frame.
      {11, 32},    // Extra byte cannot form a TLV.
      {20, 2},     // Unknown serializer.
      {21, 3},     // Unknown compressor.
      {23, 1},     // A request cannot carry a response error.
      {24, 0xff},  // Names extend beyond header.
      {25, 0},     // Empty request service.
      {27, 0},     // Empty request method.
      {30, 0x80},  // Unsupported mandatory TLV.
      {31, 0},     // Reserved TLV type zero.
      {33, 2},     // TLV value extends into the payload.
  }};
  for (const Mutation& mutation : mutations) {
    std::vector<char> wire = GoldenRequest();
    wire[mutation.offset] = static_cast<char>(mutation.value);
    if (!Check(Rejects(wire), "malformed RPC header was accepted")) {
      return false;
    }
  }
  return Check(Rejects({0, 0, 0, 0}) &&
                   Rejects({static_cast<char>(0xff), static_cast<char>(0xff),
                            static_cast<char>(0xff), static_cast<char>(0xff)}),
               "invalid total length was not rejected at the length prefix");
}

bool TestLimitsAndResponses() {
  const netlib::RpcCodecOptions exact{
      .max_frame_bytes = 34,
      .max_header_bytes = 31,
  };
  std::unique_ptr<netlib::ProtocolCodec> codec = netlib::CreateRpcCodec(exact);
  const std::vector<char> wire = GoldenRequest();
  if (!Check(
          codec && codec->MaxDecodeBufferSize() == 38 &&
              codec->Decode(wire).status == netlib::DecodeStatus::kFrameReady,
          "exact RPC length limits rejected")) {
    return false;
  }
  codec =
      netlib::CreateRpcCodec({.max_frame_bytes = 33, .max_header_bytes = 31});
  if (!Check(codec->Decode(std::span<const char>(wire).first(4)).status ==
                 netlib::DecodeStatus::kProtocolError,
             "oversized RPC frame was not rejected before body arrival")) {
    return false;
  }
  codec =
      netlib::CreateRpcCodec({.max_frame_bytes = 34, .max_header_bytes = 30});
  if (!Check(codec->Decode(std::span<const char>(wire).first(28)).status ==
                 netlib::DecodeStatus::kProtocolError,
             "oversized RPC header was not rejected before names arrived")) {
    return false;
  }
  if (!Check(
          !netlib::CreateRpcCodec({.max_frame_bytes = 0}) &&
              !netlib::CreateRpcCodec({.max_header_bytes = 23}) &&
              !netlib::CreateRpcCodec({.max_header_bytes = 65536}) &&
              !netlib::CreateRpcCodec(
                  {.max_frame_bytes = 24, .max_header_bytes = 25}) &&
              !netlib::CreateRpcCodec(
                  {.max_frame_bytes = std::numeric_limits<std::size_t>::max()}),
          "invalid RPC codec options accepted")) {
    return false;
  }

  netlib::RpcResponse response;
  response.metadata.request_id = std::numeric_limits<std::uint64_t>::max();
  response.metadata.error_code = 37;
  codec = netlib::CreateRpcCodec();
  // Requests may carry opaque compressed bytes. Never run protobuf parsing or
  // decompression here: the codec only interprets the envelope.
  for (netlib::RpcCompression compression :
       {netlib::RpcCompression::kNone, netlib::RpcCompression::kGzip,
        netlib::RpcCompression::kZstd}) {
    response.metadata.compression = compression;
    response.payload = {0, static_cast<char>(0xff)};
    std::vector<char> output;
    if (!codec->Encode(response, &output)) {
      return Check(false, "RPC response encode failed");
    }
    const netlib::DecodeResult result = codec->Decode(output);
    const netlib::RpcFrameMetadata* metadata =
        std::get_if<netlib::RpcFrameMetadata>(&result.metadata);
    if (!Check(
            result.status == netlib::DecodeStatus::kFrameReady &&
                result.payload_offset == 28 && result.payload_size == 2 &&
                metadata != nullptr &&
                metadata->message_type == netlib::RpcMessageType::kResponse &&
                metadata->request_id == response.metadata.request_id &&
                metadata->error_code == 37 &&
                metadata->compression == compression &&
                metadata->service_name.empty() &&
                metadata->method_name.empty() &&
                !codec->ShouldCloseAfterWrite(response) && output[28] == 0 &&
                output[29] == static_cast<char>(0xff),
            "RPC response metadata or opaque bytes changed")) {
      return false;
    }
  }
  response.payload.clear();
  std::vector<char> empty_response;
  std::unique_ptr<netlib::ProtocolCodec> minimum =
      netlib::CreateRpcCodec({.max_frame_bytes = 24, .max_header_bytes = 24});
  return Check(minimum->Encode(response, &empty_response) &&
                   empty_response.size() == 28 &&
                   minimum->Decode(empty_response).payload_size == 0,
               "minimum empty RPC response failed");
}

bool TestEncodeFailureIsAtomic() {
  std::unique_ptr<netlib::ProtocolCodec> codec = netlib::CreateRpcCodec();
  const std::vector<char> original{'p', 'r', 'e'};
  std::vector<char> output = original;
  netlib::RpcResponse response;
  response.metadata.message_type = netlib::RpcMessageType::kRequest;
  if (!Check(!codec->Encode(netlib::BinaryResponse{}, &output) &&
                 !codec->Encode(response, &output) && output == original &&
                 !netlib::EncodeRpcFrame(RequestMetadata(), {}, nullptr),
             "wrong response type modified output")) {
    return false;
  }
  const std::vector<char> wire = GoldenRequest();
  const std::span<const char> payload(wire.data() + 35, 3);
  const netlib::RpcCodecOptions tight{
      .max_frame_bytes = 33,
      .max_header_bytes = 31,
  };
  if (!Check(
          !netlib::EncodeRpcFrame(RequestMetadata(), payload, &output, tight) &&
              output == original,
          "oversized payload modified output")) {
    return false;
  }
  for (int mutation = 0; mutation < 9; ++mutation) {
    netlib::RpcFrameMetadata metadata = RequestMetadata();
    switch (mutation) {
      case 0:
        metadata.version = 2;
        break;
      case 1:
        metadata.message_type = static_cast<netlib::RpcMessageType>(2);
        break;
      case 2:
        metadata.serialization = static_cast<netlib::RpcSerialization>(2);
        break;
      case 3:
        metadata.compression = static_cast<netlib::RpcCompression>(3);
        break;
      case 4:
        metadata.error_code = 1;
        break;
      case 5:
        metadata.service_name.clear();
        break;
      case 6:
        metadata.service_name.resize(65536, 's');
        break;
      case 7:
        metadata.extensions[0].type = 0x8001;
        break;
      case 8:
        metadata.extensions[0].value.resize(65536, 'x');
        break;
    }
    if (!Check(!netlib::EncodeRpcFrame(metadata, payload, &output) &&
                   output == original,
               "invalid metadata was encoded or modified output")) {
      return false;
    }
  }
  if (!netlib::EncodeRpcFrame(RequestMetadata(), payload, &output)) {
    return false;
  }
  return Check(output.size() == original.size() + wire.size() &&
                   std::vector<char>(output.begin() + 3, output.end()) == wire,
               "RPC encoder did not append to existing output");
}

bool SendAll(netlib::SocketHandle fd, std::span<const char> bytes) {
  while (!bytes.empty()) {
    const std::ptrdiff_t result =
        netlib::platform::Send(fd, bytes.data(), bytes.size());
    if (result > 0) {
      bytes = bytes.subspan(static_cast<std::size_t>(result));
    } else if (result == 0 ||
               !netlib::platform::Interrupted(netlib::platform::LastError())) {
      return false;
    }
  }
  return true;
}

bool ReceiveAll(netlib::SocketHandle fd, std::span<char> bytes) {
  while (!bytes.empty()) {
    const std::ptrdiff_t result =
        netlib::platform::Receive(fd, bytes.data(), bytes.size());
    if (result > 0) {
      bytes = bytes.subspan(static_cast<std::size_t>(result));
    } else if (result == 0 ||
               !netlib::platform::Interrupted(netlib::platform::LastError())) {
      return false;
    }
  }
  return true;
}

bool TestReactorRoundTrip() {
  std::atomic<bool> handler_ok{true};
  std::mutex mutex;
  std::condition_variable condition;
  bool second_responded = false;
  std::unique_ptr<netlib::ThreadPool> workers =
      netlib::ThreadPool::Create(2, 16, [&](netlib::FrameMessage message) {
        const netlib::RpcFrameMetadata* metadata =
            std::get_if<netlib::RpcFrameMetadata>(&message.metadata);
        if (metadata == nullptr || metadata->service_name != "S" ||
            metadata->method_name != "M" || message.sequence > 1 ||
            metadata->request_id != (message.sequence == 0 ? 900 : 7) ||
            metadata->extensions.size() != 1 ||
            metadata->extensions[0].value != std::vector<char>{'x'} ||
            message.payload != std::vector<char>({0, 'p'})) {
          handler_ok.store(false);
          return;
        }
        if (message.sequence == 0) {
          std::unique_lock<std::mutex> lock(mutex);
          if (!condition.wait_for(lock, std::chrono::seconds(2),
                                  [&] { return second_responded; })) {
            handler_ok.store(false);
          }
        }
        netlib::RpcResponse response;
        response.metadata.request_id = metadata->request_id;
        response.metadata.error_code = message.sequence == 0 ? 37 : 0;
        response.payload = std::move(message.payload);
        if (!message.connection.Respond(message.sequence,
                                        std::move(response))) {
          handler_ok.store(false);
        }
        if (message.sequence == 1) {
          std::lock_guard<std::mutex> lock(mutex);
          second_responded = true;
          condition.notify_all();
        }
      });
  if (!workers) {
    return false;
  }
  std::unique_ptr<netlib::SubReactor> sub = netlib::SubReactor::Create(
      16, *workers, {}, [] { return netlib::CreateRpcCodec(); });
  if (!sub) {
    return false;
  }
  std::unique_ptr<netlib::MainReactor> main =
      netlib::MainReactor::Create(0, {sub.get()});
  if (!main) {
    return false;
  }
  std::thread sub_thread([&] { sub->Run(); });
  std::thread main_thread([&] { main->Run(); });

  netlib::UniqueFd client(netlib::platform::CreateTcpSocket(false));
  bool success = client &&
                 netlib::platform::SetReceiveTimeout(client.get(), 3000) &&
                 netlib::platform::ConnectLoopback(client.get(), main->port());
  std::vector<char> requests;
  std::vector<char> expected;
  const std::vector<char> payload{0, 'p'};
  for (std::uint64_t id : {900, 7}) {
    netlib::RpcFrameMetadata request = RequestMetadata();
    request.request_id = id;
    success = netlib::EncodeRpcFrame(request, payload, &requests) && success;
    netlib::RpcResponse response;
    response.metadata.request_id = id;
    response.metadata.error_code = id == 900 ? 37 : 0;
    success = netlib::EncodeRpcFrame(response.metadata, payload, &expected) &&
              success;
  }
  // Split the length prefix, then send the remainder and next frame together.
  success = success &&
            SendAll(client.get(), std::span<const char>(requests).first(2)) &&
            SendAll(client.get(), std::span<const char>(requests).subspan(2));
  std::vector<char> received(expected.size());
  success = success && ReceiveAll(client.get(), received) &&
            received == expected && handler_ok.load();

  client.Reset();
  main->Stop();
  sub->Stop();
  main_thread.join();
  sub_thread.join();
  workers->Stop();
  return Check(success, "RPC Reactor metadata/response round trip failed");
}

bool TestRequestTimeout() {
  std::mutex mutex;
  std::condition_variable condition;
  std::array<netlib::ConnectionHandle, 2> handles;
  std::size_t received_requests = 0;
  bool handler_ok = true;
  std::unique_ptr<netlib::ThreadPool> workers =
      netlib::ThreadPool::Create(1, 16, [&](netlib::FrameMessage message) {
        const netlib::RpcFrameMetadata* metadata =
            std::get_if<netlib::RpcFrameMetadata>(&message.metadata);
        std::lock_guard<std::mutex> lock(mutex);
        if (metadata == nullptr || message.sequence >= handles.size() ||
            metadata->request_id != (message.sequence == 0 ? 900 : 7)) {
          handler_ok = false;
        } else {
          handles[message.sequence] = message.connection;
        }
        ++received_requests;
        condition.notify_all();
        // Leave both requests unanswered; timeout must close the connection
        // without fabricating a response using the last decoded request's ID.
      });
  if (!workers) {
    return false;
  }
  const netlib::SubReactorOptions options{
      .request_timeout = std::chrono::milliseconds(100),
  };
  std::unique_ptr<netlib::SubReactor> sub = netlib::SubReactor::Create(
      options, *workers, {}, [] { return netlib::CreateRpcCodec(); });
  if (!sub) {
    return false;
  }
  std::unique_ptr<netlib::MainReactor> main =
      netlib::MainReactor::Create(0, {sub.get()});
  if (!main) {
    return false;
  }
  std::thread sub_thread([&] { sub->Run(); });
  std::thread main_thread([&] { main->Run(); });

  netlib::UniqueFd client(netlib::platform::CreateTcpSocket(false));
  bool success = client &&
                 netlib::platform::SetReceiveTimeout(client.get(), 3000) &&
                 netlib::platform::ConnectLoopback(client.get(), main->port());
  std::vector<char> requests;
  for (std::uint64_t id : {900, 7}) {
    netlib::RpcFrameMetadata request = RequestMetadata();
    request.request_id = id;
    success = netlib::EncodeRpcFrame(request, {}, &requests) && success;
  }
  success = success && SendAll(client.get(), requests);
  {
    std::unique_lock<std::mutex> lock(mutex);
    success = success && condition.wait_for(lock, std::chrono::seconds(3), [&] {
      return received_requests == handles.size();
    });
    success = success && handler_ok;
  }
  if (success) {
    char byte = 0;
    std::ptrdiff_t result = netlib::platform::Receive(client.get(), &byte, 1);
    while (result < 0 &&
           netlib::platform::Interrupted(netlib::platform::LastError())) {
      result = netlib::platform::Receive(client.get(), &byte, 1);
    }
    success = result == 0;
    std::lock_guard<std::mutex> lock(mutex);
    for (std::size_t sequence = 0; sequence < handles.size(); ++sequence) {
      success = success && !handles[sequence].valid() &&
                !handles[sequence].Respond(sequence, netlib::RpcResponse{});
    }
  }

  client.Reset();
  main->Stop();
  sub->Stop();
  main_thread.join();
  sub_thread.join();
  workers->Stop();
  return Check(success,
               "RPC timeout did not close without an uncorrelated reply");
}

}  // namespace

int main() {
  return TestWireFormatAndFragments() && TestCoalescingAndOwnership() &&
                 TestInvalidFrames() && TestLimitsAndResponses() &&
                 TestEncodeFailureIsAtomic() && TestReactorRoundTrip() &&
                 TestRequestTimeout()
             ? 0
             : 1;
}
