#ifndef NETLIB_INCLUDE_NETLIB_RPC_MESSAGE_H_
#define NETLIB_INCLUDE_NETLIB_RPC_MESSAGE_H_

#include <cstdint>
#include <string>
#include <vector>

namespace netlib {

inline constexpr std::uint8_t kRpcProtocolVersion = 1;

enum class RpcMessageType : std::uint8_t {
  kRequest = 0,
  kResponse = 1,
};

enum class RpcSerialization : std::uint8_t {
  kProtobuf = 1,
};

// Describes payload bytes on the wire. The codec does not compress/decompress.
enum class RpcCompression : std::uint8_t {
  kNone = 0,
  kGzip = 1,
  kZstd = 2,
};

struct RpcHeaderExtension {
  // 1..0x7fff: optional, preserved even when unknown. The high bit marks a
  // mandatory extension; v1 currently rejects all such unsupported extensions.
  std::uint16_t type = 0;
  std::vector<char> value;
};

// Owns all header data independently of the connection's reusable read buffer.
struct RpcFrameMetadata {
  std::uint8_t version = kRpcProtocolVersion;
  RpcMessageType message_type = RpcMessageType::kRequest;
  // Correlates RPC calls on the wire, independently of FrameMessage::sequence.
  std::uint64_t request_id = 0;
  RpcSerialization serialization = RpcSerialization::kProtobuf;
  RpcCompression compression = RpcCompression::kNone;
  // Zero means success; requests must use zero. Nonzero response codes belong
  // to the RPC framework, not the transport codec.
  std::uint16_t error_code = 0;
  std::string service_name{};
  std::string method_name{};
  std::vector<RpcHeaderExtension> extensions{};
};

struct RpcResponse {
  RpcFrameMetadata metadata{.message_type = RpcMessageType::kResponse};
  std::vector<char> payload;
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_RPC_MESSAGE_H_
