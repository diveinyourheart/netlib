#ifndef NETLIB_INCLUDE_NETLIB_RPC_CODEC_H_
#define NETLIB_INCLUDE_NETLIB_RPC_CODEC_H_

#include <cstddef>
#include <memory>
#include <span>
#include <vector>

#include "netlib/protocol_codec.h"
#include "netlib/rpc_message.h"

namespace netlib {

inline constexpr std::size_t kRpcLengthPrefixSize = 4;
inline constexpr std::size_t kRpcFixedHeaderSize = 24;

struct RpcCodecOptions {
  // Includes the header and payload, excludes the four-byte length prefix.
  std::size_t max_frame_bytes = 16 * 1024 * 1024;
  // Includes the fixed header, names, and TLV extensions; at most 65535.
  std::size_t max_header_bytes = 16 * 1024;
};

// Decodes requests and responses. ProtocolCodec::Encode accepts RpcResponse.
// The payload is opaque: serialization and compression belong to the RPC layer.
std::unique_ptr<ProtocolCodec> CreateRpcCodec(RpcCodecOptions options = {});

// Shared wire encoder for clients (requests) and servers (responses). Appends
// only after validation succeeds. Input data must not alias the output vector.
bool EncodeRpcFrame(const RpcFrameMetadata& metadata,
                    std::span<const char> payload, std::vector<char>* output,
                    RpcCodecOptions options = {});

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_RPC_CODEC_H_
