#ifndef NETLIB_INCLUDE_NETLIB_PROTOCOL_CODEC_H_
#define NETLIB_INCLUDE_NETLIB_PROTOCOL_CODEC_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "netlib/frame_protocol.h"
#include "netlib/protocol_message.h"

namespace netlib {

enum class DecodeStatus : std::uint8_t {
  kNeedMore,
  kFrameReady,
  kProtocolError,
};

struct DecodeResult {
  DecodeStatus status = DecodeStatus::kNeedMore;
  // For NeedMore, the total unread bytes required to reach the next decoder
  // state or complete the frame. Zero means the target is not known yet, as
  // while scanning an HTTP header for its delimiter.
  std::size_t required_bytes = 0;
  std::size_t consumed_bytes = 0;
  std::size_t payload_offset = 0;
  std::size_t payload_size = 0;
  FrameMetadata metadata;
};

// One instance is owned by each connection. Decode() receives the unconsumed
// input buffer. When a frame is ready, the caller consumes consumed_bytes and
// may call Decode() again with the remaining bytes. Encode() appends bytes to
// the output vector.
class ProtocolCodec {
 public:
  ProtocolCodec() = default;
  ProtocolCodec(const ProtocolCodec&) = delete;
  ProtocolCodec& operator=(const ProtocolCodec&) = delete;
  ProtocolCodec(ProtocolCodec&&) = delete;
  ProtocolCodec& operator=(ProtocolCodec&&) = delete;
  virtual ~ProtocolCodec() = default;

  virtual DecodeResult Decode(std::span<const char> input) = 0;
  virtual bool Encode(const ApplicationResponse& response,
                      std::vector<char>* output) = 0;
  virtual bool ShouldCloseAfterWrite(
      const ApplicationResponse& response) const = 0;
  // Returns a protocol-valid timeout response, or nullopt when the protocol
  // cannot represent one safely. SubReactor closes the connection afterward.
  virtual std::optional<ApplicationResponse> CreateTimeoutResponse() const = 0;
  // Maximum unconsumed bytes allowed while assembling one decoded frame.
  virtual std::size_t MaxDecodeBufferSize() const = 0;
};

using ProtocolCodecFactory = std::function<std::unique_ptr<ProtocolCodec>()>;

std::unique_ptr<ProtocolCodec> CreateLengthPrefixedCodec(
    std::uint32_t max_payload_size = kMaxFramePayloadSize);

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_PROTOCOL_CODEC_H_
