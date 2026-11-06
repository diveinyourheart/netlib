#ifndef NETLIB_INCLUDE_NETLIB_PROTOCOL_CODEC_H_
#define NETLIB_INCLUDE_NETLIB_PROTOCOL_CODEC_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <vector>

#include "netlib/frame_protocol.h"

namespace netlib {

enum class DecodeStatus : std::uint8_t {
  kNeedMore,
  kFrameReady,
  kProtocolError,
};

struct DecodeResult {
  DecodeStatus status = DecodeStatus::kNeedMore;
  std::size_t consumed_bytes = 0;
  std::size_t payload_offset = 0;
  std::size_t payload_size = 0;
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
  virtual bool Encode(std::span<const char> payload,
                      std::vector<char>* output) = 0;
  // Maximum unconsumed bytes allowed while assembling one decoded frame.
  virtual std::size_t MaxDecodeBufferSize() const = 0;
};

using ProtocolCodecFactory = std::function<std::unique_ptr<ProtocolCodec>()>;

std::unique_ptr<ProtocolCodec> CreateLengthPrefixedCodec(
    std::uint32_t max_payload_size = kMaxFramePayloadSize);

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_PROTOCOL_CODEC_H_
