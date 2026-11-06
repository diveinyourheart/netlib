#ifndef NETLIB_INCLUDE_NETLIB_FRAME_PROTOCOL_H_
#define NETLIB_INCLUDE_NETLIB_FRAME_PROTOCOL_H_

#include <cstddef>
#include <cstdint>

namespace netlib {

// Each frame is a 4-byte big-endian payload length followed by that payload.
inline constexpr std::size_t kFrameLengthPrefixSize = 4;
inline constexpr std::uint32_t kMaxFramePayloadSize = 16U * 1024U * 1024U;

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_FRAME_PROTOCOL_H_
