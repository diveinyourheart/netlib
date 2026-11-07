#ifndef NETLIB_INCLUDE_NETLIB_FRAME_MESSAGE_H_
#define NETLIB_INCLUDE_NETLIB_FRAME_MESSAGE_H_

#include <cstdint>
#include <vector>

#include "netlib/connection_handle.h"
#include "netlib/protocol_message.h"

namespace netlib {

// Owns a complete transport frame after it leaves a connection read buffer.
struct FrameMessage {
  std::uint64_t connection_id = 0;
  std::uint64_t sequence = 0;
  ConnectionHandle connection;
  FrameMetadata metadata;
  std::vector<char> payload;
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_FRAME_MESSAGE_H_
