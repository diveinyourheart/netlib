#ifndef NETLIB_INCLUDE_NETLIB_FRAME_MESSAGE_H_
#define NETLIB_INCLUDE_NETLIB_FRAME_MESSAGE_H_

#include <cstdint>
#include <utility>
#include <vector>

#include "netlib/connection_handle.h"
#include "netlib/protocol_message.h"
#include "netlib/request_context.h"

namespace netlib {

// Owns a complete transport frame after it leaves a connection read buffer.
struct FrameMessage {
  std::uint64_t connection_id = 0;
  // Zero-based per-connection request order; return it with the response.
  std::uint64_t sequence = 0;
  ConnectionHandle connection;
  FrameMetadata metadata;
  std::vector<char> payload;
  // sequence mirrors request_context.request_id for existing handlers.
  RequestContext request_context;

  bool Respond(ApplicationResponse response,
               bool close_after_write = false) const {
    return connection.Respond(sequence, std::move(response), close_after_write);
  }
  bool Respond(std::vector<char> payload,
               bool close_after_write = false) const {
    return connection.Respond(sequence, std::move(payload), close_after_write);
  }
  // Skips pipeline response ordering. Use only when the protocol or handler
  // has its own ordering guarantee.
  bool RespondUnordered(ApplicationResponse response,
                        bool close_after_write = false) const {
    return connection.RespondUnordered(sequence, std::move(response),
                                       close_after_write);
  }
  bool RespondUnordered(std::vector<char> payload,
                        bool close_after_write = false) const {
    return connection.RespondUnordered(sequence, std::move(payload),
                                       close_after_write);
  }
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_FRAME_MESSAGE_H_
