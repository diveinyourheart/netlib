#ifndef NETLIB_INCLUDE_NETLIB_FRAME_MESSAGE_H_
#define NETLIB_INCLUDE_NETLIB_FRAME_MESSAGE_H_

#include <cstdint>
#include <stop_token>
#include <utility>
#include <vector>

#include "netlib/connection_handle.h"
#include "netlib/protocol_message.h"
#include "netlib/request_context.h"

namespace netlib {

// Owns one complete protocol request after assembly. Multiplexed protocols may
// assemble it from several transport frames before dispatching to a Worker.
struct FrameMessage {
  std::uint64_t connection_id = 0;
  // Zero-based per-connection request order; return it with the response.
  std::uint64_t sequence = 0;
  ConnectionHandle connection;
  FrameMetadata metadata;
  std::vector<char> payload;
  // sequence mirrors request_context.request_id for existing handlers.
  RequestContext request_context;
  // Cooperative cancellation. Callbacks may run in Reactor, or immediately on
  // registration if already cancelled. Keep them short and nonblocking.
  std::stop_token stop_token{};
  // Internal lifetime charges. Application copies retain the original charge;
  // independently copied/expanded data is application-owned memory.
  std::shared_ptr<MemoryReservation> memory{};
  std::shared_ptr<MemoryReservation> worker_memory{};

  bool IsCancelled() const { return stop_token.stop_requested(); }

  bool Respond(ApplicationResponse response,
               bool close_after_write = false) const {
    return !IsCancelled() &&
           connection.Respond(sequence, std::move(response), close_after_write);
  }
  bool Respond(std::vector<char> response_payload,
               bool close_after_write = false) const {
    return !IsCancelled() &&
           connection.Respond(sequence, std::move(response_payload),
                              close_after_write);
  }
  // Skips pipeline response ordering. Use only when the protocol or handler
  // has its own ordering guarantee.
  bool RespondUnordered(ApplicationResponse response,
                        bool close_after_write = false) const {
    return !IsCancelled() &&
           connection.RespondUnordered(sequence, std::move(response),
                                       close_after_write);
  }
  bool RespondUnordered(std::vector<char> response_payload,
                        bool close_after_write = false) const {
    return !IsCancelled() &&
           connection.RespondUnordered(sequence, std::move(response_payload),
                                       close_after_write);
  }
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_FRAME_MESSAGE_H_
