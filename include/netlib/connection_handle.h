#ifndef NETLIB_INCLUDE_NETLIB_CONNECTION_HANDLE_H_
#define NETLIB_INCLUDE_NETLIB_CONNECTION_HANDLE_H_

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "netlib/protocol_message.h"

namespace netlib {

// A weak, thread-safe handle for routing responses back to a live connection.
// Respond() is safe to call concurrently; false means the connection closed or
// its response queue is full.
class ConnectionHandle {
 public:
  ConnectionHandle() = default;
  ConnectionHandle(const ConnectionHandle&) = default;
  ConnectionHandle& operator=(const ConnectionHandle&) = default;
  ConnectionHandle(ConnectionHandle&&) noexcept = default;
  ConnectionHandle& operator=(ConnectionHandle&&) noexcept = default;
  ~ConnectionHandle() = default;

  std::uint64_t id() const;
  bool valid() const;
  // sequence must equal FrameMessage::sequence. Each request must produce
  // exactly one accepted response. The owning Session applies wire ordering
  // (legacy Codecs restore pipeline order; multiplexed protocols route
  // streams).
  bool Respond(std::uint64_t sequence, ApplicationResponse response,
               bool close_after_write = false) const;
  bool Respond(std::uint64_t sequence, std::vector<char> payload,
               bool close_after_write = false) const {
    return Respond(sequence, BinaryResponse{.payload = std::move(payload)},
                   close_after_write);
  }
  // For legacy Codecs, bypasses pipeline ordering and appends immediately. The
  // request is still completed and its timeout cancelled. Native Sessions
  // apply their protocol's ordering rules to this hint.
  bool RespondUnordered(std::uint64_t sequence, ApplicationResponse response,
                        bool close_after_write = false) const;
  bool RespondUnordered(std::uint64_t sequence, std::vector<char> payload,
                        bool close_after_write = false) const {
    return RespondUnordered(sequence,
                            BinaryResponse{.payload = std::move(payload)},
                            close_after_write);
  }

 private:
  class State;
  friend class SubReactor;

  enum class ResponseMode : std::uint8_t {
    kOrdered,
    kUnordered,
  };
  using Responder = std::function<bool(
      std::uint64_t, std::uint64_t, ApplicationResponse, bool, ResponseMode)>;

  ConnectionHandle(std::uint64_t id,
                   std::shared_ptr<const std::atomic<bool>> alive,
                   Responder responder);

  std::shared_ptr<const State> state_;
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_CONNECTION_HANDLE_H_
