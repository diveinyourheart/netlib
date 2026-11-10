#ifndef NETLIB_INCLUDE_NETLIB_PROTOCOL_SESSION_H_
#define NETLIB_INCLUDE_NETLIB_PROTOCOL_SESSION_H_

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "netlib/protocol_codec.h"
#include "netlib/request_context.h"

namespace netlib {

struct SessionRequest {
  RequestContext context;
  FrameMetadata metadata;
  std::vector<char> payload;
};

enum class SessionInputStatus : std::uint8_t {
  kNeedMore,
  kProgress,
  kProtocolError,
};

struct SessionInputResult {
  SessionInputStatus status = SessionInputStatus::kNeedMore;
  // kProgress must consume positive, bounded bytes, with zero or more requests.
  std::size_t consumed_bytes = 0;
  // kNeedMore must consume nothing and produce no requests. Zero means unknown;
  // otherwise this is a total unread-byte target strictly larger than input.
  std::size_t required_bytes = 0;
  std::vector<SessionRequest> requests;
  // Already-dispatched requests cancelled by the peer/protocol. Reactor removes
  // their timers; late Worker replies are rejected by its trusted request
  // table.
  std::vector<RequestContext> cancelled_requests{};
};

enum class SessionResponseMode : std::uint8_t { kOrdered, kUnordered };

enum class SessionResponseStatus : std::uint8_t {
  kQueued,
  kStale,
  kClosed,
  kError,
};

struct ProtocolSessionOptions {
  std::size_t max_pending_responses = 4096;
  std::size_t max_pending_output_bytes =
      kFrameLengthPrefixSize + kMaxFramePayloadSize;
};

// One instance per connection, accessed exclusively by its Reactor thread.
// Input requests own their bytes. IDs start at zero and increase by one for
// each emitted request; stream IDs are separate. Worker threads never touch a
// Session. ResponseMode is a legacy ordering hint; a multiplexed Session must
// route by context and apply its own protocol ordering rules.
class ProtocolSession {
 public:
  using Clock = std::chrono::steady_clock;
  ProtocolSession() = default;
  ProtocolSession(const ProtocolSession&) = delete;
  ProtocolSession& operator=(const ProtocolSession&) = delete;
  ProtocolSession(ProtocolSession&&) = delete;
  ProtocolSession& operator=(ProtocolSession&&) = delete;
  virtual ~ProtocolSession() = default;

  virtual SessionInputResult OnInput(std::span<const char> input) = 0;
  virtual SessionResponseStatus OnResponse(const RequestContext& context,
                                           ApplicationResponse response,
                                           SessionResponseMode mode,
                                           bool close_after_write) = 0;
  virtual SessionResponseStatus OnTimeout(const RequestContext& context) = 0;
  // Stop accepting new work; finish accepted requests and protocol shutdown.
  // This hook does not change the immediate SubReactor::Stop() API.
  virtual void BeginDrain() = 0;

  // May generate control output without a business response. byte_budget bounds
  // the total PendingOutput, including bytes already buffered. False is fatal.
  // True with empty output may mean waiting for peer input (e.g. a window).
  virtual bool ProduceOutput(std::size_t byte_budget) = 0;
  // Protocol output can also close/reset streams. Drain these owner-thread
  // events after ProduceOutput so their timers do not wait for more peer input.
  virtual std::vector<RequestContext> TakeCancelledRequests() { return {}; }
  // Owner-thread deadlines. Reactor arms only the earliest deadline, and calls
  // OnDeadline after validating the alarm generation. No polling thread needed.
  virtual std::optional<Clock::time_point> NextDeadline() const {
    return std::nullopt;
  }
  virtual void OnDeadline(Clock::time_point) {}
  // The span remains valid until the next non-const Session call. Socket writes
  // consume only bytes actually sent; ConsumeOutput must not exceed span size.
  virtual std::span<const char> PendingOutput() const = 0;
  virtual void ConsumeOutput(std::size_t size) = 0;
  virtual bool WantsInput() const = 0;
  // Reactor closes only after PendingOutput has been sent completely.
  virtual bool ShouldClose() const = 0;
  // Opt in to TCP half-close after ShouldClose and all output is sent. Reactor
  // discards trailing input until peer EOF, a byte limit, or a fixed deadline.
  // Fatal/legacy Sessions retain immediate close by default.
  virtual bool ShouldHalfClose() const { return false; }
  // Positive and constant throughout this Session's lifetime.
  virtual std::size_t MaxInputBufferSize() const = 0;
};

using ProtocolSessionFactory = std::function<std::unique_ptr<ProtocolSession>(
    ProtocolSessionOptions options)>;

// Preserves legacy Codec ordering, timeout and close policies. Invalid options
// or an invalid Codec return nullptr. No changes to Codec subclasses required.
std::unique_ptr<ProtocolSession> CreateCodecSession(
    std::unique_ptr<ProtocolCodec> codec, ProtocolSessionOptions options = {});

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_PROTOCOL_SESSION_H_
