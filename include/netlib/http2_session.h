#ifndef NETLIB_INCLUDE_NETLIB_HTTP2_SESSION_H_
#define NETLIB_INCLUDE_NETLIB_HTTP2_SESSION_H_

#include <chrono>
#include <cstddef>
#include <memory>

#include "netlib/protocol_session.h"

namespace netlib {

struct Http2SessionOptions {
  std::size_t max_concurrent_streams = 128;
  // Decoded name/value bytes plus 32 bytes per field, including pseudo-fields.
  std::size_t max_header_bytes = 64 * 1024;
  std::size_t max_body_bytes = 16 * 1024 * 1024;
  // Session-owned request and response bodies share this connection budget.
  // Bodies already moved to Workers are governed by the Worker queue instead.
  std::size_t max_buffered_body_bytes = 32 * 1024 * 1024;
  std::size_t max_input_chunk_bytes = 16 * 1024;
  // Positive and at most 24 hours. Header also bounds preface/SETTINGS and
  // incomplete wire frames. Body is idle time; response is an absolute deadline
  // from application acceptance to serialization. Reactor bounds socket stalls.
  std::chrono::milliseconds header_timeout = std::chrono::seconds(10);
  std::chrono::milliseconds body_idle_timeout = std::chrono::seconds(30);
  std::chrono::milliseconds response_timeout = std::chrono::seconds(30);
  std::chrono::milliseconds drain_timeout = std::chrono::seconds(30);
};

// Explicit prior-knowledge, cleartext HTTP/2 server Session. The client preface
// must be passed intact, possibly fragmented. No TLS or automatic detection.
// Returns nullptr if HTTP/2 was disabled at build time or options are invalid.
// Public headers do not require nghttp2 headers or types.
std::unique_ptr<ProtocolSession> CreateHttp2Session(
    ProtocolSessionOptions limits = {}, Http2SessionOptions options = {});

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_HTTP2_SESSION_H_
