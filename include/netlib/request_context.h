#ifndef NETLIB_INCLUDE_NETLIB_REQUEST_CONTEXT_H_
#define NETLIB_INCLUDE_NETLIB_REQUEST_CONTEXT_H_

#include <cstdint>
#include <optional>

namespace netlib {

// Local task identity is independent of wire-level IDs (RPC IDs, HTTP/2
// streams). The owning Reactor retains this context until response or timeout.
struct RequestContext {
  std::uint64_t request_id = 0;
  std::optional<std::uint32_t> stream_id;

  bool operator==(const RequestContext&) const = default;
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_REQUEST_CONTEXT_H_
