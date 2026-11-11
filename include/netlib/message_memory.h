#ifndef NETLIB_INCLUDE_NETLIB_MESSAGE_MEMORY_H_
#define NETLIB_INCLUDE_NETLIB_MESSAGE_MEMORY_H_

#include <cstddef>

#include "netlib/protocol_message.h"

namespace netlib {
// Includes vector/string capacities (also conservatively charges string SSO).
std::size_t RetainedBytes(const HttpRequestMetadata& metadata);
std::size_t RetainedBytes(const FrameMetadata& metadata);
std::size_t RetainedBytes(const ApplicationResponse& response);
}  // namespace netlib
#endif  // NETLIB_INCLUDE_NETLIB_MESSAGE_MEMORY_H_
