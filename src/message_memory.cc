#include "netlib/message_memory.h"

#include <type_traits>
#include <variant>

namespace netlib {
namespace {
std::size_t HeaderBytes(const std::vector<HttpHeader>& headers) {
  std::size_t size = headers.capacity() * sizeof(HttpHeader);
  for (const HttpHeader& header : headers)
    size += header.name.capacity() + 1 + header.value.capacity() + 1;
  return size;
}
std::size_t RpcBytes(const RpcFrameMetadata& metadata) {
  std::size_t size =
      metadata.service_name.capacity() + 1 + metadata.method_name.capacity() +
      1 + metadata.extensions.capacity() * sizeof(RpcHeaderExtension);
  for (const RpcHeaderExtension& extension : metadata.extensions)
    size += extension.value.capacity();
  return size;
}
}  // namespace

std::size_t RetainedBytes(const HttpRequestMetadata& metadata) {
  return metadata.method.capacity() + metadata.target.capacity() +
         metadata.version.capacity() + metadata.scheme.capacity() +
         metadata.authority.capacity() + 5 + HeaderBytes(metadata.headers);
}
std::size_t RetainedBytes(const FrameMetadata& metadata) {
  return std::visit(
      [](const auto& value) -> std::size_t {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, HttpRequestMetadata>)
          return RetainedBytes(value);
        else if constexpr (std::is_same_v<T, RpcFrameMetadata>)
          return RpcBytes(value);
        else
          return 0;
      },
      metadata);
}
std::size_t RetainedBytes(const ApplicationResponse& response) {
  return sizeof(ApplicationResponse) +
         std::visit(
             [](const auto& value) -> std::size_t {
               using T = std::decay_t<decltype(value)>;
               if constexpr (std::is_same_v<T, HttpResponse>)
                 return value.body.capacity() + value.reason_phrase.capacity() +
                        1 + HeaderBytes(value.headers);
               else if constexpr (std::is_same_v<T, RpcResponse>)
                 return value.payload.capacity() + RpcBytes(value.metadata);
               else
                 return value.payload.capacity();
             },
             response);
}
}  // namespace netlib
