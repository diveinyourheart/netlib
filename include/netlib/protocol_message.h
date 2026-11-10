#ifndef NETLIB_INCLUDE_NETLIB_PROTOCOL_MESSAGE_H_
#define NETLIB_INCLUDE_NETLIB_PROTOCOL_MESSAGE_H_

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

#include "netlib/rpc_message.h"

namespace netlib {

struct HttpHeader {
  std::string name;
  std::string value;
};

struct BinaryFrameMetadata {};

struct HttpRequestMetadata {
  std::string method;
  std::string target;
  std::string version;
  std::vector<HttpHeader> headers;
  std::uint64_t content_length = 0;
  bool keep_alive = false;
  // HTTP/2 request pseudo-fields. Empty for existing HTTP/1 Codec requests.
  std::string scheme{};
  std::string authority{};
};

using FrameMetadata =
    std::variant<BinaryFrameMetadata, HttpRequestMetadata, RpcFrameMetadata>;

struct BinaryResponse {
  std::vector<char> payload;
};

struct HttpResponse {
  int status_code = 200;
  std::string reason_phrase = "OK";
  std::vector<HttpHeader> headers;
  std::vector<char> body;
  bool keep_alive = true;
};

using ApplicationResponse =
    std::variant<BinaryResponse, HttpResponse, RpcResponse>;

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_PROTOCOL_MESSAGE_H_
