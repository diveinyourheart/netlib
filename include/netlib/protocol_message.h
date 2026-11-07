#ifndef NETLIB_INCLUDE_NETLIB_PROTOCOL_MESSAGE_H_
#define NETLIB_INCLUDE_NETLIB_PROTOCOL_MESSAGE_H_

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

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
};

using FrameMetadata = std::variant<BinaryFrameMetadata, HttpRequestMetadata>;

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

using ApplicationResponse = std::variant<BinaryResponse, HttpResponse>;

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_PROTOCOL_MESSAGE_H_
