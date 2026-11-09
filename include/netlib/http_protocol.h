#ifndef NETLIB_INCLUDE_NETLIB_HTTP_PROTOCOL_H_
#define NETLIB_INCLUDE_NETLIB_HTTP_PROTOCOL_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>

namespace netlib {

inline constexpr std::string_view kHttp2ClientPreface =
    "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

enum class HttpVersion : std::uint8_t { kHttp10, kHttp11, kHttp2 };
enum class HttpProtocolDetectionStatus : std::uint8_t {
  kNeedMore,
  kDetected,
  kProtocolError,
};

struct HttpProtocolDetection {
  HttpProtocolDetectionStatus status = HttpProtocolDetectionStatus::kNeedMore;
  std::optional<HttpVersion> version = std::nullopt;
  // Next total input size needed, or zero while searching for a request-line
  // delimiter. Bytes are never consumed by detection.
  std::size_t required_bytes = 0;
};

// One detector per cleartext HTTP connection. Feed the growing, unconsumed
// connection prefix until a terminal result; it is then cached. Does not parse
// Headers or validate HTTP/2 frames after the preface. Never feed TLS
// ciphertext.
class HttpProtocolDetector {
 public:
  // HTTP/1 request-line limit includes CRLF. Zero is invalid. The HTTP/2
  // preface has its own fixed size and is not subject to this HTTP/1 limit.
  explicit HttpProtocolDetector(
      std::size_t max_request_line_bytes = 8 * 1024) noexcept;

  HttpProtocolDetection Detect(std::span<const char> input) noexcept;

 private:
  HttpProtocolDetection NeedMore(std::size_t required_bytes = 0) noexcept;
  HttpProtocolDetection Select(HttpVersion version) noexcept;
  HttpProtocolDetection Fail() noexcept;

  const std::size_t max_request_line_bytes_;
  std::size_t previous_input_size_ = 0;
  std::size_t line_search_offset_ = 0;
  HttpProtocolDetection result_;
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_HTTP_PROTOCOL_H_
