#include "netlib/http_protocol.h"

#include <array>
#include <cstddef>
#include <cstdio>
#include <span>
#include <string>
#include <string_view>

namespace {

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::fprintf(stderr, "%s\n", message);
  }
  return condition;
}

std::span<const char> Bytes(std::string_view text) {
  return std::span<const char>(text.data(), text.size());
}

bool TestFragments() {
  struct Case {
    std::string_view wire;
    netlib::HttpVersion version;
  };
  // Independent fixture: identifying HTTP/2 requires all 24 preface bytes.
  constexpr std::string_view kPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
  static_assert(kPreface.size() == 24);
  if (!Check(kPreface == netlib::kHttp2ClientPreface,
             "incorrect preface constant")) {
    return false;
  }
  const std::array<Case, 5> cases{{
      {"GET / HTTP/1.0\r\n", netlib::HttpVersion::kHttp10},
      {"POST /rpc?q=1 HTTP/1.1\r\n", netlib::HttpVersion::kHttp11},
      {"OPTIONS * HTTP/1.1\r\n", netlib::HttpVersion::kHttp11},
      {"PRI * HTTP/1.1\r\n", netlib::HttpVersion::kHttp11},
      {kPreface, netlib::HttpVersion::kHttp2},
  }};
  for (const Case& test : cases) {
    for (std::size_t split = 0; split < test.wire.size(); ++split) {
      netlib::HttpProtocolDetector detector;
      const netlib::HttpProtocolDetection partial =
          detector.Detect(Bytes(test.wire).first(split));
      if (!Check(partial.status ==
                         netlib::HttpProtocolDetectionStatus::kNeedMore &&
                     !partial.version.has_value() &&
                     (partial.required_bytes == 0 ||
                      partial.required_bytes > split),
                 "partial protocol prefix was misclassified")) {
        return false;
      }
      const netlib::HttpProtocolDetection full =
          detector.Detect(Bytes(test.wire));
      if (!Check(
              full.status == netlib::HttpProtocolDetectionStatus::kDetected &&
                  full.version == test.version && full.required_bytes == 0 &&
                  detector.Detect({}).version == test.version,
              "protocol selection failed or was not terminal")) {
        return false;
      }
    }
    netlib::HttpProtocolDetector detector;
    for (std::size_t size = 0; size < test.wire.size(); ++size) {
      if (!Check(detector.Detect(Bytes(test.wire).first(size)).status ==
                     netlib::HttpProtocolDetectionStatus::kNeedMore,
                 "incremental prefix scan failed")) {
        return false;
      }
    }
    if (!Check(detector.Detect(Bytes(test.wire)).version == test.version,
               "byte-at-a-time selection failed")) {
      return false;
    }
  }
  return true;
}

bool Rejects(std::string_view wire) {
  netlib::HttpProtocolDetector detector;
  const netlib::HttpProtocolDetection result = detector.Detect(Bytes(wire));
  return result.status == netlib::HttpProtocolDetectionStatus::kProtocolError &&
         !result.version.has_value() && result.required_bytes == 0 &&
         detector.Detect(Bytes(netlib::kHttp2ClientPreface)).status ==
             netlib::HttpProtocolDetectionStatus::kProtocolError;
}

bool TestMalformedPrefixes() {
  constexpr std::array<std::string_view, 12> kInvalid{{
      "GET / HTTP/2.0\r\n",
      "GET / HTTP/3.0\r\n",
      "GET / HTTP/1.2\r\n",
      "HTTP/1.1 200 OK\r\n",
      " / HTTP/1.1\r\n",
      "GET  HTTP/1.1\r\n",
      "GET / HTTP/1.1 extra\r\n",
      "GET / HTTP/1.1\n",
      "G(ET / HTTP/1.1\r\n",
      "GET /\tpath HTTP/1.1\r\n",
      "GET\t/ HTTP/1.1\r\n",
      "PRI * HTTP/2.0\r\n\r\nXX\r\n\r\n",
  }};
  for (std::string_view wire : kInvalid) {
    if (!Check(Rejects(wire), "malformed protocol prefix was accepted")) {
      return false;
    }
  }
  const char embedded_nul[] = "GET /pa\0th HTTP/1.1\r\n";
  if (!Check(Rejects(std::string_view(embedded_nul, sizeof(embedded_nul) - 1)),
             "NUL in request line was accepted")) {
    return false;
  }
  for (std::size_t i = 0; i < netlib::kHttp2ClientPreface.size(); ++i) {
    std::string broken(netlib::kHttp2ClientPreface);
    broken[i] = '#';
    if (!Check(Rejects(broken), "damaged HTTP/2 preface selected HTTP/2")) {
      return false;
    }
  }
  return true;
}

bool TestLimitsAndCoalescing() {
  constexpr std::string_view kLine = "GET / HTTP/1.1\r\n";
  std::string request(kLine);
  request.append("Host: example\r\n\r\n");
  request.append(16384, 'x');
  netlib::HttpProtocolDetector exact(kLine.size());
  netlib::HttpProtocolDetector short_limit(kLine.size() - 1);
  netlib::HttpProtocolDetector invalid(0);
  if (!Check(exact.Detect(Bytes(request)).version ==
                     netlib::HttpVersion::kHttp11 &&
                 short_limit.Detect(Bytes(request)).status ==
                     netlib::HttpProtocolDetectionStatus::kProtocolError &&
                 invalid.Detect({}).status ==
                     netlib::HttpProtocolDetectionStatus::kProtocolError,
             "HTTP/1 request-line bounds are incorrect")) {
    return false;
  }
  std::string preface(netlib::kHttp2ClientPreface);
  preface.append(9,
                 '\0');  // Subsequent frames are the future Session's concern.
  netlib::HttpProtocolDetector http2(1);
  if (!Check(
          http2.Detect(Bytes(preface)).version == netlib::HttpVersion::kHttp2,
          "HTTP/2 preface plus frame bytes failed detection")) {
    return false;
  }
  netlib::HttpProtocolDetector truncated;
  if (!Check(truncated.Detect(Bytes("GET /unfinished")).status ==
                     netlib::HttpProtocolDetectionStatus::kNeedMore &&
                 truncated.Detect(Bytes("GET")).status ==
                     netlib::HttpProtocolDetectionStatus::kProtocolError,
             "shrinking unconsumed prefix was not rejected")) {
    return false;
  }
  std::string long_line = "GET /";
  long_line.append(2048, 'x');
  long_line.append(" HTTP/1.1\r\n");
  netlib::HttpProtocolDetector growing(long_line.size());
  std::string growing_input;
  for (char byte : long_line) {
    growing_input.push_back(byte);  // Also exercises relocating input buffers.
    const netlib::HttpProtocolDetection result =
        growing.Detect(Bytes(growing_input));
    if (growing_input.size() < long_line.size() &&
        result.status != netlib::HttpProtocolDetectionStatus::kNeedMore) {
      return Check(false, "incremental long request line failed");
    }
  }
  return Check(
      growing.Detect(Bytes(long_line)).version == netlib::HttpVersion::kHttp11,
      "long request line was not recognized");
}

}  // namespace

int main() {
  return TestFragments() && TestMalformedPrefixes() && TestLimitsAndCoalescing()
             ? 0
             : 1;
}
