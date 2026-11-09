#include "netlib/http_protocol.h"

#include <algorithm>
#include <cstddef>
#include <span>
#include <string_view>

namespace netlib {
namespace {

bool ValidMethod(std::string_view method) noexcept {
  constexpr std::string_view kTokenPunctuation = "!#$%&'*+-.^_`|~";
  for (char character : method) {
    const unsigned char byte = static_cast<unsigned char>(character);
    const bool alphanumeric = (byte >= 'A' && byte <= 'Z') ||
                              (byte >= 'a' && byte <= 'z') ||
                              (byte >= '0' && byte <= '9');
    if (!alphanumeric &&
        kTokenPunctuation.find(character) == std::string_view::npos) {
      return false;
    }
  }
  return !method.empty();
}

bool ValidTarget(std::string_view target) noexcept {
  for (char character : target) {
    const unsigned char byte = static_cast<unsigned char>(character);
    if (byte <= 0x20U || byte == 0x7fU) {
      return false;
    }
  }
  return !target.empty();
}

}  // namespace

HttpProtocolDetector::HttpProtocolDetector(
    std::size_t max_request_line_bytes) noexcept
    : max_request_line_bytes_(max_request_line_bytes) {}

HttpProtocolDetection HttpProtocolDetector::Detect(
    std::span<const char> input) noexcept {
  if (result_.status != HttpProtocolDetectionStatus::kNeedMore) {
    return result_;
  }
  if (max_request_line_bytes_ == 0 || input.size() < previous_input_size_) {
    return Fail();
  }
  previous_input_size_ = input.size();
  if (input.empty()) {
    return NeedMore(1);
  }
  const std::size_t prefix_size =
      std::min(input.size(), kHttp2ClientPreface.size());
  if (std::equal(input.begin(), input.begin() + prefix_size,
                 kHttp2ClientPreface.begin())) {
    if (input.size() < kHttp2ClientPreface.size()) {
      // Ask for the next distinguishing byte, not all 24: a short HTTP/1
      // request may diverge from this prefix and finish before byte 24.
      return NeedMore(input.size() + 1);
    }
    return Select(HttpVersion::kHttp2);
  }

  const std::size_t scan_size = std::min(input.size(), max_request_line_bytes_);
  const std::string_view bytes(input.data(), scan_size);
  const std::size_t delimiter = bytes.find("\r\n", line_search_offset_);
  if (delimiter == std::string_view::npos) {
    if (input.size() >= max_request_line_bytes_ ||
        bytes.find('\n', line_search_offset_) != std::string_view::npos) {
      return Fail();
    }
    // Retain one byte of overlap for CRLF split across reads. Body/header
    // fragments do not make us rescan an ever-growing request line.
    line_search_offset_ = scan_size - 1;
    return NeedMore();
  }

  const std::string_view line = bytes.substr(0, delimiter);
  const std::size_t first_space = line.find(' ');
  if (first_space == std::string_view::npos) {
    return Fail();
  }
  const std::size_t second_space = line.find(' ', first_space + 1);
  if (second_space == std::string_view::npos ||
      !ValidMethod(line.substr(0, first_space)) ||
      !ValidTarget(
          line.substr(first_space + 1, second_space - first_space - 1))) {
    return Fail();
  }
  const std::string_view version = line.substr(second_space + 1);
  if (version == "HTTP/1.0") {
    return Select(HttpVersion::kHttp10);
  }
  if (version == "HTTP/1.1") {
    return Select(HttpVersion::kHttp11);
  }
  return Fail();
}

HttpProtocolDetection HttpProtocolDetector::NeedMore(
    std::size_t required_bytes) noexcept {
  result_.required_bytes = required_bytes;
  return result_;
}

HttpProtocolDetection HttpProtocolDetector::Select(
    HttpVersion version) noexcept {
  result_.status = HttpProtocolDetectionStatus::kDetected;
  result_.version = version;
  result_.required_bytes = 0;
  return result_;
}

HttpProtocolDetection HttpProtocolDetector::Fail() noexcept {
  result_.status = HttpProtocolDetectionStatus::kProtocolError;
  result_.version.reset();
  result_.required_bytes = 0;
  return result_;
}

}  // namespace netlib
