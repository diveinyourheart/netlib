#include "netlib/http1_codec.h"

#include <cctype>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "netlib/protocol_codec.h"
#include "netlib/protocol_message.h"

namespace netlib {
namespace {

std::string_view Trim(std::string_view value) {
  while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
    value.remove_prefix(1);
  }
  while (!value.empty() && (value.back() == ' ' || value.back() == '\t')) {
    value.remove_suffix(1);
  }
  return value;
}

bool EqualsIgnoreCase(std::string_view left, std::string_view right) {
  if (left.size() != right.size()) {
    return false;
  }
  for (std::size_t i = 0; i < left.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(left[i])) !=
        std::tolower(static_cast<unsigned char>(right[i]))) {
      return false;
    }
  }
  return true;
}

bool ContainsTokenIgnoreCase(std::string_view value, std::string_view token) {
  while (!value.empty()) {
    std::size_t comma = value.find(',');
    std::string_view item = Trim(value.substr(0, comma));
    if (EqualsIgnoreCase(item, token)) {
      return true;
    }
    if (comma == std::string_view::npos) {
      return false;
    }
    value.remove_prefix(comma + 1);
  }
  return false;
}

void AppendUnsigned(std::string& output, std::uint64_t value) {
  char buffer[32];
  std::to_chars_result result =
      std::to_chars(buffer, buffer + sizeof(buffer), value);
  output.append(buffer, result.ptr);
}

void AppendHeader(std::string& output, std::string_view name,
                  std::string_view value) {
  output.append(name);
  output.append(": ");
  output.append(value);
  output.append("\r\n");
}

class Http1Codec final : public ProtocolCodec {
 public:
  explicit Http1Codec(Http1CodecOptions options) : options_(options) {}

  DecodeResult Decode(std::span<const char> input) override {
    if (state_ == State::kHeader) {
      if (!ParseHeaders(input)) {
        return netlib::DecodeResult{
            .status = netlib::DecodeStatus::kProtocolError,
            .metadata = BinaryFrameMetadata{},
        };
      }
      if (state_ == State::kHeader) {
        return netlib::DecodeResult{
            .status = netlib::DecodeStatus::kNeedMore,
            .required_bytes = 0,
            .metadata = BinaryFrameMetadata{},
        };
      }
    }

    if (input.size() < expected_frame_size_) {
      return netlib::DecodeResult{
          .status = netlib::DecodeStatus::kNeedMore,
          .required_bytes = expected_frame_size_,
          .metadata = BinaryFrameMetadata{},
      };
    }

    netlib::DecodeResult result{
        .status = netlib::DecodeStatus::kFrameReady,
        .consumed_bytes = expected_frame_size_,
        .payload_offset = header_size_,
        .payload_size = body_size_,
        .metadata = std::move(metadata_),
    };
    state_ = State::kHeader;
    header_search_offset_ = 0;
    header_size_ = 0;
    body_size_ = 0;
    expected_frame_size_ = 0;
    metadata_ = HttpRequestMetadata{};
    return result;
  }

  bool Encode(const ApplicationResponse& response,
              std::vector<char>* output) override {
    const HttpResponse* http_response = std::get_if<HttpResponse>(&response);
    if (http_response == nullptr || http_response->status_code < 100 ||
        http_response->status_code > 999 ||
        http_response->body.size() > options_.max_body_bytes) {
      return false;
    }

    std::size_t old_size = output->size();
    std::string header;
    header.reserve(256);
    header.append("HTTP/1.1 ");
    AppendUnsigned(header,
                   static_cast<std::uint64_t>(http_response->status_code));
    header.push_back(' ');
    if (http_response->reason_phrase.find_first_of("\r\n") !=
        std::string_view::npos) {
      return false;
    }
    header.append(http_response->reason_phrase);
    header.append("\r\n");

    for (const HttpHeader& item : http_response->headers) {
      if (item.name.empty() ||
          item.name.find_first_of(":\r\n") != std::string::npos ||
          item.value.find_first_of("\r\n") != std::string::npos) {
        return false;
      }
      if (EqualsIgnoreCase(item.name, "content-length") ||
          EqualsIgnoreCase(item.name, "connection")) {
        continue;
      }
      AppendHeader(header, item.name, item.value);
      if (header.size() > options_.max_header_bytes) {
        return false;
      }
    }

    header.append("Content-Length: ");
    AppendUnsigned(header, http_response->body.size());
    header.append("\r\n");
    AppendHeader(header, "Connection",
                 http_response->keep_alive ? "keep-alive" : "close");
    header.append("\r\n");
    if (header.size() > options_.max_header_bytes) {
      return false;
    }

    output->reserve(old_size + header.size() + http_response->body.size());
    output->insert(output->end(), header.begin(), header.end());
    output->insert(output->end(), http_response->body.begin(),
                   http_response->body.end());
    return true;
  }

  bool ShouldCloseAfterWrite(
      const ApplicationResponse& response) const override {
    const HttpResponse* http_response = std::get_if<HttpResponse>(&response);
    return http_response == nullptr || !http_response->keep_alive;
  }

  std::optional<ApplicationResponse> CreateTimeoutResponse() const override {
    constexpr std::string_view kTimeoutBody = "request timed out\n";
    return HttpResponse{
        .status_code = 504,
        .reason_phrase = "Gateway Timeout",
        .headers =
            {
                HttpHeader{
                    .name = "Content-Type",
                    .value = "text/plain; charset=utf-8",
                },
            },
        .body = std::vector<char>(kTimeoutBody.begin(), kTimeoutBody.end()),
        .keep_alive = false,
    };
  }

  std::size_t MaxDecodeBufferSize() const override {
    return options_.max_header_bytes + options_.max_body_bytes;
  }

 private:
  enum class State : std::uint8_t {
    kHeader,
    kBody,
  };

  bool ParseHeaders(std::span<const char> input) {
    std::string_view bytes(input.data(), input.size());
    std::size_t delimiter = bytes.find("\r\n\r\n", header_search_offset_);
    if (delimiter == std::string_view::npos) {
      header_search_offset_ = bytes.size() > 3 ? bytes.size() - 3 : 0;
      if (bytes.size() > options_.max_header_bytes) {
        return false;
      }
      return true;
    }

    std::size_t header_end = delimiter + 4;
    if (header_end > options_.max_header_bytes) {
      return false;
    }

    std::size_t request_line_end = bytes.find("\r\n");
    if (request_line_end == std::string_view::npos ||
        request_line_end > delimiter) {
      return false;
    }
    std::string_view request_line = bytes.substr(0, request_line_end);
    std::size_t first_space = request_line.find(' ');
    std::size_t second_space = request_line.find(
        ' ',
        first_space == std::string_view::npos ? first_space : first_space + 1);
    if (first_space == std::string_view::npos ||
        second_space == std::string_view::npos ||
        request_line.find(' ', second_space + 1) != std::string_view::npos) {
      return false;
    }

    metadata_.method = std::string(request_line.substr(0, first_space));
    metadata_.target = std::string(
        request_line.substr(first_space + 1, second_space - first_space - 1));
    metadata_.version = std::string(request_line.substr(second_space + 1));
    if (metadata_.method.empty() || metadata_.target.empty() ||
        (metadata_.version != "HTTP/1.1" && metadata_.version != "HTTP/1.0")) {
      return false;
    }
    metadata_.keep_alive = metadata_.version == "HTTP/1.1";

    bool content_length_seen = false;
    bool transfer_encoding_seen = false;
    bool connection_close = false;
    bool connection_keep_alive = false;
    std::size_t position = request_line_end + 2;
    while (position < delimiter) {
      std::size_t line_end = bytes.find("\r\n", position);
      if (line_end == std::string_view::npos || line_end > delimiter) {
        return false;
      }
      std::string_view line = bytes.substr(position, line_end - position);
      std::size_t colon = line.find(':');
      if (colon == std::string_view::npos || colon == 0) {
        return false;
      }
      std::string_view name = Trim(line.substr(0, colon));
      std::string_view value = Trim(line.substr(colon + 1));
      if (name.empty()) {
        return false;
      }
      metadata_.headers.push_back(
          HttpHeader{.name = std::string(name), .value = std::string(value)});

      if (EqualsIgnoreCase(name, "content-length")) {
        if (content_length_seen) {
          return false;
        }
        std::uint64_t parsed_length = 0;
        std::from_chars_result parsed = std::from_chars(
            value.data(), value.data() + value.size(), parsed_length);
        if (parsed.ec != std::errc{} ||
            parsed.ptr != value.data() + value.size()) {
          return false;
        }
        metadata_.content_length = parsed_length;
        content_length_seen = true;
      } else if (EqualsIgnoreCase(name, "transfer-encoding")) {
        if (!EqualsIgnoreCase(value, "identity")) {
          return false;
        }
        transfer_encoding_seen = true;
      } else if (EqualsIgnoreCase(name, "connection")) {
        connection_close =
            connection_close || ContainsTokenIgnoreCase(value, "close");
        connection_keep_alive = connection_keep_alive ||
                                ContainsTokenIgnoreCase(value, "keep-alive");
      }
      position = line_end + 2;
    }

    if (transfer_encoding_seen && content_length_seen) {
      return false;
    }
    if (connection_close) {
      metadata_.keep_alive = false;
    } else if (connection_keep_alive) {
      metadata_.keep_alive = true;
    }
    if (metadata_.content_length > options_.max_body_bytes) {
      return false;
    }

    header_size_ = header_end;
    body_size_ = static_cast<std::size_t>(metadata_.content_length);
    expected_frame_size_ = header_size_ + body_size_;
    state_ = State::kBody;
    return true;
  }

  const Http1CodecOptions options_;
  State state_ = State::kHeader;
  std::size_t header_search_offset_ = 0;
  std::size_t header_size_ = 0;
  std::size_t body_size_ = 0;
  std::size_t expected_frame_size_ = 0;
  HttpRequestMetadata metadata_;
};

}  // namespace

std::unique_ptr<ProtocolCodec> CreateHttp1Codec(Http1CodecOptions options) {
  if (options.max_header_bytes == 0 || options.max_body_bytes == 0 ||
      options.max_header_bytes >
          static_cast<std::size_t>(-1) - options.max_body_bytes) {
    return nullptr;
  }
  return std::make_unique<Http1Codec>(options);
}

}  // namespace netlib
