#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "netlib/frame_protocol.h"
#include "netlib/protocol_codec.h"

namespace {

void ReportFailure(const char* message) {
  std::fprintf(stderr, "%s\n", message);
}

class TestHttpLikeCodec final : public netlib::ProtocolCodec {
 public:
  netlib::DecodeResult Decode(std::span<const char> input) override {
    if (state_ == State::kHeader) {
      std::string_view bytes(input.data(), input.size());
      std::size_t delimiter = bytes.find("\r\n\r\n");
      if (delimiter == std::string_view::npos) {
        return netlib::DecodeResult{
            .status = netlib::DecodeStatus::kNeedMore,
            .required_bytes = 0,
        };
      }

      std::size_t length_position = bytes.find("Content-Length: ");
      if (length_position == std::string_view::npos) {
        return netlib::DecodeResult{
            .status = netlib::DecodeStatus::kProtocolError,
        };
      }
      length_position += std::string_view("Content-Length: ").size();
      std::size_t length_end = bytes.find("\r\n", length_position);
      if (length_end == std::string_view::npos) {
        return netlib::DecodeResult{
            .status = netlib::DecodeStatus::kProtocolError,
        };
      }

      std::uint32_t content_length = 0;
      std::from_chars_result parsed =
          std::from_chars(bytes.data() + length_position,
                          bytes.data() + length_end, content_length);
      if (parsed.ec != std::errc{} || parsed.ptr != bytes.data() + length_end) {
        return netlib::DecodeResult{
            .status = netlib::DecodeStatus::kProtocolError,
        };
      }

      payload_offset_ = delimiter + 4;
      expected_frame_size_ = payload_offset_ + content_length;
      payload_size_ = content_length;
      state_ = State::kBody;
    }

    if (input.size() < expected_frame_size_) {
      return netlib::DecodeResult{
          .status = netlib::DecodeStatus::kNeedMore,
          .required_bytes = expected_frame_size_,
      };
    }

    netlib::DecodeResult result{
        .status = netlib::DecodeStatus::kFrameReady,
        .consumed_bytes = expected_frame_size_,
        .payload_offset = payload_offset_,
        .payload_size = payload_size_,
    };
    state_ = State::kHeader;
    return result;
  }

  bool Encode(std::span<const char>, std::vector<char>*) override {
    return false;
  }

  std::size_t MaxDecodeBufferSize() const override { return 256; }

 private:
  enum class State : std::uint8_t {
    kHeader,
    kBody,
  };

  State state_ = State::kHeader;
  std::size_t payload_offset_ = 0;
  std::size_t payload_size_ = 0;
  std::size_t expected_frame_size_ = 0;
};

int RunLengthPrefixedCodecTest() {
  std::unique_ptr<netlib::ProtocolCodec> codec =
      netlib::CreateLengthPrefixedCodec(1024);
  if (!codec) {
    ReportFailure("failed to create LengthPrefixedCodec");
    return 1;
  }

  constexpr std::string_view kFirstPayload = "first";
  constexpr std::string_view kSecondPayload = "second";
  std::vector<char> encoded;
  if (!codec->Encode(
          std::span<const char>(kFirstPayload.data(), kFirstPayload.size()),
          &encoded) ||
      !codec->Encode(
          std::span<const char>(kSecondPayload.data(), kSecondPayload.size()),
          &encoded)) {
    ReportFailure("codec encode failed");
    return 1;
  }

  std::span<const char> input(encoded.data(), encoded.size());
  netlib::DecodeResult result = codec->Decode(input.first(2));
  if (result.status != netlib::DecodeStatus::kNeedMore ||
      result.required_bytes != netlib::kFrameLengthPrefixSize) {
    ReportFailure("codec did not handle a partial length prefix");
    return 1;
  }

  const std::size_t first_frame_size =
      netlib::kFrameLengthPrefixSize + kFirstPayload.size();
  result = codec->Decode(input.first(first_frame_size - 1));
  if (result.status != netlib::DecodeStatus::kNeedMore ||
      result.required_bytes != first_frame_size) {
    ReportFailure("codec did not handle a partial payload");
    return 1;
  }

  result = codec->Decode(input);
  if (result.status != netlib::DecodeStatus::kFrameReady ||
      result.consumed_bytes != first_frame_size ||
      result.payload_offset != netlib::kFrameLengthPrefixSize ||
      result.payload_size != kFirstPayload.size() ||
      std::string_view(input.data() + result.payload_offset,
                       result.payload_size) != kFirstPayload) {
    ReportFailure("codec decoded the first frame incorrectly");
    return 1;
  }

  input = input.subspan(result.consumed_bytes);
  result = codec->Decode(input);
  if (result.status != netlib::DecodeStatus::kFrameReady ||
      result.consumed_bytes !=
          netlib::kFrameLengthPrefixSize + kSecondPayload.size() ||
      std::string_view(input.data() + result.payload_offset,
                       result.payload_size) != kSecondPayload) {
    ReportFailure("codec did not decode coalesced frames");
    return 1;
  }
  return 0;
}

int RunCodecLimitTest() {
  std::unique_ptr<netlib::ProtocolCodec> codec =
      netlib::CreateLengthPrefixedCodec(4);
  if (!codec) {
    ReportFailure("failed to create bounded codec");
    return 1;
  }

  const char oversized_header[] = {0, 0, 0, 5};
  netlib::DecodeResult result = codec->Decode(
      std::span<const char>(oversized_header, sizeof(oversized_header)));
  if (result.status != netlib::DecodeStatus::kProtocolError) {
    ReportFailure("codec accepted an oversized payload");
    return 1;
  }

  const char payload[] = {'1', '2', '3', '4', '5'};
  std::vector<char> output;
  if (codec->Encode(std::span<const char>(payload, sizeof(payload)), &output)) {
    ReportFailure("codec encoded an oversized response");
    return 1;
  }
  return 0;
}

int RunHttpLikeStateMachineTest() {
  TestHttpLikeCodec codec;
  const std::string request =
      "POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello";

  netlib::DecodeResult result =
      codec.Decode(std::span<const char>(request.data(), 12));
  if (result.status != netlib::DecodeStatus::kNeedMore ||
      result.required_bytes != 0) {
    ReportFailure("HTTP-like codec header scan returned a wrong hint");
    return 1;
  }

  const std::size_t header_size = request.find("\r\n\r\n") + 4;
  result = codec.Decode(std::span<const char>(request.data(), header_size));
  if (result.status != netlib::DecodeStatus::kNeedMore ||
      result.required_bytes != request.size()) {
    ReportFailure("HTTP-like codec did not report the body target");
    return 1;
  }

  result = codec.Decode(std::span<const char>(request.data(), request.size()));
  if (result.status != netlib::DecodeStatus::kFrameReady ||
      result.consumed_bytes != request.size() ||
      result.payload_offset != header_size || result.payload_size != 5 ||
      std::string_view(request.data() + result.payload_offset,
                       result.payload_size) != "hello") {
    ReportFailure("HTTP-like codec did not produce a complete frame");
    return 1;
  }
  return 0;
}

}  // namespace

int main() {
  int framing_result = RunLengthPrefixedCodecTest();
  if (framing_result != 0) {
    return framing_result;
  }
  int limit_result = RunCodecLimitTest();
  if (limit_result != 0) {
    return limit_result;
  }
  return RunHttpLikeStateMachineTest();
}
