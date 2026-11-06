#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "netlib/frame_protocol.h"
#include "netlib/protocol_codec.h"

namespace {

void ReportFailure(const char* message) {
  std::fprintf(stderr, "%s\n", message);
}

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
  if (result.status != netlib::DecodeStatus::kNeedMore) {
    ReportFailure("codec did not handle a partial length prefix");
    return 1;
  }

  const std::size_t first_frame_size =
      netlib::kFrameLengthPrefixSize + kFirstPayload.size();
  result = codec->Decode(input.first(first_frame_size - 1));
  if (result.status != netlib::DecodeStatus::kNeedMore) {
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

}  // namespace

int main() {
  int framing_result = RunLengthPrefixedCodecTest();
  if (framing_result != 0) {
    return framing_result;
  }
  return RunCodecLimitTest();
}
