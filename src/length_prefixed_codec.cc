#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "netlib/frame_protocol.h"
#include "netlib/protocol_codec.h"

namespace netlib {
namespace {

class LengthPrefixedCodec final : public ProtocolCodec {
 public:
  explicit LengthPrefixedCodec(std::uint32_t max_payload_size)
      : max_payload_size_(max_payload_size) {}

  DecodeResult Decode(std::span<const char> input) override {
    if (expected_frame_size_ == 0) {
      if (input.size() < kFrameLengthPrefixSize) {
        return DecodeResult{.status = DecodeStatus::kNeedMore};
      }
      std::uint32_t payload_size =
          DecodePayloadSize(input.first(kFrameLengthPrefixSize));
      if (payload_size > max_payload_size_) {
        return DecodeResult{.status = DecodeStatus::kProtocolError};
      }
      expected_frame_size_ =
          kFrameLengthPrefixSize + static_cast<std::size_t>(payload_size);
    }

    if (input.size() < expected_frame_size_) {
      return DecodeResult{.status = DecodeStatus::kNeedMore};
    }

    DecodeResult result{
        .status = DecodeStatus::kFrameReady,
        .consumed_bytes = expected_frame_size_,
        .payload_offset = kFrameLengthPrefixSize,
        .payload_size = expected_frame_size_ - kFrameLengthPrefixSize,
    };
    expected_frame_size_ = 0;
    return result;
  }

  bool Encode(std::span<const char> payload,
              std::vector<char>* output) override {
    if (payload.size() > max_payload_size_) {
      return false;
    }

    std::uint32_t payload_size = static_cast<std::uint32_t>(payload.size());
    output->reserve(output->size() + kFrameLengthPrefixSize + payload.size());
    output->push_back(static_cast<char>((payload_size >> 24U) & 0xFFU));
    output->push_back(static_cast<char>((payload_size >> 16U) & 0xFFU));
    output->push_back(static_cast<char>((payload_size >> 8U) & 0xFFU));
    output->push_back(static_cast<char>(payload_size & 0xFFU));
    output->insert(output->end(), payload.begin(), payload.end());
    return true;
  }

  std::size_t MaxDecodeBufferSize() const override {
    return kFrameLengthPrefixSize + max_payload_size_;
  }

 private:
  std::uint32_t DecodePayloadSize(std::span<const char> header) const {
    return (static_cast<std::uint32_t>(static_cast<unsigned char>(header[0]))
            << 24U) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(header[1]))
            << 16U) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(header[2]))
            << 8U) |
           static_cast<std::uint32_t>(static_cast<unsigned char>(header[3]));
  }

  const std::uint32_t max_payload_size_;
  std::size_t expected_frame_size_ = 0;
};

}  // namespace

std::unique_ptr<ProtocolCodec> CreateLengthPrefixedCodec(
    std::uint32_t max_payload_size) {
  if (max_payload_size == 0) {
    return nullptr;
  }
  return std::make_unique<LengthPrefixedCodec>(max_payload_size);
}

}  // namespace netlib
