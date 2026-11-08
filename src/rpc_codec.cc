#include "netlib/rpc_codec.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <variant>
#include <vector>

namespace netlib {
namespace {

constexpr std::uint32_t kRpcMagic = 0x4e525043;  // "NRPC".
constexpr std::uint16_t kCriticalExtensionBit = 0x8000;
constexpr std::size_t kExtensionPrefixSize = 4;

// Read bytes explicitly: network buffers can be unaligned, and native structs
// have platform-dependent padding and byte order.
std::uint64_t ReadUnsigned(std::span<const char> bytes, std::size_t offset,
                           std::size_t width) {
  std::uint64_t value = 0;
  for (std::size_t i = 0; i < width; ++i) {
    value = (value << 8U) | static_cast<unsigned char>(bytes[offset + i]);
  }
  return value;
}

void AppendUnsigned(std::uint64_t value, std::size_t width,
                    std::vector<char>* output) {
  for (std::size_t i = width; i != 0; --i) {
    output->push_back(static_cast<char>((value >> ((i - 1) * 8U)) & 0xffU));
  }
}

bool ValidOptions(const RpcCodecOptions& options) {
  return options.max_header_bytes >= kRpcFixedHeaderSize &&
         options.max_header_bytes <=
             std::numeric_limits<std::uint16_t>::max() &&
         options.max_header_bytes <= options.max_frame_bytes &&
         options.max_frame_bytes <= std::numeric_limits<std::uint32_t>::max() &&
         options.max_frame_bytes <=
             std::numeric_limits<std::size_t>::max() - kRpcLengthPrefixSize;
}

bool ValidMetadata(const RpcFrameMetadata& metadata) {
  if (metadata.version != kRpcProtocolVersion ||
      metadata.serialization != RpcSerialization::kProtobuf) {
    return false;
  }
  switch (metadata.compression) {
    case RpcCompression::kNone:
    case RpcCompression::kGzip:
    case RpcCompression::kZstd:
      break;
    default:
      return false;
  }
  switch (metadata.message_type) {
    case RpcMessageType::kRequest:
      return metadata.error_code == 0 && !metadata.service_name.empty() &&
             !metadata.method_name.empty();
    case RpcMessageType::kResponse:
      return true;
    default:
      return false;
  }
}

bool ValidExtensionType(std::uint16_t type) {
  return type != 0 && (type & kCriticalExtensionBit) == 0;
}

// Subtract first so a malicious or enormous input cannot wrap the size sum.
bool AddBounded(std::size_t size, std::size_t limit, std::size_t* total) {
  if (size > limit - *total) {
    return false;
  }
  *total += size;
  return true;
}

class RpcCodec final : public ProtocolCodec {
 public:
  explicit RpcCodec(RpcCodecOptions options) : options_(options) {}

  DecodeResult Decode(std::span<const char> input) override {
    if (state_ == State::kError) {
      return ProtocolError();
    }
    if (state_ == State::kLength) {
      if (input.size() < kRpcLengthPrefixSize) {
        return NeedMore(kRpcLengthPrefixSize);
      }
      const std::size_t frame_size =
          static_cast<std::size_t>(ReadUnsigned(input, 0, 4));
      if (frame_size < kRpcFixedHeaderSize ||
          frame_size > options_.max_frame_bytes) {
        return ProtocolError();
      }
      expected_frame_size_ = kRpcLengthPrefixSize + frame_size;
      state_ = State::kHeader;
    }
    if (state_ == State::kHeader) {
      if (input.size() < kRpcLengthPrefixSize + kRpcFixedHeaderSize) {
        return NeedMore(kRpcLengthPrefixSize + kRpcFixedHeaderSize);
      }
      const std::span<const char> header = input.subspan(kRpcLengthPrefixSize);
      if (ReadUnsigned(header, 0, 4) != kRpcMagic ||
          ReadUnsigned(header, 4, 1) != kRpcProtocolVersion ||
          ReadUnsigned(header, 5, 1) > 1 ||
          ReadUnsigned(header, 16, 1) !=
              static_cast<std::uint8_t>(RpcSerialization::kProtobuf) ||
          ReadUnsigned(header, 17, 1) >
              static_cast<std::uint8_t>(RpcCompression::kZstd)) {
        return ProtocolError();
      }
      const std::size_t header_size =
          static_cast<std::size_t>(ReadUnsigned(header, 6, 2));
      const std::size_t service_size =
          static_cast<std::size_t>(ReadUnsigned(header, 20, 2));
      const std::size_t method_size =
          static_cast<std::size_t>(ReadUnsigned(header, 22, 2));
      if (header_size < kRpcFixedHeaderSize ||
          header_size > options_.max_header_bytes ||
          header_size > expected_frame_size_ - kRpcLengthPrefixSize ||
          service_size + method_size > header_size - kRpcFixedHeaderSize ||
          (header[5] == 0 && (ReadUnsigned(header, 18, 2) != 0 ||
                              service_size == 0 || method_size == 0))) {
        return ProtocolError();
      }
      payload_offset_ = kRpcLengthPrefixSize + header_size;
      if (input.size() < payload_offset_) {
        return NeedMore(payload_offset_);
      }
      metadata_.message_type = static_cast<RpcMessageType>(header[5]);
      metadata_.request_id = ReadUnsigned(header, 8, 8);
      metadata_.serialization = RpcSerialization::kProtobuf;
      metadata_.compression = static_cast<RpcCompression>(header[17]);
      metadata_.error_code =
          static_cast<std::uint16_t>(ReadUnsigned(header, 18, 2));
      metadata_.service_name.assign(header.data() + kRpcFixedHeaderSize,
                                    service_size);
      metadata_.method_name.assign(
          header.data() + kRpcFixedHeaderSize + service_size, method_size);
      if (!ParseExtensions(header.first(header_size),
                           kRpcFixedHeaderSize + service_size + method_size)) {
        return ProtocolError();
      }
      state_ = State::kPayload;
    }
    if (input.size() < expected_frame_size_) {
      return NeedMore(expected_frame_size_);
    }
    DecodeResult result{
        .status = DecodeStatus::kFrameReady,
        .consumed_bytes = expected_frame_size_,
        .payload_offset = payload_offset_,
        .payload_size = expected_frame_size_ - payload_offset_,
        .metadata = std::move(metadata_),
    };
    metadata_ = RpcFrameMetadata{};
    state_ = State::kLength;
    return result;
  }

  bool Encode(const ApplicationResponse& response,
              std::vector<char>* output) override {
    const RpcResponse* rpc = std::get_if<RpcResponse>(&response);
    return rpc != nullptr &&
           rpc->metadata.message_type == RpcMessageType::kResponse &&
           EncodeRpcFrame(rpc->metadata, rpc->payload, output, options_);
  }

  bool ShouldCloseAfterWrite(const ApplicationResponse&) const override {
    return false;
  }

  std::optional<ApplicationResponse> CreateTimeoutResponse() const override {
    // A response needs the timed-out request's ID. This context-free interface
    // cannot identify it when multiple requests are in flight; the last decoded
    // header may belong to another request. Let SubReactor close the
    // connection.
    return std::nullopt;
  }

  std::size_t MaxDecodeBufferSize() const override {
    return kRpcLengthPrefixSize + options_.max_frame_bytes;
  }

 private:
  enum class State : std::uint8_t { kLength, kHeader, kPayload, kError };

  DecodeResult NeedMore(std::size_t required_bytes) const {
    return DecodeResult{
        .status = DecodeStatus::kNeedMore,
        .required_bytes = required_bytes,
        .metadata = BinaryFrameMetadata{},
    };
  }

  DecodeResult ProtocolError() {
    state_ = State::kError;
    return DecodeResult{
        .status = DecodeStatus::kProtocolError,
        .metadata = BinaryFrameMetadata{},
    };
  }

  bool ParseExtensions(std::span<const char> header, std::size_t offset) {
    while (offset < header.size()) {
      if (header.size() - offset < kExtensionPrefixSize) {
        return false;
      }
      const std::uint16_t type =
          static_cast<std::uint16_t>(ReadUnsigned(header, offset, 2));
      const std::size_t size =
          static_cast<std::size_t>(ReadUnsigned(header, offset + 2, 2));
      offset += kExtensionPrefixSize;
      if (!ValidExtensionType(type) || size > header.size() - offset) {
        return false;
      }
      const std::span<const char> value = header.subspan(offset, size);
      metadata_.extensions.push_back(RpcHeaderExtension{
          .type = type,
          .value = std::vector<char>(value.begin(), value.end()),
      });
      offset += size;
    }
    return true;
  }

  const RpcCodecOptions options_;
  State state_ = State::kLength;
  std::size_t expected_frame_size_ = 0;
  std::size_t payload_offset_ = 0;
  RpcFrameMetadata metadata_;
};

}  // namespace

std::unique_ptr<ProtocolCodec> CreateRpcCodec(RpcCodecOptions options) {
  if (!ValidOptions(options)) {
    return nullptr;
  }
  return std::make_unique<RpcCodec>(options);
}

bool EncodeRpcFrame(const RpcFrameMetadata& metadata,
                    std::span<const char> payload, std::vector<char>* output,
                    RpcCodecOptions options) {
  if (output == nullptr || !ValidOptions(options) || !ValidMetadata(metadata)) {
    return false;
  }
  std::size_t header_size = kRpcFixedHeaderSize;
  if (!AddBounded(metadata.service_name.size(), options.max_header_bytes,
                  &header_size) ||
      !AddBounded(metadata.method_name.size(), options.max_header_bytes,
                  &header_size)) {
    return false;
  }
  for (const RpcHeaderExtension& extension : metadata.extensions) {
    if (!ValidExtensionType(extension.type) ||
        !AddBounded(kExtensionPrefixSize, options.max_header_bytes,
                    &header_size) ||
        !AddBounded(extension.value.size(), options.max_header_bytes,
                    &header_size)) {
      return false;
    }
  }
  std::size_t frame_size = header_size;
  if (!AddBounded(payload.size(), options.max_frame_bytes, &frame_size) ||
      frame_size + kRpcLengthPrefixSize > output->max_size() - output->size()) {
    return false;
  }

  output->reserve(output->size() + kRpcLengthPrefixSize + frame_size);
  AppendUnsigned(frame_size, 4, output);
  AppendUnsigned(kRpcMagic, 4, output);
  AppendUnsigned(metadata.version, 1, output);
  AppendUnsigned(static_cast<std::uint8_t>(metadata.message_type), 1, output);
  AppendUnsigned(header_size, 2, output);
  AppendUnsigned(metadata.request_id, 8, output);
  AppendUnsigned(static_cast<std::uint8_t>(metadata.serialization), 1, output);
  AppendUnsigned(static_cast<std::uint8_t>(metadata.compression), 1, output);
  AppendUnsigned(metadata.error_code, 2, output);
  AppendUnsigned(metadata.service_name.size(), 2, output);
  AppendUnsigned(metadata.method_name.size(), 2, output);
  output->insert(output->end(), metadata.service_name.begin(),
                 metadata.service_name.end());
  output->insert(output->end(), metadata.method_name.begin(),
                 metadata.method_name.end());
  for (const RpcHeaderExtension& extension : metadata.extensions) {
    AppendUnsigned(extension.type, 2, output);
    AppendUnsigned(extension.value.size(), 2, output);
    output->insert(output->end(), extension.value.begin(),
                   extension.value.end());
  }
  output->insert(output->end(), payload.begin(), payload.end());
  return true;
}

}  // namespace netlib
