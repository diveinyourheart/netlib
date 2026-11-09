#include "netlib/protocol_session.h"

#include <cassert>
#include <cstring>
#include <limits>
#include <map>
#include <optional>
#include <utility>

namespace netlib {
namespace {

class CodecSession final : public ProtocolSession {
 public:
  CodecSession(std::unique_ptr<ProtocolCodec> codec,
               ProtocolSessionOptions options)
      : codec_(std::move(codec)), options_(options) {}

  SessionInputResult OnInput(std::span<const char> input) override;
  SessionResponseStatus OnResponse(const RequestContext& context,
                                   ApplicationResponse response,
                                   SessionResponseMode mode,
                                   bool close_after_write) override;
  SessionResponseStatus OnTimeout(const RequestContext& context) override;
  void BeginDrain() override { draining_ = true; }

  bool ProduceOutput(std::size_t byte_budget) override {
    return PendingOutput().size() <= byte_budget;
  }
  std::span<const char> PendingOutput() const override {
    return std::span<const char>(output_).subspan(output_offset_);
  }
  void ConsumeOutput(std::size_t size) override {
    assert(size <= PendingOutput().size());
    output_offset_ += size;
    if (output_offset_ == output_.size()) {
      output_.clear();
      output_offset_ = 0;
    }
  }
  bool WantsInput() const override { return !draining_ && !close_after_write_; }
  bool ShouldClose() const override {
    return close_after_write_ ||
           (draining_ && next_response_id_ == next_request_id_);
  }
  std::size_t MaxInputBufferSize() const override {
    return codec_->MaxDecodeBufferSize();
  }

 private:
  struct PendingResponse {
    // An empty response is a completion placeholder for an unordered reply.
    std::optional<ApplicationResponse> response;
    bool close_after_write = false;
  };

  bool AppendResponse(const ApplicationResponse& response, bool close);
  SessionResponseStatus DrainPending();

  const std::unique_ptr<ProtocolCodec> codec_;
  const ProtocolSessionOptions options_;
  std::uint64_t next_request_id_ = 0;
  std::uint64_t next_response_id_ = 0;
  std::map<std::uint64_t, PendingResponse> pending_responses_;
  std::vector<char> output_;
  std::size_t output_offset_ = 0;
  bool draining_ = false;
  bool close_after_write_ = false;
};

SessionInputResult CodecSession::OnInput(std::span<const char> input) {
  if (!WantsInput()) {
    return {.status = SessionInputStatus::kProtocolError, .requests = {}};
  }
  DecodeResult decoded = codec_->Decode(input);
  if (decoded.status == DecodeStatus::kNeedMore) {
    if (decoded.consumed_bytes != 0 ||
        decoded.required_bytes > MaxInputBufferSize() ||
        (decoded.required_bytes != 0 &&
         decoded.required_bytes <= input.size())) {
      return {.status = SessionInputStatus::kProtocolError, .requests = {}};
    }
    return {.required_bytes = decoded.required_bytes, .requests = {}};
  }
  if (decoded.consumed_bytes == 0 || decoded.consumed_bytes > input.size()) {
    return {.status = SessionInputStatus::kProtocolError, .requests = {}};
  }
  if (decoded.status == DecodeStatus::kConsumed) {
    if (decoded.payload_offset != 0 || decoded.payload_size != 0) {
      return {.status = SessionInputStatus::kProtocolError, .requests = {}};
    }
    return {.status = SessionInputStatus::kProgress,
            .consumed_bytes = decoded.consumed_bytes,
            .requests = {}};
  }
  if (decoded.status != DecodeStatus::kFrameReady ||
      decoded.payload_offset > decoded.consumed_bytes ||
      decoded.payload_size > decoded.consumed_bytes - decoded.payload_offset ||
      next_request_id_ == std::numeric_limits<std::uint64_t>::max()) {
    return {.status = SessionInputStatus::kProtocolError, .requests = {}};
  }
  std::span<const char> payload =
      input.subspan(decoded.payload_offset, decoded.payload_size);
  SessionInputResult result{.status = SessionInputStatus::kProgress,
                            .consumed_bytes = decoded.consumed_bytes,
                            .requests = {}};
  result.requests.push_back(SessionRequest{
      .context = {.request_id = next_request_id_++, .stream_id = std::nullopt},
      .metadata = std::move(decoded.metadata),
      .payload = std::vector<char>(payload.begin(), payload.end()),
  });
  return result;
}

bool CodecSession::AppendResponse(const ApplicationResponse& response,
                                  bool close) {
  assert(!close_after_write_);
  if (output_offset_ != 0) {
    std::size_t remaining = output_.size() - output_offset_;
    if (remaining != 0) {
      std::memmove(output_.data(), output_.data() + output_offset_, remaining);
    }
    output_.resize(remaining);
    output_offset_ = 0;
  }
  std::size_t old_size = output_.size();
  if (!codec_->Encode(response, &output_) ||
      output_.size() > options_.max_pending_output_bytes) {
    output_.resize(old_size);
    return false;
  }
  close_after_write_ = close || codec_->ShouldCloseAfterWrite(response);
  return true;
}

SessionResponseStatus CodecSession::DrainPending() {
  while (!close_after_write_) {
    auto current = pending_responses_.find(next_response_id_);
    if (current == pending_responses_.end()) {
      return SessionResponseStatus::kQueued;
    }
    PendingResponse pending = std::move(current->second);
    pending_responses_.erase(current);
    if (pending.response.has_value() &&
        !AppendResponse(*pending.response, pending.close_after_write)) {
      return SessionResponseStatus::kError;
    }
    ++next_response_id_;
  }
  // Never append later pipeline replies after the terminal protocol response.
  pending_responses_.clear();
  return SessionResponseStatus::kQueued;
}

SessionResponseStatus CodecSession::OnResponse(const RequestContext& context,
                                               ApplicationResponse response,
                                               SessionResponseMode mode,
                                               bool close_after_write) {
  std::uint64_t id = context.request_id;
  if (context.stream_id.has_value() || id < next_response_id_ ||
      id >= next_request_id_ || pending_responses_.contains(id)) {
    return SessionResponseStatus::kStale;
  }
  if (close_after_write_) {
    return SessionResponseStatus::kClosed;
  }
  if (id != next_response_id_ &&
      pending_responses_.size() >= options_.max_pending_responses) {
    return SessionResponseStatus::kError;
  }
  bool unordered = mode == SessionResponseMode::kUnordered;
  if (id == next_response_id_ || unordered) {
    if (!AppendResponse(response, close_after_write)) {
      return SessionResponseStatus::kError;
    }
    if (id == next_response_id_) {
      ++next_response_id_;
    } else {
      pending_responses_.emplace(id, PendingResponse{});
    }
  } else {
    pending_responses_.emplace(
        id, PendingResponse{.response = std::move(response),
                            .close_after_write = close_after_write});
  }
  return DrainPending();
}

SessionResponseStatus CodecSession::OnTimeout(const RequestContext& context) {
  std::optional<ApplicationResponse> response = codec_->CreateTimeoutResponse();
  if (!response.has_value()) {
    return SessionResponseStatus::kError;
  }
  return OnResponse(context, std::move(*response),
                    SessionResponseMode::kOrdered, true);
}

}  // namespace

std::unique_ptr<ProtocolSession> CreateCodecSession(
    std::unique_ptr<ProtocolCodec> codec, ProtocolSessionOptions options) {
  if (!codec || codec->MaxDecodeBufferSize() == 0 ||
      options.max_pending_responses == 0 ||
      options.max_pending_output_bytes == 0) {
    return nullptr;
  }
  return std::make_unique<CodecSession>(std::move(codec), options);
}

}  // namespace netlib
