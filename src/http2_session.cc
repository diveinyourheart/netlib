#include "netlib/http2_session.h"

#if defined(NETLIB_HAS_HTTP2)
#include <nghttp2/nghttp2.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace netlib {
namespace {

bool IsConnectionField(std::string_view name) {
  return name == "connection" || name == "keep-alive" ||
         name == "proxy-connection" || name == "transfer-encoding" ||
         name == "upgrade" || name == "te";
}

class Http2Session final : public ProtocolSession {
 public:
  Http2Session(ProtocolSessionOptions limits, Http2SessionOptions options)
      : limits_(limits), options_(options) {}
  ~Http2Session() override { nghttp2_session_del(session_); }
  bool Initialize();
  SessionInputResult OnInput(std::span<const char> input) override;
  SessionResponseStatus OnResponse(const RequestContext& context,
                                   ApplicationResponse response,
                                   SessionResponseMode mode,
                                   bool close_after_write) override;
  SessionResponseStatus OnTimeout(const RequestContext& context) override;
  void BeginDrain() override;
  std::optional<Clock::time_point> NextDeadline() const override;
  void OnDeadline(Clock::time_point now) override;
  bool ProduceOutput(std::size_t byte_budget) override;
  std::vector<RequestContext> TakeCancelledRequests() override {
    std::vector<RequestContext> result;
    result.swap(cancelled_requests_);
    return result;
  }
  std::span<const char> PendingOutput() const override {
    return std::span<const char>(output_).subspan(output_offset_);
  }
  void ConsumeOutput(std::size_t size) override {
    assert(size <= PendingOutput().size());
    output_offset_ += size;
  }
  bool WantsInput() const override { return !failed_ && !ShouldClose(); }
  bool ShouldClose() const override {
    return failed_ || (!nghttp2_session_want_read(session_) &&
                       !nghttp2_session_want_write(session_));
  }
  bool ShouldHalfClose() const override {
    return draining_ && !failed_ && ShouldClose();
  }
  std::size_t MaxInputBufferSize() const override {
    return options_.max_input_chunk_bytes;
  }

 private:
  using Deadlines = std::multimap<Clock::time_point, std::int32_t>;
  enum class Stage { kHeaders, kBody, kBusiness, kResponse };
  struct Stream {
    HttpRequestMetadata metadata{.method = {},
                                 .target = {},
                                 .version = "HTTP/2",
                                 .headers = {},
                                 .keep_alive = true};
    std::vector<char> body;
    std::vector<char> response_body;
    std::size_t response_offset = 0;
    std::size_t header_bytes = 0;
    RequestContext context;
    bool dispatched = false;
    bool responded = false;
    bool rejected = false;
    bool headers_received = false;
    Stage stage = Stage::kHeaders;
    std::optional<Deadlines::iterator> deadline;
  };

  static Http2Session& Self(void* data) {
    return *static_cast<Http2Session*>(data);
  }
  static int BeginHeaders(nghttp2_session*, const nghttp2_frame* frame,
                          void* data);
  static int Header(nghttp2_session*, const nghttp2_frame* frame,
                    const std::uint8_t* name, std::size_t name_size,
                    const std::uint8_t* value, std::size_t value_size,
                    std::uint8_t, void* data);
  static int Data(nghttp2_session*, std::uint8_t, std::int32_t id,
                  const std::uint8_t* bytes, std::size_t size, void* data);
  static int Frame(nghttp2_session*, const nghttp2_frame* frame, void* data);
  static int Closed(nghttp2_session*, std::int32_t id, std::uint32_t,
                    void* data);
  static nghttp2_ssize Send(nghttp2_session*, const std::uint8_t* bytes,
                            std::size_t size, int, void* data);
  static nghttp2_ssize ReadBody(nghttp2_session*, std::int32_t,
                                std::uint8_t* bytes, std::size_t size,
                                std::uint32_t* flags,
                                nghttp2_data_source* source, void*);
  Stream* Find(std::int32_t id);
  Stream* Find(const RequestContext& context);
  int Reject(std::int32_t id, std::uint32_t error);
  void SetDeadline(std::int32_t id, Stream& stream,
                   std::optional<Clock::time_point> deadline);
  void ObserveFrames(std::span<const char> input);
  int Fatal() {
    failed_ = true;
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }

  const ProtocolSessionOptions limits_;
  const Http2SessionOptions options_;
  nghttp2_session* session_ = nullptr;
  std::unordered_map<std::int32_t, std::unique_ptr<Stream>> streams_;
  std::unordered_map<std::uint64_t, std::int32_t> requests_;
  std::vector<std::int32_t> completed_streams_;
  std::vector<RequestContext> cancelled_requests_;
  std::vector<char> output_;
  std::size_t output_offset_ = 0;
  std::size_t output_budget_ = 0;
  std::size_t body_bytes_ = 0;
  std::uint64_t next_request_id_ = 0;
  std::int32_t last_accepted_stream_ = 0;
  bool draining_ = false;
  bool failed_ = false;
  Deadlines deadlines_;
  std::optional<Clock::time_point> handshake_deadline_;
  std::optional<Clock::time_point> frame_deadline_;
  std::optional<Clock::time_point> drain_deadline_;
  std::size_t preface_remaining_ = 24;
  std::array<unsigned char, 9> frame_header_{};
  std::size_t frame_header_size_ = 0;
  std::size_t frame_payload_remaining_ = 0;
};

bool Http2Session::Initialize() {
  handshake_deadline_ = Clock::now() + options_.header_timeout;
  nghttp2_session_callbacks* callbacks = nullptr;
  if (nghttp2_session_callbacks_new(&callbacks) != 0) return false;
  nghttp2_session_callbacks_set_on_begin_headers_callback(callbacks,
                                                          BeginHeaders);
  nghttp2_session_callbacks_set_on_header_callback(callbacks, Header);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks, Data);
  nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, Frame);
  nghttp2_session_callbacks_set_on_stream_close_callback(callbacks, Closed);
  nghttp2_session_callbacks_set_send_callback2(callbacks, Send);
  nghttp2_option* settings = nullptr;
  if (nghttp2_option_new(&settings) != 0) {
    nghttp2_session_callbacks_del(callbacks);
    return false;
  }
  nghttp2_option_set_no_auto_window_update(settings, 1);
  nghttp2_option_set_max_send_header_block_length(settings,
                                                  options_.max_header_bytes);
  int result =
      nghttp2_session_server_new2(&session_, callbacks, this, settings);
  nghttp2_option_del(settings);
  nghttp2_session_callbacks_del(callbacks);
  if (result != 0) return false;
  const nghttp2_settings_entry entries[]{
      {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS,
       static_cast<std::uint32_t>(options_.max_concurrent_streams)},
      {NGHTTP2_SETTINGS_MAX_HEADER_LIST_SIZE,
       static_cast<std::uint32_t>(options_.max_header_bytes)},
  };
  return nghttp2_submit_settings(session_, NGHTTP2_FLAG_NONE, entries, 2) == 0;
}

Http2Session::Stream* Http2Session::Find(std::int32_t id) {
  auto found = streams_.find(id);
  return found == streams_.end() ? nullptr : found->second.get();
}

Http2Session::Stream* Http2Session::Find(const RequestContext& context) {
  auto found = requests_.find(context.request_id);
  if (found == requests_.end() || !context.stream_id.has_value() ||
      *context.stream_id != static_cast<std::uint32_t>(found->second)) {
    return nullptr;
  }
  Stream* stream = Find(found->second);
  return stream != nullptr && stream->context == context ? stream : nullptr;
}

int Http2Session::Reject(std::int32_t id, std::uint32_t error) {
  Stream* stream = Find(id);
  if (stream != nullptr) {
    SetDeadline(id, *stream, std::nullopt);
    stream->rejected = true;
    body_bytes_ -= stream->body.size();
    stream->body.clear();
  }
  return nghttp2_submit_rst_stream(session_, NGHTTP2_FLAG_NONE, id, error) == 0
             ? 0
             : Fatal();
}

int Http2Session::BeginHeaders(nghttp2_session*, const nghttp2_frame* frame,
                               void* data) {
  Http2Session& self = Self(data);
  std::int32_t id = frame->hd.stream_id;
  if (frame->headers.cat != NGHTTP2_HCAT_REQUEST) {
    // Request trailers/streaming are a later protocol feature, not a new task.
    return self.Reject(id, NGHTTP2_PROTOCOL_ERROR);
  }
  if (self.draining_ ||
      self.streams_.size() >= self.options_.max_concurrent_streams) {
    return self.Reject(id, NGHTTP2_REFUSED_STREAM);
  }
  std::unique_ptr<Stream> stream = std::make_unique<Stream>();
  self.SetDeadline(id, *stream, Clock::now() + self.options_.header_timeout);
  self.streams_.emplace(id, std::move(stream));
  self.last_accepted_stream_ = std::max(self.last_accepted_stream_, id);
  return 0;
}

int Http2Session::Header(nghttp2_session*, const nghttp2_frame* frame,
                         const std::uint8_t* name, std::size_t name_size,
                         const std::uint8_t* value, std::size_t value_size,
                         std::uint8_t, void* data) {
  Http2Session& self = Self(data);
  Stream* stream = self.Find(frame->hd.stream_id);
  if (stream == nullptr || stream->rejected) return 0;
  std::size_t remaining = self.options_.max_header_bytes - stream->header_bytes;
  if (remaining < 32 || name_size > remaining - 32 ||
      value_size > remaining - 32 - name_size) {
    return self.Reject(frame->hd.stream_id, NGHTTP2_ENHANCE_YOUR_CALM);
  }
  stream->header_bytes += name_size + value_size + 32;
  std::string_view field(reinterpret_cast<const char*>(name), name_size);
  std::string text(reinterpret_cast<const char*>(value), value_size);
  if (field == ":method")
    stream->metadata.method = std::move(text);
  else if (field == ":path")
    stream->metadata.target = std::move(text);
  else if (field == ":scheme")
    stream->metadata.scheme = std::move(text);
  else if (field == ":authority")
    stream->metadata.authority = std::move(text);
  else
    stream->metadata.headers.push_back({std::string(field), std::move(text)});
  return 0;
}

int Http2Session::Data(nghttp2_session*, std::uint8_t, std::int32_t id,
                       const std::uint8_t* bytes, std::size_t size,
                       void* data) {
  Http2Session& self = Self(data);
  Stream* stream = self.Find(id);
  if (stream != nullptr && !stream->rejected) {
    if (size != 0 && stream->stage == Stage::kBody) {
      self.SetDeadline(id, *stream,
                       Clock::now() + self.options_.body_idle_timeout);
    }
    if (size > self.options_.max_body_bytes - stream->body.size() ||
        size > self.options_.max_buffered_body_bytes - self.body_bytes_) {
      if (self.Reject(id, NGHTTP2_ENHANCE_YOUR_CALM) != 0) return self.Fatal();
    } else {
      stream->body.insert(stream->body.end(), bytes, bytes + size);
      self.body_bytes_ += size;
    }
  }
  // Return credit after bounded storage/discard, not only after END_STREAM.
  // Otherwise a request larger than the initial window can never complete.
  int result = stream == nullptr
                   ? nghttp2_session_consume_connection(self.session_, size)
                   : nghttp2_session_consume(self.session_, id, size);
  return result == 0 ? 0 : self.Fatal();
}

int Http2Session::Frame(nghttp2_session*, const nghttp2_frame* frame,
                        void* data) {
  Http2Session& self = Self(data);
  if (frame->hd.type == NGHTTP2_SETTINGS) self.handshake_deadline_.reset();
  if (frame->hd.type == NGHTTP2_GOAWAY) self.BeginDrain();
  Stream* stream = self.Find(frame->hd.stream_id);
  if (stream == nullptr || stream->rejected) return 0;
  if (frame->hd.type == NGHTTP2_HEADERS) {
    stream->stage = Stage::kBody;
    self.SetDeadline(frame->hd.stream_id, *stream,
                     Clock::now() + self.options_.body_idle_timeout);
    stream->headers_received = true;
    // CONNECT tunnels and request trailers are outside the unary API.
    if (stream->metadata.method == "CONNECT" ||
        stream->metadata.method.empty() || stream->metadata.target.empty() ||
        stream->metadata.scheme.empty()) {
      return self.Reject(frame->hd.stream_id, NGHTTP2_PROTOCOL_ERROR);
    }
  }
  if ((frame->hd.type == NGHTTP2_HEADERS || frame->hd.type == NGHTTP2_DATA) &&
      (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0 &&
      stream->headers_received && !stream->dispatched) {
    self.completed_streams_.push_back(frame->hd.stream_id);
    stream->stage = Stage::kBusiness;
    self.SetDeadline(frame->hd.stream_id, *stream, std::nullopt);
  }
  return 0;
}

int Http2Session::Closed(nghttp2_session*, std::int32_t id, std::uint32_t,
                         void* data) {
  Http2Session& self = Self(data);
  auto found = self.streams_.find(id);
  if (found == self.streams_.end()) return 0;
  Stream& stream = *found->second;
  self.SetDeadline(id, stream, std::nullopt);
  if (stream.dispatched) {
    self.requests_.erase(stream.context.request_id);
    if (!stream.responded) {
      self.cancelled_requests_.push_back(stream.context);
    }
  }
  self.body_bytes_ -= stream.body.size() + stream.response_body.size();
  self.streams_.erase(found);
  return 0;
}

SessionInputResult Http2Session::OnInput(std::span<const char> input) {
  SessionInputResult result;
  if (input.empty()) return result;
  if (!WantsInput()) {
    result.status = SessionInputStatus::kProtocolError;
    return result;
  }
  completed_streams_.clear();
  nghttp2_ssize consumed = nghttp2_session_mem_recv2(
      session_, reinterpret_cast<const std::uint8_t*>(input.data()),
      std::min(input.size(), options_.max_input_chunk_bytes));
  result.cancelled_requests = TakeCancelledRequests();
  if (consumed <= 0 || failed_) {
    failed_ = true;
    result.status = SessionInputStatus::kProtocolError;
    return result;
  }
  result.status = SessionInputStatus::kProgress;
  result.consumed_bytes = static_cast<std::size_t>(consumed);
  ObserveFrames(input.first(result.consumed_bytes));
  for (std::int32_t id : completed_streams_) {
    Stream* stream = Find(id);
    if (stream == nullptr || stream->rejected || stream->dispatched) continue;
    if (next_request_id_ == std::numeric_limits<std::uint64_t>::max()) {
      failed_ = true;
      result.status = SessionInputStatus::kProtocolError;
      return result;
    }
    stream->context = {.request_id = next_request_id_++,
                       .stream_id = static_cast<std::uint32_t>(id)};
    stream->dispatched = true;
    stream->metadata.content_length = stream->body.size();
    body_bytes_ -= stream->body.size();
    requests_.emplace(stream->context.request_id, id);
    result.requests.push_back({stream->context, std::move(stream->metadata),
                               std::move(stream->body)});
    // Preserve only the request method needed to encode HEAD responses.
    stream->metadata.method =
        std::get<HttpRequestMetadata>(result.requests.back().metadata).method;
  }
  return result;
}

nghttp2_ssize Http2Session::Send(nghttp2_session*, const std::uint8_t* bytes,
                                 std::size_t size, int, void* data) {
  Http2Session& self = Self(data);
  std::size_t accepted =
      std::min(size, self.output_budget_ - self.output_.size());
  if (accepted == 0) return NGHTTP2_ERR_WOULDBLOCK;
  self.output_.insert(self.output_.end(), bytes, bytes + accepted);
  return static_cast<nghttp2_ssize>(accepted);
}

bool Http2Session::ProduceOutput(std::size_t byte_budget) {
  if (failed_) return false;
  output_budget_ = std::min(byte_budget, limits_.max_pending_output_bytes);
  if (!PendingOutput().empty()) return PendingOutput().size() <= output_budget_;
  if (output_budget_ == 0) return false;
  output_.clear();
  output_offset_ = 0;
  // The send callback copies only within budget. nghttp2 retains partial frame
  // state; no pointer returned by mem_send2 survives another protocol call.
  if (nghttp2_session_send(session_) != 0) failed_ = true;
  return !failed_;
}

nghttp2_ssize Http2Session::ReadBody(nghttp2_session*, std::int32_t,
                                     std::uint8_t* bytes, std::size_t size,
                                     std::uint32_t* flags,
                                     nghttp2_data_source* source, void*) {
  Stream& stream = *static_cast<Stream*>(source->ptr);
  // Cap each DATA read independently of a peer's larger maximum frame size.
  std::size_t count =
      std::min({size, std::size_t{16 * 1024},
                stream.response_body.size() - stream.response_offset});
  if (count != 0) {
    std::memcpy(bytes, stream.response_body.data() + stream.response_offset,
                count);
  }
  stream.response_offset += count;
  if (stream.response_offset == stream.response_body.size()) {
    *flags |= NGHTTP2_DATA_FLAG_EOF;
  }
  return static_cast<nghttp2_ssize>(count);
}

SessionResponseStatus Http2Session::OnResponse(const RequestContext& context,
                                               ApplicationResponse response,
                                               SessionResponseMode,
                                               bool close_after_write) {
  Stream* stream = Find(context);
  if (stream == nullptr || stream->responded || stream->rejected) {
    return SessionResponseStatus::kStale;
  }
  HttpResponse* http = std::get_if<HttpResponse>(&response);
  std::int32_t id = static_cast<std::int32_t>(*context.stream_id);
  auto reject = [&] {
    return Reject(id, NGHTTP2_INTERNAL_ERROR) == 0
               ? SessionResponseStatus::kQueued
               : SessionResponseStatus::kError;
  };
  if (http == nullptr || http->status_code < 200 || http->status_code > 599 ||
      http->body.size() > options_.max_body_bytes ||
      http->body.size() > options_.max_buffered_body_bytes - body_bytes_) {
    return reject();
  }
  bool head = stream->metadata.method == "HEAD";
  bool no_body = http->status_code == 204 || http->status_code == 304;
  if (no_body && !http->body.empty()) return reject();
  std::string status = std::to_string(http->status_code);
  std::string length = std::to_string(http->body.size());
  std::size_t header_bytes = status.size() + 7 + 32;
  bool has_length = false;
  for (HttpHeader& header : http->headers) {
    for (char& byte : header.name) {
      if (byte >= 'A' && byte <= 'Z')
        byte = static_cast<char>(byte + ('a' - 'A'));
    }
    std::size_t remaining = options_.max_header_bytes -
                            std::min(header_bytes, options_.max_header_bytes);
    if (remaining < 32 || header.name.size() > remaining - 32 ||
        header.value.size() > remaining - 32 - header.name.size() ||
        header.name.empty() || header.name.front() == ':' ||
        IsConnectionField(header.name) ||
        (!header.value.empty() &&
         (header.value.front() == ' ' || header.value.front() == '\t' ||
          header.value.back() == ' ' || header.value.back() == '\t')) ||
        !nghttp2_check_header_name(
            reinterpret_cast<const std::uint8_t*>(header.name.data()),
            header.name.size()) ||
        !nghttp2_check_header_value(
            reinterpret_cast<const std::uint8_t*>(header.value.data()),
            header.value.size())) {
      return reject();
    }
    header_bytes += header.name.size() + header.value.size() + 32;
    if (header.name == "content-length") {
      std::uint64_t declared = 0;
      std::from_chars_result parsed =
          std::from_chars(header.value.data(),
                          header.value.data() + header.value.size(), declared);
      if (has_length || parsed.ec != std::errc{} ||
          parsed.ptr != header.value.data() + header.value.size() ||
          http->status_code == 204 ||
          (!no_body && declared != http->body.size())) {
        return reject();
      }
      has_length = true;
    }
  }
  if (!has_length && !no_body) header_bytes += 14 + length.size() + 32;
  if (header_bytes > options_.max_header_bytes) return reject();
  std::vector<nghttp2_nv> fields;
  fields.reserve(http->headers.size() + 2);
  auto append = [&fields](std::string_view name, std::string_view value) {
    fields.push_back(
        {reinterpret_cast<std::uint8_t*>(const_cast<char*>(name.data())),
         reinterpret_cast<std::uint8_t*>(const_cast<char*>(value.data())),
         name.size(), value.size(), NGHTTP2_NV_FLAG_NONE});
  };
  append(":status", status);
  for (const HttpHeader& header : http->headers)
    append(header.name, header.value);
  if (!has_length && !no_body) append("content-length", length);
  if (!head) stream->response_body = std::move(http->body);
  body_bytes_ += stream->response_body.size();
  nghttp2_data_provider2 provider{.source = {.ptr = stream},
                                  .read_callback = ReadBody};
  if (nghttp2_submit_response2(
          session_, id, fields.data(), fields.size(),
          stream->response_body.empty() ? nullptr : &provider) != 0) {
    return reject();
  }
  stream->responded = true;
  stream->stage = Stage::kResponse;
  SetDeadline(id, *stream, Clock::now() + options_.response_timeout);
  if (close_after_write || !http->keep_alive) BeginDrain();
  return failed_ ? SessionResponseStatus::kError
                 : SessionResponseStatus::kQueued;
}

SessionResponseStatus Http2Session::OnTimeout(const RequestContext& context) {
  return OnResponse(context,
                    HttpResponse{.status_code = 504,
                                 .reason_phrase = {},
                                 .headers = {},
                                 .body = {'t', 'i', 'm', 'e', 'o', 'u', 't'}},
                    SessionResponseMode::kUnordered, false);
}

void Http2Session::BeginDrain() {
  if (draining_) return;
  draining_ = true;
  drain_deadline_ = Clock::now() + options_.drain_timeout;
  if (nghttp2_submit_goaway(session_, NGHTTP2_FLAG_NONE, last_accepted_stream_,
                            NGHTTP2_NO_ERROR, nullptr, 0) != 0)
    failed_ = true;
}

void Http2Session::SetDeadline(std::int32_t id, Stream& stream,
                               std::optional<Clock::time_point> deadline) {
  if (stream.deadline.has_value()) {
    deadlines_.erase(*stream.deadline);
    stream.deadline.reset();
  }
  if (deadline.has_value()) stream.deadline = deadlines_.emplace(*deadline, id);
}

std::optional<ProtocolSession::Clock::time_point> Http2Session::NextDeadline()
    const {
  std::optional<Clock::time_point> result;
  auto include = [&result](std::optional<Clock::time_point> deadline) {
    if (deadline.has_value() && (!result.has_value() || *deadline < *result)) {
      result = deadline;
    }
  };
  include(handshake_deadline_);
  include(frame_deadline_);
  include(drain_deadline_);
  if (!deadlines_.empty()) include(deadlines_.begin()->first);
  return result;
}

void Http2Session::OnDeadline(Clock::time_point now) {
  if ((handshake_deadline_ && now >= *handshake_deadline_) ||
      (frame_deadline_ && now >= *frame_deadline_) ||
      (drain_deadline_ && now >= *drain_deadline_)) {
    failed_ = true;
    return;
  }
  while (!deadlines_.empty() && deadlines_.begin()->first <= now) {
    std::int32_t id = deadlines_.begin()->second;
    Stream* stream = Find(id);
    assert(stream != nullptr);
    if (stream->stage == Stage::kHeaders) {
      // An incomplete HPACK block cannot be abandoned without compromising the
      // shared compression/framing state. Bound it by closing the connection.
      failed_ = true;
      return;
    }
    if (Reject(id, NGHTTP2_CANCEL) != 0) return;
  }
}

void Http2Session::ObserveFrames(std::span<const char> input) {
  std::size_t preface = std::min(preface_remaining_, input.size());
  preface_remaining_ -= preface;
  input = input.subspan(preface);
  while (!input.empty()) {
    if (!frame_deadline_)
      frame_deadline_ = Clock::now() + options_.header_timeout;
    if (frame_header_size_ < frame_header_.size()) {
      std::size_t count =
          std::min(input.size(), frame_header_.size() - frame_header_size_);
      std::memcpy(frame_header_.data() + frame_header_size_, input.data(),
                  count);
      frame_header_size_ += count;
      input = input.subspan(count);
      if (frame_header_size_ < frame_header_.size()) return;
      frame_payload_remaining_ = (std::size_t{frame_header_[0]} << 16) |
                                 (std::size_t{frame_header_[1]} << 8) |
                                 frame_header_[2];
    }
    std::size_t count = std::min(input.size(), frame_payload_remaining_);
    input = input.subspan(count);
    frame_payload_remaining_ -= count;
    if (frame_payload_remaining_ == 0) {
      frame_header_size_ = 0;
      frame_deadline_.reset();
    }
  }
}

}  // namespace

std::unique_ptr<ProtocolSession> CreateHttp2Session(
    ProtocolSessionOptions limits, Http2SessionOptions options) {
  if (limits.max_pending_responses == 0 ||
      limits.max_pending_output_bytes == 0 ||
      options.max_concurrent_streams == 0 || options.max_header_bytes < 128 ||
      options.max_header_bytes > std::numeric_limits<std::uint32_t>::max() ||
      options.max_body_bytes == 0 || options.max_buffered_body_bytes == 0 ||
      options.max_input_chunk_bytes == 0)
    return nullptr;
  for (std::chrono::milliseconds timeout :
       {options.header_timeout, options.body_idle_timeout,
        options.response_timeout, options.drain_timeout}) {
    if (timeout <= std::chrono::milliseconds::zero() ||
        timeout > std::chrono::hours(24)) {
      return nullptr;
    }
  }
  options.max_concurrent_streams =
      std::min({options.max_concurrent_streams, limits.max_pending_responses,
                std::size_t{0x7fffffff}});
  std::unique_ptr<Http2Session> session =
      std::make_unique<Http2Session>(limits, options);
  return session->Initialize() ? std::move(session) : nullptr;
}

}  // namespace netlib
#else
namespace netlib {
std::unique_ptr<ProtocolSession> CreateHttp2Session(ProtocolSessionOptions,
                                                    Http2SessionOptions) {
  return nullptr;
}
}  // namespace netlib
#endif
