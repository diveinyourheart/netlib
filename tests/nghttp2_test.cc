#include <nghttp2/nghttp2.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string_view>

namespace {

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::fprintf(stderr, "%s\n", message);
  }
  return condition;
}

struct SessionDeleter {
  void operator()(nghttp2_session* session) const {
    nghttp2_session_del(session);
  }
};
struct CallbacksDeleter {
  void operator()(nghttp2_session_callbacks* callbacks) const {
    nghttp2_session_callbacks_del(callbacks);
  }
};
using Session = std::unique_ptr<nghttp2_session, SessionDeleter>;
using Callbacks = std::unique_ptr<nghttp2_session_callbacks, CallbacksDeleter>;

struct ReceivedFrames {
  int settings = 0;
  int settings_ack = 0;
};

int OnFrame(nghttp2_session*, const nghttp2_frame* frame, void* user_data) {
  ReceivedFrames& received = *static_cast<ReceivedFrames*>(user_data);
  if (frame->hd.type == NGHTTP2_SETTINGS) {
    if ((frame->hd.flags & NGHTTP2_FLAG_ACK) != 0) {
      ++received.settings_ack;
    } else {
      ++received.settings;
    }
  }
  return 0;
}

// Exercise the linked protocol kernel entirely in memory. Fragment every write
// into single bytes to cover the public mem_send2/mem_recv2 API and preface.
bool Transfer(nghttp2_session* sender, nghttp2_session* receiver) {
  for (int writes = 0; writes != 16; ++writes) {
    const std::uint8_t* data = nullptr;
    nghttp2_ssize size = nghttp2_session_mem_send2(sender, &data);
    if (size < 0) {
      return false;
    }
    if (size == 0) {
      return true;
    }
    for (nghttp2_ssize offset = 0; offset < size; ++offset) {
      if (nghttp2_session_mem_recv2(receiver, data + offset, 1) != 1) {
        return false;
      }
    }
  }
  return false;
}

bool TestVersionAndMemorySessions() {
  const nghttp2_info* info = nghttp2_version(NGHTTP2_VERSION_NUM);
  if (!Check(
          info != nullptr && info->version_num == NGHTTP2_VERSION_NUM &&
              std::string_view(info->version_str) ==
                  NETLIB_NGHTTP2_EXPECTED_VERSION &&
              std::string_view(NGHTTP2_VERSION) ==
                  NETLIB_NGHTTP2_EXPECTED_VERSION,
          "nghttp2 header/runtime version does not match the pinned release")) {
    return false;
  }
  nghttp2_session_callbacks* raw_callbacks = nullptr;
  if (nghttp2_session_callbacks_new(&raw_callbacks) != 0) {
    return false;
  }
  Callbacks callbacks(raw_callbacks);
  nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks.get(),
                                                       OnFrame);
  ReceivedFrames client_frames;
  ReceivedFrames server_frames;
  nghttp2_session* raw_client = nullptr;
  int client_status =
      nghttp2_session_client_new(&raw_client, callbacks.get(), &client_frames);
  Session client(raw_client);
  nghttp2_session* raw_server = nullptr;
  int server_status =
      nghttp2_session_server_new(&raw_server, callbacks.get(), &server_frames);
  Session server(raw_server);
  if (!Check(client_status == 0 && server_status == 0,
             "nghttp2 memory session creation failed")) {
    return false;
  }
  if (nghttp2_submit_settings(client.get(), NGHTTP2_FLAG_NONE, nullptr, 0) !=
          0 ||
      nghttp2_submit_settings(server.get(), NGHTTP2_FLAG_NONE, nullptr, 0) !=
          0) {
    return false;
  }
  if (!Transfer(client.get(), server.get()) ||
      !Transfer(server.get(), client.get()) ||
      !Transfer(client.get(), server.get())) {
    return Check(false, "nghttp2 memory SETTINGS exchange failed");
  }
  return Check(client_frames.settings == 1 && client_frames.settings_ack == 1 &&
                   server_frames.settings == 1 &&
                   server_frames.settings_ack == 1,
               "nghttp2 did not acknowledge both SETTINGS frames");
}

}  // namespace

int main() { return TestVersionAndMemorySessions() ? 0 : 1; }
