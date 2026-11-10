#include <cstdio>

#include "netlib/http2_session.h"

#if defined(NGHTTP2_STATICLIB) || defined(NGHTTP2_NO_SSIZE_T)
#error "nghttp2 implementation macros must not leak to netlib consumers"
#endif

int main() {
  bool available = netlib::CreateHttp2Session() != nullptr;
  if (available != (NETLIB_TEST_HTTP2 != 0) ||
      netlib::CreateHttp2Session({.max_pending_responses = 0}) ||
      netlib::CreateHttp2Session({}, {.max_header_bytes = 0}) ||
      netlib::CreateHttp2Session(
          {}, {.header_timeout = std::chrono::milliseconds::zero()}) ||
      netlib::CreateHttp2Session({},
                                 {.drain_timeout = std::chrono::hours(25)})) {
    std::fprintf(stderr,
                 "HTTP/2 public factory configuration contract failed\n");
    return 1;
  }
  return 0;
}
