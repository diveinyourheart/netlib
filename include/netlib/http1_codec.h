#ifndef NETLIB_INCLUDE_NETLIB_HTTP1_CODEC_H_
#define NETLIB_INCLUDE_NETLIB_HTTP1_CODEC_H_

#include <cstddef>
#include <memory>

#include "netlib/protocol_codec.h"

namespace netlib {

struct Http1CodecOptions {
  std::size_t max_header_bytes = 16 * 1024;
  std::size_t max_body_bytes = 16 * 1024 * 1024;
};

std::unique_ptr<ProtocolCodec> CreateHttp1Codec(Http1CodecOptions options = {});

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_HTTP1_CODEC_H_
