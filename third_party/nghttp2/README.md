# nghttp2 dependency

netlib pins the MIT-licensed **nghttp2 1.71.0** C protocol library. Source code is
fetched into the build directory when `NETLIB_ENABLE_HTTP2=ON`; this directory
only retains the upstream [COPYING](COPYING) notice.

- [Official release](https://github.com/nghttp2/nghttp2/releases/tag/v1.71.0)
- Archive: `nghttp2-1.71.0.tar.xz` (the versioned release asset, not GitHub's
  automatically generated source archives).
- [Official checksums](https://github.com/nghttp2/nghttp2/releases/download/v1.71.0/checksums.txt)
- SHA-256: `c0f55121d88650e4b9227aec28cebaf664d2417ca60fe70be0f8d38829f52116`.
- [Build integration](../../cmake/NetlibNghttp2.cmake)
- [Build and offline usage](../../docs/http2_build.md)

The protocol library is built statically. No nghttp/nghttpd/nghttpx/h2load,
HPACK utilities, examples, upstream tests, documentation, TLS or HTTP/3
application dependencies are built. netlib's C++ code remains C++20.

The MIT copyright and permission notice must accompany distributed copies or
substantial portions of the library. Preserve the notice when distributing
applications that statically link this dependency. This integration does not
change the licenses of other bundled libraries.
