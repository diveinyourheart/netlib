#ifndef NETLIB_INCLUDE_NETLIB_UNIQUE_FD_H_
#define NETLIB_INCLUDE_NETLIB_UNIQUE_FD_H_

#include "netlib/platform.h"

namespace netlib {

// Owns one native socket (or POSIX descriptor on Linux) and closes it exactly
// once.
class UniqueFd {
 public:
  UniqueFd() noexcept = default;
  explicit UniqueFd(SocketHandle fd) noexcept;
  ~UniqueFd();

  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;
  UniqueFd(UniqueFd&& other) noexcept;
  UniqueFd& operator=(UniqueFd&& other) noexcept;

  SocketHandle get() const noexcept;
  explicit operator bool() const noexcept;
  SocketHandle Release() noexcept;
  void Reset(SocketHandle fd = kInvalidSocket) noexcept;

 private:
  SocketHandle fd_ = kInvalidSocket;
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_UNIQUE_FD_H_
