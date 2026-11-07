#include "netlib/unique_fd.h"

#include <utility>

namespace netlib {

UniqueFd::UniqueFd(SocketHandle fd) noexcept : fd_(fd) {}

UniqueFd::~UniqueFd() { Reset(); }

UniqueFd::UniqueFd(UniqueFd&& other) noexcept : fd_(other.Release()) {}

UniqueFd& UniqueFd::operator=(UniqueFd&& other) noexcept {
  if (this != &other) {
    Reset(other.Release());
  }
  return *this;
}

SocketHandle UniqueFd::get() const noexcept { return fd_; }

UniqueFd::operator bool() const noexcept { return fd_ != kInvalidSocket; }

SocketHandle UniqueFd::Release() noexcept {
  SocketHandle fd = fd_;
  fd_ = kInvalidSocket;
  return fd;
}

void UniqueFd::Reset(SocketHandle fd) noexcept {
  if (fd_ == fd) {
    return;
  }
  if (fd_ != kInvalidSocket) {
    platform::CloseSocket(fd_);
  }
  fd_ = fd;
}

}  // namespace netlib
