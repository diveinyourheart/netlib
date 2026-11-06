#include "netlib/unique_fd.h"

#include <unistd.h>

#include <utility>

namespace netlib {

UniqueFd::UniqueFd(int fd) noexcept : fd_(fd) {}

UniqueFd::~UniqueFd() { Reset(); }

UniqueFd::UniqueFd(UniqueFd&& other) noexcept : fd_(other.Release()) {}

UniqueFd& UniqueFd::operator=(UniqueFd&& other) noexcept {
  if (this != &other) {
    Reset(other.Release());
  }
  return *this;
}

int UniqueFd::get() const noexcept { return fd_; }

UniqueFd::operator bool() const noexcept { return fd_ >= 0; }

int UniqueFd::Release() noexcept {
  int fd = fd_;
  fd_ = -1;
  return fd;
}

void UniqueFd::Reset(int fd) noexcept {
  if (fd_ == fd) {
    return;
  }
  if (fd_ >= 0) {
    close(fd_);
  }
  fd_ = fd;
}

}  // namespace netlib
