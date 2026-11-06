#ifndef NETLIB_INCLUDE_NETLIB_UNIQUE_FD_H_
#define NETLIB_INCLUDE_NETLIB_UNIQUE_FD_H_

namespace netlib {

// Owns one POSIX file descriptor and closes it exactly once.
class UniqueFd {
 public:
  UniqueFd() noexcept = default;
  explicit UniqueFd(int fd) noexcept;
  ~UniqueFd();

  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;
  UniqueFd(UniqueFd&& other) noexcept;
  UniqueFd& operator=(UniqueFd&& other) noexcept;

  int get() const noexcept;
  explicit operator bool() const noexcept;
  int Release() noexcept;
  void Reset(int fd = -1) noexcept;

 private:
  int fd_ = -1;
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_UNIQUE_FD_H_
