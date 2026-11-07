#include "netlib/platform.h"

#ifdef _WIN32
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <array>
#include <cerrno>
#include <climits>
#include <system_error>
#include <utility>
#include <vector>

namespace netlib::platform {
namespace {
#ifdef _WIN32
using AddressLength = int;
bool InitializeSockets() {
  struct Runtime {
    int error;
    Runtime() {
      WSADATA data{};
      error = WSAStartup(MAKEWORD(2, 2), &data);
    }
    ~Runtime() {
      if (error == 0) WSACleanup();
    }
  };
  static Runtime runtime;
  if (runtime.error != 0) WSASetLastError(runtime.error);
  return runtime.error == 0;
}
void RestoreError(int error) { WSASetLastError(error); }
#else
using AddressLength = socklen_t;
bool InitializeSockets() { return true; }
void RestoreError(int error) { errno = error; }
#endif
SocketHandle CloseFailed(SocketHandle socket) {
  int error = LastError();
  CloseSocket(socket);
  RestoreError(error);
  return kInvalidSocket;
}
bool SetIntOption(SocketHandle socket, int option, int value) {
  return setsockopt(socket, SOL_SOCKET, option,
                    reinterpret_cast<const char*>(&value), sizeof(value)) == 0;
}
}  // namespace

int LastError() noexcept {
#ifdef _WIN32
  return WSAGetLastError();
#else
  return errno;
#endif
}
bool Interrupted(int error) noexcept {
#ifdef _WIN32
  return error == WSAEINTR;
#else
  return error == EINTR;
#endif
}
bool WouldBlock(int error) noexcept {
#ifdef _WIN32
  return error == WSAEWOULDBLOCK;
#else
  return error == EAGAIN || error == EWOULDBLOCK;
#endif
}
bool TransientAcceptError(int error) noexcept {
#ifdef _WIN32
  return error == WSAECONNRESET || error == WSAECONNABORTED;
#else
  return error == ECONNABORTED || error == EPROTO;
#endif
}
std::string ErrorMessage(int error) {
#ifdef _WIN32
  return std::system_category().message(error);
#else
  return std::generic_category().message(error);
#endif
}
void CloseSocket(SocketHandle socket) noexcept {
  if (socket == kInvalidSocket) return;
#ifdef _WIN32
  closesocket(socket);
#else
  close(socket);
#endif
}
bool SetNonBlocking(SocketHandle socket) {
#ifdef _WIN32
  u_long enabled = 1;
  return ioctlsocket(socket, FIONBIO, &enabled) == 0;
#else
  int flags = fcntl(socket, F_GETFL, 0);
  return flags >= 0 && fcntl(socket, F_SETFL, flags | O_NONBLOCK) == 0;
#endif
}
bool SetCloseOnExec(SocketHandle socket) {
#ifdef _WIN32
  if (SetHandleInformation(reinterpret_cast<HANDLE>(socket),
                           HANDLE_FLAG_INHERIT, 0))
    return true;
  WSASetLastError(static_cast<int>(GetLastError()));
  return false;
#else
  int flags = fcntl(socket, F_GETFD, 0);
  return flags >= 0 && fcntl(socket, F_SETFD, flags | FD_CLOEXEC) == 0;
#endif
}
SocketHandle CreateTcpSocket(bool nonblocking) {
  if (!InitializeSockets()) return kInvalidSocket;
#ifdef _WIN32
  SocketHandle result = WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr,
                                   0, WSA_FLAG_NO_HANDLE_INHERIT);
#else
  SocketHandle result =
      socket(AF_INET,
             SOCK_STREAM | SOCK_CLOEXEC | (nonblocking ? SOCK_NONBLOCK : 0), 0);
#endif
  if (result == kInvalidSocket) return result;
#ifdef _WIN32
  if (!SetCloseOnExec(result) || (nonblocking && !SetNonBlocking(result)))
    return CloseFailed(result);
#endif
  return result;
}
SocketHandle ListenTcp(std::uint16_t requested_port, std::uint16_t& port,
                       bool loopback_only) {
  SocketHandle result = CreateTcpSocket();
  if (result == kInvalidSocket) return result;
#ifdef _WIN32
  constexpr int option = SO_EXCLUSIVEADDRUSE;
#else
  constexpr int option = SO_REUSEADDR;
#endif
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(loopback_only ? INADDR_LOOPBACK : INADDR_ANY);
  address.sin_port = htons(requested_port);
  AddressLength length = sizeof(address);
  if (!SetIntOption(result, option, 1) ||
      bind(result, reinterpret_cast<const sockaddr*>(&address),
           sizeof(address)) != 0 ||
      listen(result, 256) != 0 ||
      getsockname(result, reinterpret_cast<sockaddr*>(&address), &length) != 0)
    return CloseFailed(result);
  port = ntohs(address.sin_port);
  return result;
}
SocketHandle Accept(SocketHandle listener) {
#ifdef _WIN32
  SocketHandle result = accept(listener, nullptr, nullptr);
#else
  SocketHandle result =
      accept4(listener, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
#endif
  if (result == kInvalidSocket) return result;
#ifdef _WIN32
  if (!SetNonBlocking(result) || !SetCloseOnExec(result))
    return CloseFailed(result);
#endif
  return result;
}
bool ConnectLoopback(SocketHandle socket, std::uint16_t port) {
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(port);
  return connect(socket, reinterpret_cast<const sockaddr*>(&address),
                 sizeof(address)) == 0;
}
bool SetReceiveTimeout(SocketHandle socket, int milliseconds) {
#ifdef _WIN32
  return SetIntOption(socket, SO_RCVTIMEO, milliseconds);
#else
  timeval timeout{milliseconds / 1000, (milliseconds % 1000) * 1000};
  return setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                    sizeof(timeout)) == 0;
#endif
}
bool SetReceiveBuffer(SocketHandle socket, int bytes) {
  return SetIntOption(socket, SO_RCVBUF, bytes);
}
bool ShutdownWrite(SocketHandle socket) {
#ifdef _WIN32
  return shutdown(socket, SD_SEND) == 0;
#else
  return shutdown(socket, SHUT_WR) == 0;
#endif
}
std::ptrdiff_t Receive(SocketHandle socket, char* data, std::size_t size) {
  return recv(socket, data,
              static_cast<int>(std::min(size, std::size_t{INT_MAX})), 0);
}
std::ptrdiff_t Send(SocketHandle socket, const char* data, std::size_t size) {
#ifdef _WIN32
  constexpr int flags = 0;
#else
  constexpr int flags = MSG_NOSIGNAL;
#endif
  return send(socket, data,
              static_cast<int>(std::min(size, std::size_t{INT_MAX})), flags);
}

struct Wakeup::Impl {
  SocketHandle reader = kInvalidSocket;
  SocketHandle writer = kInvalidSocket;
  ~Impl() {
    CloseSocket(reader);
    if (writer != reader) CloseSocket(writer);
  }
};
Wakeup::Wakeup(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Wakeup::~Wakeup() = default;
std::unique_ptr<Wakeup> Wakeup::Create() {
  auto impl = std::make_unique<Impl>();
#ifdef _WIN32
  // TCP loopback works with WSAPoll; unlike UDP, notifications cannot be lost.
  std::uint16_t port = 0;
  SocketHandle listener = ListenTcp(0, port, true);
  if (listener == kInvalidSocket) return nullptr;
  impl->writer = CreateTcpSocket(false);
  int no_delay = 1;
  if (impl->writer != kInvalidSocket &&
      setsockopt(impl->writer, IPPROTO_TCP, TCP_NODELAY,
                 reinterpret_cast<const char*>(&no_delay),
                 sizeof(no_delay)) != 0) {
    CloseFailed(listener);
    return nullptr;
  }
  if (impl->writer == kInvalidSocket || !ConnectLoopback(impl->writer, port)) {
    CloseFailed(listener);
    return nullptr;
  }
  // Wait for accept readiness rather than relying on connection timing.
  WSAPOLLFD pending{listener, POLLRDNORM, 0};
  if (WSAPoll(&pending, 1, 5000) > 0) impl->reader = Accept(listener);
  CloseSocket(listener);
  if (impl->reader == kInvalidSocket || !SetNonBlocking(impl->writer))
    return nullptr;
#else
  impl->reader = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  impl->writer = impl->reader;
  if (impl->reader == kInvalidSocket) return nullptr;
#endif
  return std::unique_ptr<Wakeup>(new Wakeup(std::move(impl)));
}
SocketHandle Wakeup::handle() const { return impl_->reader; }
bool Wakeup::Notify() {
  for (;;) {
#ifdef _WIN32
    auto result = Send(impl_->writer, "w", 1);
#else
    std::uint64_t one = 1;
    auto result = write(impl_->writer, &one, sizeof(one));
#endif
    if (result > 0) return true;
    int error = LastError();
    if (result < 0 && Interrupted(error)) continue;
    return result < 0 && WouldBlock(error);
  }
}
bool Wakeup::Drain() {
  for (;;) {
#ifdef _WIN32
    char buffer[1024];
    auto result = Receive(impl_->reader, buffer, sizeof(buffer));
#else
    std::uint64_t value = 0;
    auto result = read(impl_->reader, &value, sizeof(value));
#endif
    if (result > 0) continue;
    int error = LastError();
    if (result < 0 && Interrupted(error)) continue;
    return result < 0 && WouldBlock(error);
  }
}

struct Poller::Impl {
#ifdef _WIN32
  std::vector<WSAPOLLFD> sockets;
  std::vector<std::uint64_t> tokens;
#else
  int epoll_fd = -1;
  ~Impl() {
    if (epoll_fd >= 0) close(epoll_fd);
  }
#endif
};
Poller::Poller(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Poller::~Poller() = default;
std::unique_ptr<Poller> Poller::Create() {
  if (!InitializeSockets()) return nullptr;
  auto impl = std::make_unique<Impl>();
#ifndef _WIN32
  impl->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
  if (impl->epoll_fd < 0) return nullptr;
#endif
  return std::unique_ptr<Poller>(new Poller(std::move(impl)));
}
bool Poller::Add(SocketHandle socket, std::uint64_t token, bool read,
                 bool write) {
#ifdef _WIN32
  for (const auto& entry : impl_->sockets)
    if (entry.fd == socket) return false;
  impl_->sockets.push_back(
      {socket,
       static_cast<short>((read ? POLLRDNORM : 0) | (write ? POLLWRNORM : 0)),
       0});
  impl_->tokens.push_back(token);
  return true;
#else
  epoll_event event{};
  event.events =
      EPOLLET | (read ? EPOLLIN | EPOLLRDHUP : 0U) | (write ? EPOLLOUT : 0U);
  event.data.u64 = token;
  return epoll_ctl(impl_->epoll_fd, EPOLL_CTL_ADD, socket, &event) == 0;
#endif
}
bool Poller::Modify(SocketHandle socket, std::uint64_t token, bool read,
                    bool write) {
#ifdef _WIN32
  for (std::size_t i = 0; i < impl_->sockets.size(); ++i) {
    if (impl_->sockets[i].fd != socket) continue;
    impl_->sockets[i].events =
        static_cast<short>((read ? POLLRDNORM : 0) | (write ? POLLWRNORM : 0));
    impl_->tokens[i] = token;
    return true;
  }
  return false;
#else
  epoll_event event{};
  event.events =
      EPOLLET | (read ? EPOLLIN | EPOLLRDHUP : 0U) | (write ? EPOLLOUT : 0U);
  event.data.u64 = token;
  return epoll_ctl(impl_->epoll_fd, EPOLL_CTL_MOD, socket, &event) == 0;
#endif
}
void Poller::Remove(SocketHandle socket) {
#ifdef _WIN32
  for (std::size_t i = 0; i < impl_->sockets.size(); ++i) {
    if (impl_->sockets[i].fd != socket) continue;
    impl_->sockets.erase(impl_->sockets.begin() + i);
    impl_->tokens.erase(impl_->tokens.begin() + i);
    return;
  }
#else
  epoll_ctl(impl_->epoll_fd, EPOLL_CTL_DEL, socket, nullptr);
#endif
}
int Poller::Wait(std::span<ReadyEvent> events, int timeout_ms) {
  if (events.empty()) return 0;
#ifdef _WIN32
  int result = WSAPoll(impl_->sockets.data(),
                       static_cast<ULONG>(impl_->sockets.size()), timeout_ms);
  if (result <= 0) return result;
  std::size_t count = 0;
  // Rotate the scan start so a permanently readable socket cannot starve
  // others.
  for (std::size_t i = 0; i < impl_->sockets.size() && count < events.size();
       ++i) {
    auto flags = impl_->sockets[i].revents;
    if (flags == 0) continue;
    std::uint32_t ready = 0;
    if ((flags & (POLLRDNORM | POLLHUP)) &&
        (impl_->sockets[i].events & POLLRDNORM))
      ready |= kReadable;
    if (flags & POLLWRNORM) ready |= kWritable;
    if (flags & (POLLERR | POLLHUP | POLLNVAL)) ready |= kError;
    events[count++] = {impl_->tokens[i], ready};
  }
  if (!impl_->sockets.empty()) {
    auto offset = std::min(events.size(), impl_->sockets.size());
    std::rotate(impl_->sockets.begin(), impl_->sockets.begin() + offset,
                impl_->sockets.end());
    std::rotate(impl_->tokens.begin(), impl_->tokens.begin() + offset,
                impl_->tokens.end());
  }
  return static_cast<int>(count);
#else
  std::array<epoll_event, 64> native{};
  int count = epoll_wait(
      impl_->epoll_fd, native.data(),
      static_cast<int>(std::min(events.size(), native.size())), timeout_ms);
  for (int i = 0; i < count; ++i) {
    std::uint32_t ready = 0;
    if (native[i].events & (EPOLLIN | EPOLLRDHUP)) ready |= kReadable;
    if (native[i].events & EPOLLOUT) ready |= kWritable;
    if (native[i].events & (EPOLLERR | EPOLLHUP)) ready |= kError;
    events[i] = {native[i].data.u64, ready};
  }
  return count;
#endif
}
}  // namespace netlib::platform
