#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>

namespace netlib {
#ifdef _WIN32
using SocketHandle = std::uintptr_t;
inline constexpr SocketHandle kInvalidSocket = ~SocketHandle{0};
#else
using SocketHandle = int;
inline constexpr SocketHandle kInvalidSocket = -1;
#endif

namespace platform {
// Socket errors remain native (WSAGetLastError on Windows, errno on Linux).
int LastError() noexcept;
bool Interrupted(int error) noexcept;
bool WouldBlock(int error) noexcept;
bool TransientAcceptError(int error) noexcept;
std::string ErrorMessage(int error = LastError());
void CloseSocket(SocketHandle socket) noexcept;
bool SetNonBlocking(SocketHandle socket);
bool SetCloseOnExec(SocketHandle socket);
SocketHandle CreateTcpSocket(bool nonblocking = true);
SocketHandle ListenTcp(std::uint16_t requested_port, std::uint16_t& port,
                       bool loopback_only = false);
SocketHandle Accept(SocketHandle listener);
bool ConnectLoopback(SocketHandle socket, std::uint16_t port);
bool SetReceiveTimeout(SocketHandle socket, int milliseconds);
bool SetReceiveBuffer(SocketHandle socket, int bytes);
bool ShutdownWrite(SocketHandle socket);
std::ptrdiff_t Receive(SocketHandle socket, char* data, std::size_t size);
std::ptrdiff_t Send(SocketHandle socket, const char* data, std::size_t size);

// A pollable, coalescing notification. Notify is safe for concurrent producers.
// The object must outlive all producers; Drain is called by one consumer.
class Wakeup {
 public:
  static std::unique_ptr<Wakeup> Create();
  ~Wakeup();
  SocketHandle handle() const;
  bool Notify();
  bool Drain();

 private:
  struct Impl;
  explicit Wakeup(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

inline constexpr std::uint32_t kReadable = 1;
inline constexpr std::uint32_t kWritable = 2;
inline constexpr std::uint32_t kError = 4;
struct ReadyEvent {
  std::uint64_t token;
  std::uint32_t events;
};

// Single loop thread only. Tokens distinguish successive uses of the same fd.
class Poller {
 public:
  static std::unique_ptr<Poller> Create();
  ~Poller();
  bool Add(SocketHandle socket, std::uint64_t token, bool read, bool write);
  bool Modify(SocketHandle socket, std::uint64_t token, bool read, bool write);
  void Remove(SocketHandle socket);
  int Wait(std::span<ReadyEvent> events, int timeout_ms = -1);

 private:
  struct Impl;
  explicit Poller(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};
}  // namespace platform
}  // namespace netlib
