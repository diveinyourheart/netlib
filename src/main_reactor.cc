#include "netlib/main_reactor.h"

#include <arpa/inet.h>
#include <sys/socket.h>

#include <cerrno>
#include <cstdint>
#include <memory>
#include <utility>

#include "netlib/channel.h"
#include "netlib/event_loop.h"
#include "netlib/unique_fd.h"

namespace netlib {
namespace {

constexpr int kListenBacklog = 256;

struct ListenSocket {
  UniqueFd socket;
  uint16_t port = 0;
};

ListenSocket CreateListenSocket(uint16_t requested_port) {
  ListenSocket result;
  result.socket.Reset(
      socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
  if (!result.socket) {
    return result;
  }

  int reuse_address = 1;
  if (setsockopt(result.socket.get(), SOL_SOCKET, SO_REUSEADDR, &reuse_address,
                 sizeof(reuse_address)) != 0) {
    result.socket.Reset();
    return result;
  }

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_ANY);
  address.sin_port = htons(requested_port);
  if (bind(result.socket.get(), reinterpret_cast<const sockaddr*>(&address),
           sizeof(address)) != 0 ||
      listen(result.socket.get(), kListenBacklog) != 0) {
    result.socket.Reset();
    return result;
  }

  socklen_t address_length = sizeof(address);
  if (getsockname(result.socket.get(), reinterpret_cast<sockaddr*>(&address),
                  &address_length) != 0) {
    result.socket.Reset();
    return result;
  }
  result.port = ntohs(address.sin_port);
  return result;
}

}  // namespace

std::unique_ptr<MainReactor> MainReactor::Create(uint16_t requested_port,
                                                 AcceptHandler accept_handler) {
  std::unique_ptr<EventLoop> event_loop = EventLoop::Create();
  if (!event_loop) {
    return nullptr;
  }

  ListenSocket listen_socket = CreateListenSocket(requested_port);
  if (!listen_socket.socket) {
    return nullptr;
  }

  auto reactor = std::unique_ptr<MainReactor>(new MainReactor(
      std::move(event_loop), listen_socket.port,
      std::move(listen_socket.socket), std::move(accept_handler)));
  MainReactor* reactor_pointer = reactor.get();
  ChannelCallbacks callbacks{
      [reactor_pointer] { reactor_pointer->HandleReadable(); },
      {},
      [reactor_pointer] { reactor_pointer->event_loop_->Stop(); },
  };
  reactor->listener_channel_ =
      Channel::Create(*reactor->event_loop_, reactor->listen_fd_.get(),
                      ChannelInterest{.read = true}, std::move(callbacks));
  if (!reactor->listener_channel_) {
    return nullptr;
  }
  return reactor;
}

MainReactor::MainReactor(std::unique_ptr<EventLoop> event_loop, uint16_t port,
                         UniqueFd listen_fd, AcceptHandler accept_handler)
    : event_loop_(std::move(event_loop)),
      accept_handler_(std::move(accept_handler)),
      port_(port),
      listen_fd_(std::move(listen_fd)) {}

MainReactor::~MainReactor() = default;

void MainReactor::Run() { event_loop_->Run(); }

void MainReactor::Stop() { event_loop_->Stop(); }

uint16_t MainReactor::port() const { return port_; }

void MainReactor::HandleReadable() {
  while (true) {
    int connection_fd = accept4(listen_fd_.get(), nullptr, nullptr,
                                SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (connection_fd >= 0) {
      UniqueFd connection(connection_fd);
      if (accept_handler_) {
        accept_handler_(std::move(connection));
      }
      continue;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return;
    }
    if (errno == ECONNABORTED || errno == EPROTO) {
      continue;
    }
    event_loop_->Stop();
    return;
  }
}

}  // namespace netlib
