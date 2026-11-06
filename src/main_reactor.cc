#include "netlib/main_reactor.h"

#include <arpa/inet.h>
#include <sys/socket.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>

#include "netlib/channel.h"
#include "netlib/event_loop.h"
#include "netlib/log/logging.h"
#include "netlib/sub_reactor.h"
#include "netlib/unique_fd.h"

namespace netlib {
namespace {

constexpr int kListenBacklog = 256;

struct ListenSocket {
  UniqueFd socket;
  uint16_t port = 0;
};

ListenSocket CreateListenSocket(uint16_t requested_port, log::Logger* logger) {
  ListenSocket result;
  result.socket.Reset(
      socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
  if (!result.socket) {
    if (logger != nullptr) {
      NETLIB_LOG_ERROR(*logger, "listen socket creation failed: {}",
                       std::strerror(errno));
    }
    return result;
  }

  int reuse_address = 1;
  if (setsockopt(result.socket.get(), SOL_SOCKET, SO_REUSEADDR, &reuse_address,
                 sizeof(reuse_address)) != 0) {
    if (logger != nullptr) {
      NETLIB_LOG_ERROR(*logger, "SO_REUSEADDR failed: {}",
                       std::strerror(errno));
    }
    result.socket.Reset();
    return result;
  }

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_ANY);
  address.sin_port = htons(requested_port);
  if (bind(result.socket.get(), reinterpret_cast<const sockaddr*>(&address),
           sizeof(address)) != 0) {
    if (logger != nullptr) {
      NETLIB_LOG_ERROR(*logger, "listen socket bind failed for port={}: {}",
                       requested_port, std::strerror(errno));
    }
    result.socket.Reset();
    return result;
  }
  if (listen(result.socket.get(), kListenBacklog) != 0) {
    if (logger != nullptr) {
      NETLIB_LOG_ERROR(*logger, "listen failed: {}", std::strerror(errno));
    }
    result.socket.Reset();
    return result;
  }

  socklen_t address_length = sizeof(address);
  if (getsockname(result.socket.get(), reinterpret_cast<sockaddr*>(&address),
                  &address_length) != 0) {
    if (logger != nullptr) {
      NETLIB_LOG_ERROR(*logger, "getsockname failed: {}", std::strerror(errno));
    }
    result.socket.Reset();
    return result;
  }
  result.port = ntohs(address.sin_port);
  return result;
}

}  // namespace

std::unique_ptr<MainReactor> MainReactor::Create(uint16_t requested_port,
                                                 SubReactorList sub_reactors,
                                                 log::Logger* logger) {
  if (sub_reactors.empty()) {
    if (logger != nullptr) {
      NETLIB_LOG_ERROR(*logger, "main reactor requires a SubReactor");
    }
    return nullptr;
  }
  for (SubReactor* sub_reactor : sub_reactors) {
    if (sub_reactor == nullptr) {
      if (logger != nullptr) {
        NETLIB_LOG_ERROR(*logger, "main reactor received a null SubReactor");
      }
      return nullptr;
    }
  }

  std::unique_ptr<EventLoop> event_loop = EventLoop::Create(logger);
  if (!event_loop) {
    return nullptr;
  }

  ListenSocket listen_socket = CreateListenSocket(requested_port, logger);
  if (!listen_socket.socket) {
    return nullptr;
  }

  auto reactor = std::unique_ptr<MainReactor>(new MainReactor(
      std::move(event_loop), listen_socket.port,
      std::move(listen_socket.socket), std::move(sub_reactors), logger));
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
  if (logger != nullptr) {
    NETLIB_LOG_INFO(*logger,
                    "main reactor listening on port={} with {} SubReactors",
                    reactor->port_, reactor->sub_reactors_.size());
  }
  return reactor;
}

MainReactor::MainReactor(std::unique_ptr<EventLoop> event_loop, uint16_t port,
                         UniqueFd listen_fd, SubReactorList sub_reactors,
                         log::Logger* logger)
    : event_loop_(std::move(event_loop)),
      sub_reactors_(std::move(sub_reactors)),
      port_(port),
      listen_fd_(std::move(listen_fd)),
      logger_(logger) {}

MainReactor::~MainReactor() = default;

void MainReactor::Run() { event_loop_->Run(); }

void MainReactor::Stop() {
  if (logger_ != nullptr) {
    NETLIB_LOG_INFO(*logger_, "main reactor stopping");
  }
  event_loop_->Stop();
}

uint16_t MainReactor::port() const { return port_; }

void MainReactor::DispatchConnection(UniqueFd& connection) {
  int fd = connection.get();
  for (std::size_t attempt = 0; attempt < sub_reactors_.size(); ++attempt) {
    std::size_t index = next_sub_reactor_;
    next_sub_reactor_ = (next_sub_reactor_ + 1) % sub_reactors_.size();
    if (sub_reactors_[index]->AdoptConnection(connection)) {
      if (logger_ != nullptr) {
        NETLIB_LOG_DEBUG(*logger_, "dispatched fd={} to SubReactor index={}",
                         fd, index);
      }
      return;
    }
  }
  if (logger_ != nullptr) {
    NETLIB_LOG_WARN(*logger_, "all SubReactor queues are full; closing fd={}",
                    fd);
  }
}

void MainReactor::HandleReadable() {
  while (true) {
    int connection_fd = accept4(listen_fd_.get(), nullptr, nullptr,
                                SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (connection_fd >= 0) {
      UniqueFd connection(connection_fd);
      if (logger_ != nullptr) {
        NETLIB_LOG_DEBUG(*logger_, "accepted fd={}", connection_fd);
      }
      DispatchConnection(connection);
      continue;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return;
    }
    if (errno == ECONNABORTED || errno == EPROTO) {
      if (logger_ != nullptr) {
        NETLIB_LOG_WARN(*logger_, "transient accept error: {}",
                        std::strerror(errno));
      }
      continue;
    }
    if (logger_ != nullptr) {
      NETLIB_LOG_ERROR(*logger_, "fatal accept error: {}",
                       std::strerror(errno));
    }
    event_loop_->Stop();
    return;
  }
}

}  // namespace netlib
