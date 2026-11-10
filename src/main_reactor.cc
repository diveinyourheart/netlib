#include "netlib/main_reactor.h"

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

struct ListenSocket {
  UniqueFd socket;
  uint16_t port = 0;
};

ListenSocket CreateListenSocket(uint16_t requested_port) {
  ListenSocket result;
  result.socket.Reset(platform::ListenTcp(requested_port, result.port));
  if (!result.socket) {
    NETLIB_LOG_ERROR("listen failed: {}", platform::ErrorMessage());
  }
  return result;
}

}  // namespace

std::unique_ptr<MainReactor> MainReactor::Create(uint16_t requested_port,
                                                 SubReactorList sub_reactors) {
  if (sub_reactors.empty()) {
    NETLIB_LOG_ERROR("main reactor requires a SubReactor");
    return nullptr;
  }
  for (SubReactor* sub_reactor : sub_reactors) {
    if (sub_reactor == nullptr) {
      NETLIB_LOG_ERROR("main reactor received a null SubReactor");
      return nullptr;
    }
  }

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
      std::move(listen_socket.socket), std::move(sub_reactors)));
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
  NETLIB_LOG_INFO("main reactor listening on port={} with {} SubReactors",
                  reactor->port_, reactor->sub_reactors_.size());
  return reactor;
}

MainReactor::MainReactor(std::unique_ptr<EventLoop> event_loop, uint16_t port,
                         UniqueFd listen_fd, SubReactorList sub_reactors)
    : event_loop_(std::move(event_loop)),
      sub_reactors_(std::move(sub_reactors)),
      port_(port),
      listen_fd_(std::move(listen_fd)) {}

MainReactor::~MainReactor() = default;

void MainReactor::Run() {
  event_loop_->Run();
  stopping_.store(true);
  listener_channel_.reset();
  listen_fd_.Reset();
}

void MainReactor::Stop() {
  stopping_.store(true);
  NETLIB_LOG_INFO("main reactor stopping");
  event_loop_->Stop();
}

uint16_t MainReactor::port() const { return port_; }

bool MainReactor::Drain(std::chrono::milliseconds timeout) {
  if (timeout <= std::chrono::milliseconds::zero() ||
      timeout > std::chrono::hours(24))
    return false;
  Stop();
  bool accepted = true;
  for (SubReactor* reactor : sub_reactors_) {
    if (!reactor->Drain(timeout)) accepted = false;
  }
  return accepted;
}

void MainReactor::DispatchConnection(UniqueFd& connection) {
  SocketHandle fd = connection.get();
  for (std::size_t attempt = 0; attempt < sub_reactors_.size(); ++attempt) {
    std::size_t index = next_sub_reactor_;
    next_sub_reactor_ = (next_sub_reactor_ + 1) % sub_reactors_.size();
    if (sub_reactors_[index]->AdoptConnection(connection)) {
      NETLIB_LOG_DEBUG("dispatched fd={} to SubReactor index={}", fd, index);
      return;
    }
  }
  NETLIB_LOG_WARN("all SubReactor queues are full; closing fd={}", fd);
}

void MainReactor::HandleReadable() {
  while (!stopping_.load()) {
    SocketHandle connection_fd = platform::Accept(listen_fd_.get());
    if (connection_fd != kInvalidSocket) {
      UniqueFd connection(connection_fd);
      NETLIB_LOG_DEBUG("accepted fd={}", connection_fd);
      DispatchConnection(connection);
      continue;
    }
    if (platform::Interrupted(platform::LastError())) {
      continue;
    }
    if (platform::WouldBlock(platform::LastError())) {
      return;
    }
    if (platform::TransientAcceptError(platform::LastError())) {
      NETLIB_LOG_WARN("transient accept error: {}", platform::ErrorMessage());
      continue;
    }
    NETLIB_LOG_ERROR("fatal accept error: {}", platform::ErrorMessage());
    event_loop_->Stop();
    return;
  }
}

}  // namespace netlib
