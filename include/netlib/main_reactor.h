#ifndef NETLIB_INCLUDE_NETLIB_MAIN_REACTOR_H_
#define NETLIB_INCLUDE_NETLIB_MAIN_REACTOR_H_

#include <cstdint>
#include <functional>
#include <memory>

#include "netlib/channel.h"
#include "netlib/event_loop.h"
#include "netlib/unique_fd.h"

namespace netlib {

// Accepts IPv4 connections and transfers each descriptor to an AcceptHandler.
class MainReactor {
 public:
  using AcceptHandler = std::function<void(UniqueFd)>;

  // A requested_port of zero asks the kernel to choose an available port.
  static std::unique_ptr<MainReactor> Create(uint16_t requested_port,
                                             AcceptHandler accept_handler);

  MainReactor(const MainReactor&) = delete;
  MainReactor& operator=(const MainReactor&) = delete;
  MainReactor(MainReactor&&) = delete;
  MainReactor& operator=(MainReactor&&) = delete;
  ~MainReactor();

  void Run();
  void Stop();
  uint16_t port() const;

 private:
  MainReactor(std::unique_ptr<EventLoop> event_loop, uint16_t port,
              UniqueFd listen_fd, AcceptHandler accept_handler);

  void HandleReadable();

  std::unique_ptr<EventLoop> event_loop_;
  AcceptHandler accept_handler_;
  uint16_t port_;
  UniqueFd listen_fd_;
  std::unique_ptr<Channel> listener_channel_;
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_MAIN_REACTOR_H_
