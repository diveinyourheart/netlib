#ifndef NETLIB_INCLUDE_NETLIB_MAIN_REACTOR_H_
#define NETLIB_INCLUDE_NETLIB_MAIN_REACTOR_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "netlib/channel.h"
#include "netlib/event_loop.h"
#include "netlib/log/logger_fwd.h"
#include "netlib/sub_reactor.h"
#include "netlib/unique_fd.h"

namespace netlib {

// Accepts IPv4 connections and distributes them across externally owned
// SubReactors. The SubReactors must outlive the MainReactor.
class MainReactor {
 public:
  using SubReactorList = std::vector<SubReactor*>;

  // A requested_port of zero asks the kernel to choose an available port.
  // logger is optional and must outlive the MainReactor when non-null.
  static std::unique_ptr<MainReactor> Create(uint16_t requested_port,
                                             SubReactorList sub_reactors,
                                             log::Logger* logger = nullptr);

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
              UniqueFd listen_fd, SubReactorList sub_reactors,
              log::Logger* logger);

  void DispatchConnection(UniqueFd& connection);
  void HandleReadable();

  std::unique_ptr<EventLoop> event_loop_;
  SubReactorList sub_reactors_;
  uint16_t port_;
  std::size_t next_sub_reactor_ = 0;
  UniqueFd listen_fd_;
  std::unique_ptr<Channel> listener_channel_;
  log::Logger* logger_;
};

}  // namespace netlib

#endif  // NETLIB_INCLUDE_NETLIB_MAIN_REACTOR_H_
