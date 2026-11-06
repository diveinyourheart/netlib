#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "netlib/main_reactor.h"
#include "netlib/sub_reactor.h"
#include "netlib/unique_fd.h"

namespace {

constexpr auto kWaitTimeout = std::chrono::seconds(2);

struct Observation {
  std::mutex mutex;
  std::condition_variable condition;
  std::string data;
  bool connected = false;
  bool closed = false;
};

void ReportFailure(const char* message) {
  std::fprintf(stderr, "%s\n", message);
}

bool ConnectTo(uint16_t port, netlib::UniqueFd* connection) {
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

  connection->Reset(socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
  if (!*connection) {
    return false;
  }
  return connect(connection->get(), reinterpret_cast<const sockaddr*>(&address),
                 sizeof(address)) == 0;
}

bool SendAll(int fd, std::string_view data) {
  std::size_t sent = 0;
  while (sent < data.size()) {
    ssize_t result = send(fd, data.data() + sent, data.size() - sent, 0);
    if (result > 0) {
      sent += static_cast<std::size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    return false;
  }
  return true;
}

int RunPendingCapacityTest() {
  std::unique_ptr<netlib::SubReactor> sub_reactor =
      netlib::SubReactor::Create(1, {});
  if (!sub_reactor) {
    ReportFailure("failed to create bounded SubReactor");
    return 1;
  }

  netlib::UniqueFd first(dup(STDIN_FILENO));
  netlib::UniqueFd second(dup(STDIN_FILENO));
  int second_fd = second.get();
  if (!first || !second || !sub_reactor->AdoptConnection(std::move(first)) ||
      sub_reactor->AdoptConnection(std::move(second))) {
    ReportFailure("pending queue capacity was not enforced");
    return 1;
  }
  if (fcntl(second_fd, F_GETFD, 0) != -1 || errno != EBADF) {
    ReportFailure("rejected connection was not closed");
    return 1;
  }
  return 0;
}

int RunReactorTest() {
  Observation observation;
  netlib::SubReactor::ConnectionCallbacks callbacks;
  callbacks.on_connected = [&observation](int) {
    std::lock_guard<std::mutex> lock(observation.mutex);
    observation.connected = true;
    observation.condition.notify_all();
  };
  callbacks.on_data = [&observation](int, std::string_view data) {
    std::lock_guard<std::mutex> lock(observation.mutex);
    observation.data.append(data);
    observation.condition.notify_all();
  };
  callbacks.on_closed = [&observation](int) {
    std::lock_guard<std::mutex> lock(observation.mutex);
    observation.closed = true;
    observation.condition.notify_all();
  };

  std::unique_ptr<netlib::SubReactor> sub_reactor =
      netlib::SubReactor::Create(16, std::move(callbacks));
  if (!sub_reactor) {
    ReportFailure("failed to create SubReactor");
    return 1;
  }

  netlib::MainReactor::AcceptHandler accept_handler =
      [&sub_reactor](netlib::UniqueFd connection) {
        sub_reactor->AdoptConnection(std::move(connection));
      };
  std::unique_ptr<netlib::MainReactor> main_reactor =
      netlib::MainReactor::Create(0, std::move(accept_handler));
  if (!main_reactor) {
    ReportFailure("failed to create MainReactor");
    return 1;
  }

  std::thread sub_thread([&sub_reactor] { sub_reactor->Run(); });
  std::thread main_thread([&main_reactor] { main_reactor->Run(); });

  bool success = true;
  netlib::UniqueFd connection;
  if (!ConnectTo(main_reactor->port(), &connection)) {
    ReportFailure("failed to connect");
    success = false;
  }

  {
    std::unique_lock<std::mutex> lock(observation.mutex);
    if (success && !observation.condition.wait_for(
                       lock, kWaitTimeout,
                       [&observation] { return observation.connected; })) {
      ReportFailure("connection was not handed to SubReactor");
      success = false;
    }
  }

  const std::string payload(128 * 1024, 'x');
  if (success && !SendAll(connection.get(), payload)) {
    ReportFailure("failed to send test data");
    success = false;
  }

  {
    std::unique_lock<std::mutex> lock(observation.mutex);
    if (success && !observation.condition.wait_for(
                       lock, kWaitTimeout, [&observation, &payload] {
                         return observation.data.size() == payload.size() &&
                                observation.data == payload;
                       })) {
      ReportFailure("SubReactor did not read test data");
      success = false;
    }
  }

  if (connection) {
    shutdown(connection.get(), SHUT_WR);
    connection.Reset();
  }
  {
    std::unique_lock<std::mutex> lock(observation.mutex);
    if (success && !observation.condition.wait_for(
                       lock, kWaitTimeout,
                       [&observation] { return observation.closed; })) {
      ReportFailure("SubReactor did not observe close");
      success = false;
    }
  }

  main_reactor->Stop();
  sub_reactor->Stop();
  main_thread.join();
  sub_thread.join();
  return success ? 0 : 1;
}

}  // namespace

int main() {
  int capacity_result = RunPendingCapacityTest();
  if (capacity_result != 0) {
    return capacity_result;
  }
  return RunReactorTest();
}
