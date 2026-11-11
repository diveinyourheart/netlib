#include <cstdio>
#include <functional>
#include <memory>

#include "netlib/channel.h"
#include "netlib/event_loop.h"
#include "netlib/platform.h"

int main() {
  std::unique_ptr<netlib::EventLoop> loop = netlib::EventLoop::Create();
  std::unique_ptr<netlib::platform::Wakeup> wake =
      netlib::platform::Wakeup::Create();
  if (!loop || !wake) return 1;
  int hot_calls = 0;
  bool quiet_serviced = false;
  std::uint64_t last_turn = 0;
  bool recursive_turn = false;
  std::unique_ptr<netlib::Channel> channel =
      netlib::Channel::Create(*loop, wake->handle(), {.read = true},
                              {[&] {
                                 quiet_serviced = wake->Drain();
                                 loop->Stop();
                               },
                               {},
                               [&] { loop->Stop(); }});
  if (!channel) return 1;
  std::function<void()> hot;
  hot = [&] {
    recursive_turn = recursive_turn || last_turn == loop->turn();
    last_turn = loop->turn();
    ++hot_calls;
    if (hot_calls == 2 && !wake->Notify()) loop->Stop();
    if (hot_calls < 1000)
      loop->Defer(hot);
    else
      loop->Stop();
  };
  loop->Defer(hot);
  loop->Run();
  // A perpetually runnable continuation must still poll real readiness each
  // turn. Newly deferred work cannot recursively consume the current turn.
  if (!quiet_serviced || hot_calls >= 1000 || recursive_turn) {
    std::fprintf(stderr, "deferred work starved readiness: hot=%d quiet=%d\n",
                 hot_calls, quiet_serviced);
    return 1;
  }
  return 0;
}
