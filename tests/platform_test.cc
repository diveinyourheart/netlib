#include "netlib/platform.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <utility>
#include <vector>

#include "netlib/channel.h"
#include "netlib/event_loop.h"
#include "netlib/unique_fd.h"

namespace {
bool TestSocketOwnership() {
  netlib::UniqueFd first(netlib::platform::CreateTcpSocket());
  if (!first) return false;
  auto native = first.get();
  netlib::UniqueFd second(std::move(first));
  if (first || second.get() != native) return false;
  first.Reset(second.Release());
  if (second || first.get() != native) return false;
  first.Reset();
  char byte;
  return netlib::platform::Receive(native, &byte, 1) < 0;
}

bool TestWakeup() {
  auto wakeup = netlib::platform::Wakeup::Create();
  auto poller = netlib::platform::Poller::Create();
  if (!wakeup || !poller || !poller->Add(wakeup->handle(), 17, true, false))
    return false;
  std::atomic<int> produced{0};
  std::atomic<bool> failed{false};
  std::vector<std::thread> producers;
  for (int i = 0; i < 4; ++i) {
    producers.emplace_back([&] {
      for (int n = 0; n < 3000; ++n) {
        produced.fetch_add(1);
        if (!wakeup->Notify()) failed = true;
      }
    });
  }
  std::array<netlib::platform::ReadyEvent, 4> events{};
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (produced < 12000 && std::chrono::steady_clock::now() < deadline) {
    int count = poller->Wait(events, 100);
    if (count < 0 ||
        (count > 0 && (events[0].token != 17 || !wakeup->Drain()))) {
      failed = true;
      break;
    }
  }
  for (auto& producer : producers) producer.join();
  if (produced != 12000 || failed || !wakeup->Drain()) return false;
  // On Windows, Notify writes TCP bytes. Joined writers can still have bytes
  // in flight, so Drain only guarantees that currently received bytes are
  // consumed. Wait for quiescence instead of assuming an immediate empty poll.
  const std::chrono::steady_clock::time_point drain_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  for (;;) {
    int remaining = poller->Wait(events, 20);
    if (remaining < 0) return false;
    if (remaining == 0) break;
    if (std::chrono::steady_clock::now() >= drain_deadline ||
        events[0].token != 17 || !wakeup->Drain()) {
      return false;
    }
  }
  // Interest changes carry the new registration token, including on epoll.
  if (!poller->Modify(wakeup->handle(), 23, true, false) || !wakeup->Notify())
    return false;
  int count = poller->Wait(events, 1000);
  poller->Remove(wakeup->handle());
  return count == 1 && events[0].token == 23 && wakeup->Drain();
}

bool TestDispatch(bool remove_on_read) {
  auto wakeup = netlib::platform::Wakeup::Create();
  auto loop = netlib::EventLoop::Create();
  if (!wakeup || !loop) return false;
  int reads = 0;
  int writes = 0;
  std::unique_ptr<netlib::Channel> channel;
  channel = netlib::Channel::Create(*loop, wakeup->handle(), {true, true},
                                    {
                                        [&] {
                                          ++reads;
                                          wakeup->Drain();
                                          if (remove_on_read) {
                                            channel.reset();
                                            loop->Stop();
                                          }
                                        },
                                        [&] {
                                          ++writes;
                                          loop->Stop();
                                        },
                                        [&] { loop->Stop(); },
                                    });
  if (!channel || !wakeup->Notify()) return false;
  // TCP send completion does not promise immediate receive readiness. Ensure
  // both flags are ready before testing dispatch of one combined ready event.
  auto probe = netlib::platform::Poller::Create();
  std::array<netlib::platform::ReadyEvent, 1> ready{};
  if (!probe || !probe->Add(wakeup->handle(), 1, true, false) ||
      probe->Wait(ready, 1000) != 1)
    return false;
  probe->Remove(wakeup->handle());
  loop->Run();  // CTest timeout catches lost writable readiness / stop wakeups.
  return reads == 1 && writes == (remove_on_read ? 0 : 1);
}

bool TestIdleStop() {
  auto loop = netlib::EventLoop::Create();
  if (!loop) return false;
  std::thread runner([&] { loop->Run(); });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  loop->Stop();
  loop->Stop();
  runner.join();
  return true;
}

bool TestReadyBatches() {
  auto poller = netlib::platform::Poller::Create();
  if (!poller) return false;
  std::vector<std::unique_ptr<netlib::platform::Wakeup>> wakeups;
  std::array<bool, 70> seen{};
  for (std::size_t i = 0; i < seen.size(); ++i) {
    auto wakeup = netlib::platform::Wakeup::Create();
    if (!wakeup || !poller->Add(wakeup->handle(), i, true, false) ||
        !wakeup->Notify())
      return false;
    wakeups.push_back(std::move(wakeup));
  }
  std::size_t received = 0;
  std::array<netlib::platform::ReadyEvent, 16> events{};
  for (int batch = 0; batch < 70 && received < seen.size(); ++batch) {
    int count = poller->Wait(events, 1000);
    if (count <= 0) return false;
    for (int i = 0; i < count; ++i) {
      auto token = events[i].token;
      if (token >= seen.size() || seen[token] || !wakeups[token]->Drain())
        return false;
      seen[token] = true;
      ++received;
    }
  }
  for (const auto& wakeup : wakeups) poller->Remove(wakeup->handle());
  return received == seen.size();
}
}  // namespace

int main() {
  struct TestCase {
    const char* name;
    bool (*run)();
  };
  const TestCase tests[]{
      {"socket ownership", TestSocketOwnership},
      {"concurrent wakeup", TestWakeup},
      {"read/write dispatch", [] { return TestDispatch(false); }},
      {"removal during dispatch", [] { return TestDispatch(true); }},
      {"idle stop", TestIdleStop},
      {"ready batches", TestReadyBatches},
  };
  for (const TestCase& test : tests) {
    if (!test.run()) {
      std::fprintf(stderr, "platform regression failed (%s): %s\n", test.name,
                   netlib::platform::ErrorMessage().c_str());
      return 1;
    }
  }
  return 0;
}
