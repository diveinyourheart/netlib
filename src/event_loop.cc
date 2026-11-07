#include "netlib/event_loop.h"

#include <array>
#include <utility>

#include "netlib/channel.h"
#include "netlib/log/logging.h"

namespace netlib {
std::unique_ptr<EventLoop> EventLoop::Create(log::Logger* logger) {
  auto poller = platform::Poller::Create();
  auto wakeup = platform::Wakeup::Create();
  if (!poller || !wakeup) {
    if (logger)
      NETLIB_LOG_ERROR(*logger, "event loop creation failed: {}",
                       platform::ErrorMessage());
    return nullptr;
  }
  auto loop = std::unique_ptr<EventLoop>(
      new EventLoop(std::move(poller), std::move(wakeup), logger));
  auto* pointer = loop.get();
  loop->stop_channel_ = Channel::Create(
      *loop, loop->wakeup_->handle(), ChannelInterest{.read = true},
      ChannelCallbacks{[pointer] { pointer->DrainStopEvent(); },
                       {},
                       [pointer] { pointer->Stop(); }});
  if (!loop->stop_channel_) return nullptr;
  return loop;
}
EventLoop::EventLoop(std::unique_ptr<platform::Poller> poller,
                     std::unique_ptr<platform::Wakeup> wakeup,
                     log::Logger* logger)
    : poller_(std::move(poller)), wakeup_(std::move(wakeup)), logger_(logger) {}
EventLoop::~EventLoop() {
  // Unregister while the channel map and poller are still alive.
  stop_channel_.reset();
}
void EventLoop::Run() {
  if (logger_) NETLIB_LOG_DEBUG(*logger_, "event loop started");
  std::array<platform::ReadyEvent, 64> events{};
  while (!stopping_.load()) {
    int count = poller_->Wait(events);
    if (count < 0) {
      if (platform::Interrupted(platform::LastError())) continue;
      if (logger_)
        NETLIB_LOG_ERROR(*logger_, "event wait failed: {}",
                         platform::ErrorMessage());
      break;
    }
    for (int i = 0; i < count; ++i) Dispatch(events[i].token, events[i].events);
  }
  if (logger_) NETLIB_LOG_DEBUG(*logger_, "event loop stopped");
}
void EventLoop::Stop() {
  if (!stopping_.exchange(true)) WakeForStop();
}
bool EventLoop::AddChannel(Channel& channel) {
  channel.token_ = next_token_++;
  if (!poller_->Add(channel.fd_, channel.token_, channel.interest_.read,
                    channel.interest_.write))
    return false;
  channels_.emplace(channel.token_, &channel);
  return true;
}
void EventLoop::RemoveChannel(Channel& channel) {
  channels_.erase(channel.token_);
  poller_->Remove(channel.fd_);
}
bool EventLoop::ModifyChannel(Channel& channel) {
  return poller_->Modify(channel.fd_, channel.token_, channel.interest_.read,
                         channel.interest_.write);
}
void EventLoop::Dispatch(std::uint64_t token, uint32_t events) {
  // Re-lookup after each callback: it may destroy a Channel, reuse its socket,
  // or change interests. Both read and write must be serviced for epoll ET.
  for (auto flag :
       {platform::kReadable, platform::kWritable, platform::kError}) {
    if (!(events & flag)) continue;
    auto found = channels_.find(token);
    if (found == channels_.end()) return;
    Channel& channel = *found->second;
    std::function<void()> callback;
    if (flag == platform::kReadable && channel.interest_.read)
      callback = channel.callbacks_.on_readable;
    if (flag == platform::kWritable && channel.interest_.write)
      callback = channel.callbacks_.on_writable;
    if (flag == platform::kError) callback = channel.callbacks_.on_error;
    if (callback) callback();
  }
}
void EventLoop::DrainStopEvent() {
  if (!wakeup_->Drain()) Stop();
}
void EventLoop::WakeForStop() {
  if (!wakeup_->Notify() && logger_)
    NETLIB_LOG_ERROR(*logger_, "stop notification failed: {}",
                     platform::ErrorMessage());
}
}  // namespace netlib
