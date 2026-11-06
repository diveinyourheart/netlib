#include "netlib/connection_handle.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

namespace netlib {

class ConnectionHandle::State {
 public:
  State(std::uint64_t id, std::shared_ptr<const std::atomic<bool>> alive,
        Responder responder)
      : id_(id), alive_(std::move(alive)), responder_(std::move(responder)) {}

  std::uint64_t id() const { return id_; }

  bool valid() const { return alive_->load(std::memory_order_acquire); }

  bool Respond(std::vector<char> payload, bool close_after_write) const {
    return valid() && responder_(id_, std::move(payload), close_after_write);
  }

 private:
  const std::uint64_t id_;
  const std::shared_ptr<const std::atomic<bool>> alive_;
  const Responder responder_;
};

ConnectionHandle::ConnectionHandle(
    std::uint64_t id, std::shared_ptr<const std::atomic<bool>> alive,
    Responder responder)
    : state_(std::make_shared<State>(id, std::move(alive),
                                     std::move(responder))) {}

std::uint64_t ConnectionHandle::id() const {
  return state_ == nullptr ? 0 : state_->id();
}

bool ConnectionHandle::valid() const {
  return state_ != nullptr && state_->valid();
}

bool ConnectionHandle::Respond(std::vector<char> payload,
                               bool close_after_write) const {
  return state_ != nullptr &&
         state_->Respond(std::move(payload), close_after_write);
}

}  // namespace netlib
