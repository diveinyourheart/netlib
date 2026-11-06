#include "netlib/thread_pool.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "blockingconcurrentqueue.h"

namespace netlib {
namespace {

enum class WorkItemKind : std::uint8_t {
  kMessage,
  kStop,
};

struct WorkItem {
  WorkItemKind kind = WorkItemKind::kMessage;
  FrameMessage message;
};

}  // namespace

class ThreadPool::Impl {
 public:
  Impl(std::size_t worker_count, std::size_t max_pending_messages,
       Handler handler);
  ~Impl();

  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;
  Impl(Impl&&) = delete;
  Impl& operator=(Impl&&) = delete;

  bool Submit(FrameMessage message);
  void Stop();
  std::size_t queued_count() const;

 private:
  bool ReserveQueueSlot();
  void WorkerLoop(moodycamel::ConsumerToken& consumer_token);

  const std::size_t max_pending_messages_;
  Handler handler_;
  moodycamel::BlockingConcurrentQueue<WorkItem> queue_;
  std::atomic<std::size_t> queued_count_{0};
  std::atomic<bool> stopping_{false};
  std::vector<std::unique_ptr<moodycamel::ConsumerToken>> consumer_tokens_;
  std::vector<std::thread> workers_;
};

ThreadPool::Impl::Impl(std::size_t worker_count,
                       std::size_t max_pending_messages, Handler handler)
    : max_pending_messages_(max_pending_messages),
      handler_(std::move(handler)),
      queue_(max_pending_messages) {
  consumer_tokens_.reserve(worker_count);
  workers_.reserve(worker_count);
  for (std::size_t i = 0; i < worker_count; ++i) {
    std::unique_ptr<moodycamel::ConsumerToken> consumer_token =
        std::make_unique<moodycamel::ConsumerToken>(queue_);
    moodycamel::ConsumerToken* token_pointer = consumer_token.get();
    consumer_tokens_.push_back(std::move(consumer_token));
    workers_.emplace_back(
        [this, token_pointer] { WorkerLoop(*token_pointer); });
  }
}

ThreadPool::Impl::~Impl() { Stop(); }

bool ThreadPool::Impl::Submit(FrameMessage message) {
  if (!ReserveQueueSlot()) {
    return false;
  }
  if (stopping_.load(std::memory_order_acquire)) {
    queued_count_.fetch_sub(1, std::memory_order_release);
    return false;
  }

  WorkItem item{
      .kind = WorkItemKind::kMessage,
      .message = std::move(message),
  };
  if (!queue_.enqueue(std::move(item))) {
    queued_count_.fetch_sub(1, std::memory_order_release);
    return false;
  }
  return true;
}

void ThreadPool::Impl::Stop() {
  if (stopping_.exchange(true, std::memory_order_acq_rel)) {
    return;
  }

  for (std::size_t i = 0; i < workers_.size(); ++i) {
    queue_.enqueue(
        WorkItem{.kind = WorkItemKind::kStop, .message = FrameMessage{}});
  }
  for (std::thread& worker : workers_) {
    worker.join();
  }
}

std::size_t ThreadPool::Impl::queued_count() const {
  return queued_count_.load(std::memory_order_acquire);
}

bool ThreadPool::Impl::ReserveQueueSlot() {
  std::size_t current = queued_count_.load(std::memory_order_relaxed);
  while (current < max_pending_messages_) {
    if (queued_count_.compare_exchange_weak(current, current + 1,
                                            std::memory_order_acq_rel,
                                            std::memory_order_relaxed)) {
      return true;
    }
  }
  return false;
}

void ThreadPool::Impl::WorkerLoop(moodycamel::ConsumerToken& consumer_token) {
  while (true) {
    WorkItem item;
    queue_.wait_dequeue(consumer_token, item);
    if (item.kind == WorkItemKind::kStop) {
      return;
    }
    queued_count_.fetch_sub(1, std::memory_order_acq_rel);
    handler_(std::move(item.message));
  }
}

std::unique_ptr<ThreadPool> ThreadPool::Create(std::size_t worker_count,
                                               std::size_t max_pending_messages,
                                               Handler handler) {
  if (worker_count == 0 || max_pending_messages == 0 || !handler) {
    return nullptr;
  }

  std::unique_ptr<Impl> impl = std::make_unique<Impl>(
      worker_count, max_pending_messages, std::move(handler));
  return std::unique_ptr<ThreadPool>(new ThreadPool(std::move(impl)));
}

ThreadPool::ThreadPool(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

ThreadPool::~ThreadPool() = default;

bool ThreadPool::Submit(FrameMessage message) {
  return impl_->Submit(std::move(message));
}

void ThreadPool::Stop() { impl_->Stop(); }

std::size_t ThreadPool::queued_count() const { return impl_->queued_count(); }

}  // namespace netlib
