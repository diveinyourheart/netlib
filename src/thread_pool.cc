#include "netlib/thread_pool.h"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "blockingconcurrentqueue.h"
#include "netlib/log/logging.h"

namespace netlib {
namespace {

enum class WorkItemKind : std::uint8_t { kMessage, kStop };

struct WorkItem {
  WorkItemKind kind = WorkItemKind::kMessage;
  FrameMessage message;
};

constexpr std::size_t kAdmissionClosed =
    std::size_t{1} << (std::numeric_limits<std::size_t>::digits - 1);

}  // namespace

class ThreadPool::Impl {
 public:
  Impl(std::size_t worker_count, std::size_t max_pending_messages,
       Handler handler);
  ~Impl();
  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;

  bool Submit(FrameMessage message, moodycamel::ProducerToken* producer);
  void Stop();
  std::size_t queued_count() const;
  std::size_t worker_count() const { return workers_.size(); }
  std::optional<std::size_t> CurrentWorkerIndex() const {
    return current_pool_ == this ? std::optional<std::size_t>(current_index_)
                                 : std::nullopt;
  }
  moodycamel::ProducerToken CreateProducer() {
    return moodycamel::ProducerToken(queue_);
  }

 private:
  bool EnterSubmission();
  void LeaveSubmission();
  bool ReserveQueueSlot();
  void FinishWork();
  void WorkerLoop(moodycamel::ConsumerToken& consumer_token, std::size_t index);

  const std::size_t max_pending_messages_;
  Handler handler_;
  moodycamel::BlockingConcurrentQueue<WorkItem> queue_;
  std::atomic<std::size_t> queued_count_{0};
  // The high bit closes admission; lower bits count submissions in progress.
  // Stop cannot miss a submitter racing between a separate flag and counter.
  std::atomic<std::size_t> admission_{0};
  std::atomic<std::size_t> outstanding_{0};
  std::mutex stop_mutex_;
  bool stopped_ = false;
  std::vector<std::unique_ptr<moodycamel::ConsumerToken>> consumer_tokens_;
  std::vector<std::thread> workers_;
  static thread_local const Impl* current_pool_;
  static thread_local std::size_t current_index_;
};

thread_local const ThreadPool::Impl* ThreadPool::Impl::current_pool_ = nullptr;
thread_local std::size_t ThreadPool::Impl::current_index_ = 0;

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
        [this, token_pointer, i] { WorkerLoop(*token_pointer, i); });
  }
  NETLIB_LOG_INFO("ThreadPool started with {} workers and queue capacity {}",
                  worker_count, max_pending_messages);
}

ThreadPool::Impl::~Impl() { Stop(); }

bool ThreadPool::Impl::EnterSubmission() {
  std::size_t state = admission_.load(std::memory_order_relaxed);
  while (state < kAdmissionClosed - 1) {
    if (admission_.compare_exchange_weak(state, state + 1,
                                         std::memory_order_acq_rel,
                                         std::memory_order_relaxed)) {
      return true;
    }
  }
  return false;
}

void ThreadPool::Impl::LeaveSubmission() {
  const std::size_t previous =
      admission_.fetch_sub(1, std::memory_order_release);
  if (previous == kAdmissionClosed + 1) {
    admission_.notify_all();
  }
}

bool ThreadPool::Impl::Submit(FrameMessage message,
                              moodycamel::ProducerToken* producer) {
  if (!EnterSubmission()) {
    return false;
  }
  if (!ReserveQueueSlot()) {
    LeaveSubmission();
    return false;
  }
  outstanding_.fetch_add(1, std::memory_order_relaxed);
  WorkItem item{.kind = WorkItemKind::kMessage, .message = std::move(message)};
  const bool enqueued = producer != nullptr
                            ? queue_.enqueue(*producer, std::move(item))
                            : queue_.enqueue(std::move(item));
  if (!enqueued) {
    queued_count_.fetch_sub(1, std::memory_order_release);
    FinishWork();
  }
  LeaveSubmission();
  return enqueued;
}

void ThreadPool::Impl::Stop() {
  // Joining from one of this pool's handlers would wait for the caller itself.
  assert(current_pool_ != this);
  std::lock_guard<std::mutex> lock(stop_mutex_);
  if (stopped_) {
    return;
  }
  admission_.fetch_or(kAdmissionClosed, std::memory_order_acq_rel);
  NETLIB_LOG_INFO("ThreadPool stopping");
  std::size_t state = admission_.load(std::memory_order_acquire);
  while (state != kAdmissionClosed) {
    admission_.wait(state, std::memory_order_acquire);
    state = admission_.load(std::memory_order_acquire);
  }
  // All accepted messages are published. Wait for handlers, not just dequeue,
  // before posting controls from another (implicit) producer stream.
  std::size_t outstanding = outstanding_.load(std::memory_order_acquire);
  while (outstanding != 0) {
    outstanding_.wait(outstanding, std::memory_order_acquire);
    outstanding = outstanding_.load(std::memory_order_acquire);
  }
  for (std::size_t i = 0; i < workers_.size(); ++i) {
    if (!queue_.enqueue(
            WorkItem{.kind = WorkItemKind::kStop, .message = FrameMessage{}})) {
      // A failed wake would leave a worker blocked forever. Resource exhaustion
      // on this mandatory control path is fatal instead of silently hanging.
      std::abort();
    }
  }
  for (std::thread& worker : workers_) {
    worker.join();
  }
  stopped_ = true;
  NETLIB_LOG_INFO("ThreadPool stopped");
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

void ThreadPool::Impl::FinishWork() {
  if (outstanding_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    outstanding_.notify_all();
  }
}

void ThreadPool::Impl::WorkerLoop(moodycamel::ConsumerToken& consumer_token,
                                  std::size_t index) {
  current_pool_ = this;
  current_index_ = index;
  while (true) {
    WorkItem item;
    queue_.wait_dequeue(consumer_token, item);
    if (item.kind == WorkItemKind::kStop) {
      break;
    }
    queued_count_.fetch_sub(1, std::memory_order_acq_rel);
    handler_(std::move(item.message));
    FinishWork();
  }
  current_pool_ = nullptr;
}

class ThreadPool::Submitter::State {
 public:
  explicit State(std::shared_ptr<Impl> pool)
      : pool_(std::move(pool)), producer_(pool_->CreateProducer()) {}
  bool valid() const { return producer_.valid(); }
  bool Submit(FrameMessage message) {
    return valid() && pool_->Submit(std::move(message), &producer_);
  }

 private:
  std::shared_ptr<Impl> pool_;
  moodycamel::ProducerToken producer_;
};

ThreadPool::Submitter::Submitter() = default;
ThreadPool::Submitter::Submitter(std::unique_ptr<State> state)
    : state_(std::move(state)) {}
ThreadPool::Submitter::Submitter(Submitter&&) noexcept = default;
ThreadPool::Submitter& ThreadPool::Submitter::operator=(Submitter&&) noexcept =
    default;
ThreadPool::Submitter::~Submitter() = default;
bool ThreadPool::Submitter::valid() const {
  return state_ != nullptr && state_->valid();
}
bool ThreadPool::Submitter::Submit(FrameMessage message) {
  return valid() && state_->Submit(std::move(message));
}

std::unique_ptr<ThreadPool> ThreadPool::Create(std::size_t worker_count,
                                               std::size_t max_pending_messages,
                                               Handler handler) {
  if (worker_count == 0 || max_pending_messages == 0 || !handler) {
    NETLIB_LOG_ERROR("invalid ThreadPool configuration");
    return nullptr;
  }
  std::shared_ptr<Impl> impl = std::make_shared<Impl>(
      worker_count, max_pending_messages, std::move(handler));
  return std::unique_ptr<ThreadPool>(new ThreadPool(std::move(impl)));
}

ThreadPool::ThreadPool(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}
ThreadPool::~ThreadPool() { impl_->Stop(); }
bool ThreadPool::Submit(FrameMessage message) {
  return impl_->Submit(std::move(message), nullptr);
}
ThreadPool::Submitter ThreadPool::CreateSubmitter() {
  return Submitter(std::make_unique<Submitter::State>(impl_));
}
std::size_t ThreadPool::worker_count() const { return impl_->worker_count(); }
std::optional<std::size_t> ThreadPool::CurrentWorkerIndex(
    const std::shared_ptr<Impl>& pool) {
  return pool->CurrentWorkerIndex();
}
void ThreadPool::Stop() { impl_->Stop(); }
std::size_t ThreadPool::queued_count() const { return impl_->queued_count(); }

}  // namespace netlib
