#ifdef VELOX_ENABLE_LIBAV

#include "frame_pipeline_queue.hpp"

#include <algorithm>

namespace velox::media::pipeline_detail {

BoundedQueue::BoundedQueue(int capacity)
    : capacity_(std::max(capacity, 1)),
      slots_(static_cast<std::size_t>(capacity_)) {}

bool BoundedQueue::push(int value) {
    // Fast path is lock-free: one acquire load of the consumer cursor, one
    // release store of the producer cursor. The mutex is touched only when
    // the ring is full (blocking wait) or after shutdown.
    const auto wait_start = Clock::now();
    while (true) {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t head = head_.load(std::memory_order_acquire);
        if (tail - head < static_cast<std::size_t>(capacity_)) {
            slots_[tail % static_cast<std::size_t>(capacity_)] = value;
            tail_.store(tail + 1, std::memory_order_release);
            break;
        }
        if (done_.load(std::memory_order_acquire)) {
            full_wait_ns_ += elapsedNs(wait_start);
            return false;
        }
        // Ring full and not shutting down: block on the slow-path CV. A pop
        // notifies the waiter; the predicate closes the check-to-sleep race.
        std::unique_lock<std::mutex> lock(wait_mutex_);
        wait_cv_.wait(lock, [&] {
            return done_.load(std::memory_order_acquire) ||
                   tail_.load(std::memory_order_relaxed) -
                           head_.load(std::memory_order_acquire) <
                       static_cast<std::size_t>(capacity_);
        });
    }
    full_wait_ns_ += elapsedNs(wait_start);

    // Producer-private depth sample: no shared cache line with the consumer.
    const std::size_t depth =
        tail_.load(std::memory_order_relaxed) -
        head_.load(std::memory_order_acquire);
    high_water_ = std::max<int64_t>(high_water_,
                                    static_cast<int64_t>(depth));
    sampleDepthProducer(depth);
    wait_cv_.notify_one();
    return true;
}

bool BoundedQueue::pop(int& value) {
    // Symmetric lock-free fast path: one acquire load of the producer cursor,
    // one release store of the consumer cursor.
    const auto wait_start = Clock::now();
    while (true) {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t tail = tail_.load(std::memory_order_acquire);
        if (head != tail) {
            value = slots_[head % static_cast<std::size_t>(capacity_)];
            head_.store(head + 1, std::memory_order_release);
            break;
        }
        if (done_.load(std::memory_order_acquire)) {
            empty_wait_ns_ += elapsedNs(wait_start);
            return false;
        }
        // Ring empty and not shutting down: block on the slow-path CV. A
        // push notifies the waiter; the predicate closes the check-to-sleep
        // race.
        std::unique_lock<std::mutex> lock(wait_mutex_);
        wait_cv_.wait(lock, [&] {
            return done_.load(std::memory_order_acquire) ||
                   head_.load(std::memory_order_relaxed) !=
                       tail_.load(std::memory_order_acquire);
        });
    }
    empty_wait_ns_ += elapsedNs(wait_start);

    // Consumer-private depth sample.
    const std::size_t depth =
        tail_.load(std::memory_order_acquire) -
        head_.load(std::memory_order_relaxed);
    sampleDepthConsumer(depth);
    wait_cv_.notify_one();
    return true;
}

void BoundedQueue::shutdown() {
    // Store done_ under the mutex so a waiter that just evaluated its
    // predicate (before done_ was set) cannot miss the wakeup: it either
    // sees done_ in the predicate or is already inside wait_for and gets
    // the notify below.
    {
        std::lock_guard<std::mutex> lock(wait_mutex_);
        done_.store(true, std::memory_order_release);
    }
    wait_cv_.notify_all();
}

int64_t BoundedQueue::highWater() const { return high_water_; }
int64_t BoundedQueue::fullWaitMs() const { return nsToMs(full_wait_ns_); }
int64_t BoundedQueue::emptyWaitMs() const { return nsToMs(empty_wait_ns_); }
int64_t BoundedQueue::fullWaitNs() const { return full_wait_ns_; }
int64_t BoundedQueue::emptyWaitNs() const { return empty_wait_ns_; }

int64_t BoundedQueue::averageDepth() const {
    // Each side integrated depth over its own observed window; the union of
    // the two windows covers the queue's lifetime, so the sum of the two
    // integrals divided by the sum of the two windows is the time-weighted
    // average depth (same semantics as the previous single-sampled version).
    const int64_t window = window_ns_producer_ + window_ns_consumer_;
    if (window <= 0) {
        return 0;
    }
    const double average = static_cast<double>(depth_ns_producer_ +
                                               depth_ns_consumer_) /
                           static_cast<double>(window);
    return static_cast<int64_t>(average + 0.5);
}

int64_t BoundedQueue::elapsedNs(const Clock::time_point& start) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               Clock::now() - start).count();
}

int64_t BoundedQueue::nsToMs(int64_t ns) {
    return (ns + 500'000) / 1'000'000;
}

void BoundedQueue::sampleDepthProducer(std::size_t depth) {
    const auto now = Clock::now();
    const int64_t delta = elapsedNs(last_sample_producer_);
    depth_ns_producer_ += last_depth_producer_ * delta;
    window_ns_producer_ += delta;
    last_sample_producer_ = now;
    last_depth_producer_ = static_cast<int64_t>(depth);
}

void BoundedQueue::sampleDepthConsumer(std::size_t depth) {
    const auto now = Clock::now();
    const int64_t delta = elapsedNs(last_sample_consumer_);
    depth_ns_consumer_ += last_depth_consumer_ * delta;
    window_ns_consumer_ += delta;
    last_sample_consumer_ = now;
    last_depth_consumer_ = static_cast<int64_t>(depth);
}

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
