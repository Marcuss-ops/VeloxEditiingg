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
    bool blocked = false;
    Clock::time_point wait_start{};
    while (true) {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t head = head_.load(std::memory_order_acquire);
        if (tail - head < static_cast<std::size_t>(capacity_)) {
            slots_[tail % static_cast<std::size_t>(capacity_)] = value;
            tail_.store(tail + 1, std::memory_order_release);
            break;
        }
        if (done_.load(std::memory_order_acquire)) {
            if (blocked) {
                producer_metrics_.full_wait_ns += elapsedNs(wait_start, Clock::now());
            }
            return false;
        }
        // Ring full and not shutting down: block on the slow-path CV. A pop
        // notifies the waiter; the predicate closes the check-to-sleep race.
        std::unique_lock<std::mutex> lock(wait_mutex_);
        if (!blocked) {
            wait_start = Clock::now();
            blocked = true;
        }
        waiters_.fetch_add(1, std::memory_order_relaxed);
        wait_cv_.wait(lock, [&] {
            return done_.load(std::memory_order_acquire) ||
                   tail_.load(std::memory_order_relaxed) -
                           head_.load(std::memory_order_acquire) <
                       static_cast<std::size_t>(capacity_);
        });
        waiters_.fetch_sub(1, std::memory_order_relaxed);
    }
    const auto now = blocked ? Clock::now() : Clock::time_point{};
    if (blocked) {
        producer_metrics_.full_wait_ns += elapsedNs(wait_start, now);
    }

    // Producer-private depth sample: no shared cache line with the consumer.
    const std::size_t depth =
        tail_.load(std::memory_order_relaxed) -
        head_.load(std::memory_order_acquire);
    producer_metrics_.high_water = std::max<int64_t>(
        producer_metrics_.high_water, static_cast<int64_t>(depth));
    ++producer_metrics_.operations;
    if (blocked || producer_metrics_.operations % kDepthSampleInterval == 0) {
        sampleDepthProducer(depth, blocked ? now : Clock::now());
    }
    if (waiters_.load(std::memory_order_relaxed) > 0) {
        wait_cv_.notify_one();
    }
    return true;
}

bool BoundedQueue::pop(int& value) {
    // Symmetric lock-free fast path: one acquire load of the producer cursor,
    // one release store of the consumer cursor.
    bool blocked = false;
    Clock::time_point wait_start{};
    while (true) {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t tail = tail_.load(std::memory_order_acquire);
        if (head != tail) {
            value = slots_[head % static_cast<std::size_t>(capacity_)];
            head_.store(head + 1, std::memory_order_release);
            break;
        }
        if (done_.load(std::memory_order_acquire)) {
            if (blocked) {
                consumer_metrics_.empty_wait_ns += elapsedNs(wait_start, Clock::now());
            }
            return false;
        }
        // Ring empty and not shutting down: block on the slow-path CV. A
        // push notifies the waiter; the predicate closes the check-to-sleep
        // race.
        std::unique_lock<std::mutex> lock(wait_mutex_);
        if (!blocked) {
            wait_start = Clock::now();
            blocked = true;
        }
        waiters_.fetch_add(1, std::memory_order_relaxed);
        wait_cv_.wait(lock, [&] {
            return done_.load(std::memory_order_acquire) ||
                   head_.load(std::memory_order_relaxed) !=
                       tail_.load(std::memory_order_acquire);
        });
        waiters_.fetch_sub(1, std::memory_order_relaxed);
    }
    const auto now = blocked ? Clock::now() : Clock::time_point{};
    if (blocked) {
        consumer_metrics_.empty_wait_ns += elapsedNs(wait_start, now);
    }

    // Consumer-private depth sample.
    const std::size_t depth =
        tail_.load(std::memory_order_acquire) -
        head_.load(std::memory_order_relaxed);
    ++consumer_metrics_.operations;
    if (blocked || consumer_metrics_.operations % kDepthSampleInterval == 0) {
        sampleDepthConsumer(depth, blocked ? now : Clock::now());
    }
    if (waiters_.load(std::memory_order_relaxed) > 0) {
        wait_cv_.notify_one();
    }
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

int64_t BoundedQueue::highWater() const { return producer_metrics_.high_water; }
int64_t BoundedQueue::fullWaitMs() const { return nsToMs(producer_metrics_.full_wait_ns); }
int64_t BoundedQueue::emptyWaitMs() const { return nsToMs(consumer_metrics_.empty_wait_ns); }
int64_t BoundedQueue::fullWaitNs() const { return producer_metrics_.full_wait_ns; }
int64_t BoundedQueue::emptyWaitNs() const { return consumer_metrics_.empty_wait_ns; }

int64_t BoundedQueue::averageDepth() {
    // Each side integrated depth over its own observed window; the union of
    // the two windows covers the queue's lifetime, so the sum of the two
    // integrals divided by the sum of the two windows is the time-weighted
    // average depth (same semantics as the previous single-sampled version).
    const auto now = Clock::now();
    const std::size_t depth = tail_.load(std::memory_order_relaxed) -
        head_.load(std::memory_order_acquire);
    sampleDepthProducer(depth, now);
    sampleDepthConsumer(depth, now);
    const int64_t window = producer_metrics_.window_ns + consumer_metrics_.window_ns;
    if (window <= 0) {
        return 0;
    }
    const double average = static_cast<double>(producer_metrics_.depth_ns +
                                               consumer_metrics_.depth_ns) /
                           static_cast<double>(window);
    return static_cast<int64_t>(average + 0.5);
}

int64_t BoundedQueue::elapsedNs(const Clock::time_point& start,
                                const Clock::time_point& end) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               end - start).count();
}

int64_t BoundedQueue::nsToMs(int64_t ns) {
    return (ns + 500'000) / 1'000'000;
}

void BoundedQueue::sampleDepthProducer(std::size_t depth, Clock::time_point now) {
    const int64_t delta = elapsedNs(producer_metrics_.last_sample, now);
    producer_metrics_.depth_ns += producer_metrics_.last_depth * delta;
    producer_metrics_.window_ns += delta;
    producer_metrics_.last_sample = now;
    producer_metrics_.last_depth = static_cast<int64_t>(depth);
}

void BoundedQueue::sampleDepthConsumer(std::size_t depth, Clock::time_point now) {
    const int64_t delta = elapsedNs(consumer_metrics_.last_sample, now);
    consumer_metrics_.depth_ns += consumer_metrics_.last_depth * delta;
    consumer_metrics_.window_ns += delta;
    consumer_metrics_.last_sample = now;
    consumer_metrics_.last_depth = static_cast<int64_t>(depth);
}

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
