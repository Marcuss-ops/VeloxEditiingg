#pragma once

#ifdef VELOX_ENABLE_LIBAV

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>

namespace velox::media::pipeline_detail {

// Bounded single-producer/single-consumer ring handed between two pipeline
// stages (decode→render, render→encode). The data path is lock-free: the
// producer publishes a slot with a release store on its cursor, the consumer
// reads it with an acquire load on the same cursor. A mutex/condition-variable
// pair exists only for the blocking slow path (queue full/empty) and for
// shutdown wakeup. Depth telemetry is sampled in batches so the healthy fast
// path does not read the clock for every hand-off.
//
// Each side integrates its own depth/time samples so the averageDepth metric
// stays single-writer per field (no shared cache line between the two stage
// threads); averageDepth() sums both sides' integrals.
class BoundedQueue {
public:
    explicit BoundedQueue(int capacity);

    BoundedQueue(const BoundedQueue&) = delete;
    BoundedQueue& operator=(const BoundedQueue&) = delete;

    bool push(int value);
    bool pop(int& value);
    void shutdown();

    int64_t highWater() const;
    int64_t fullWaitMs() const;
    int64_t emptyWaitMs() const;
    int64_t fullWaitNs() const;
    int64_t emptyWaitNs() const;
    int64_t averageDepth();

private:
    using Clock = std::chrono::steady_clock;

    static int64_t elapsedNs(const Clock::time_point& start,
                             const Clock::time_point& end);
    static int64_t nsToMs(int64_t ns);

    // Per-side depth/time integral. Each is called only by its owning stage
    // thread, so the fields below need no synchronization.
    void sampleDepthProducer(std::size_t depth, Clock::time_point now);
    void sampleDepthConsumer(std::size_t depth, Clock::time_point now);

    static constexpr std::size_t kDepthSampleInterval = 32;

    struct alignas(64) ProducerMetrics {
        int64_t high_water{0};
        int64_t full_wait_ns{0};
        Clock::time_point last_sample{Clock::now()};
        int64_t last_depth{0};
        int64_t depth_ns{0};
        int64_t window_ns{0};
        std::size_t operations{0};
    };

    struct alignas(64) ConsumerMetrics {
        int64_t empty_wait_ns{0};
        Clock::time_point last_sample{Clock::now()};
        int64_t last_depth{0};
        int64_t depth_ns{0};
        int64_t window_ns{0};
        std::size_t operations{0};
    };

    int capacity_;
    std::vector<int> slots_;

    // Cursors count items (not slots); depth is a plain subtraction and slot
    // index is cursor % capacity. Each cursor has exactly one writer.
    alignas(64) std::atomic<std::size_t> tail_{0};  // producer cursor
    alignas(64) std::atomic<std::size_t> head_{0};  // consumer cursor

    // Blocking slow path + shutdown wakeup only. Lock order: a waiter holds
    // this mutex while evaluating predicates on the atomic cursors;
    // shutdown() takes the mutex before storing done_ so a wakeup can never
    // be lost between a waiter's predicate check and its sleep.
    std::mutex wait_mutex_;
    std::condition_variable wait_cv_;
    std::atomic<bool> done_{false};
    std::atomic<int> waiters_{0};

    // Explicit cache-line separation is part of the SPSC telemetry contract:
    // producer and consumer update disjoint lines while the data cursors are
    // being handed off.
    ProducerMetrics producer_metrics_;
    ConsumerMetrics consumer_metrics_;
};

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
