#pragma once

#ifdef VELOX_ENABLE_LIBAV

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>

namespace velox::media::pipeline_detail {

class BoundedQueue {
public:
    explicit BoundedQueue(int capacity);

    bool push(int value);
    bool pop(int& value);
    void shutdown();

    int64_t highWater() const;
    int64_t fullWaitMs() const;
    int64_t emptyWaitMs() const;
    int64_t fullWaitNs() const;
    int64_t emptyWaitNs() const;
    int64_t averageDepth() const;

private:
    using Clock = std::chrono::steady_clock;

    static int64_t elapsedNs(const Clock::time_point& start);
    static int64_t nsToMs(int64_t ns);
    void sampleDepth();

    int capacity_;
    std::deque<int> items_;
    mutable std::mutex mutex_;
    std::condition_variable not_full_;
    std::condition_variable not_empty_;
    bool done_{false};
    int64_t high_water_{0};
    int64_t full_wait_ns_{0};
    int64_t empty_wait_ns_{0};
    int64_t depth_ns_{0};
    int64_t window_ns_{0};
    Clock::time_point last_sample_{Clock::now()};
    int64_t last_depth_{0};
};

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
