#ifdef VELOX_ENABLE_LIBAV

#include "frame_pipeline_queue.hpp"

#include <algorithm>

namespace velox::media::pipeline_detail {

BoundedQueue::BoundedQueue(int capacity) : capacity_(capacity) {}

bool BoundedQueue::push(int value) {
    std::unique_lock<std::mutex> lock(mutex_);
    const auto wait_start = Clock::now();
    not_full_.wait(lock, [&] {
        return done_ || static_cast<int>(items_.size()) < capacity_;
    });
    full_wait_ns_ += elapsedNs(wait_start);
    if (done_) {
        return false;
    }
    items_.push_back(value);
    high_water_ = std::max<int64_t>(high_water_, static_cast<int64_t>(items_.size()));
    sampleDepth();
    not_empty_.notify_one();
    return true;
}

bool BoundedQueue::pop(int& value) {
    std::unique_lock<std::mutex> lock(mutex_);
    const auto wait_start = Clock::now();
    not_empty_.wait(lock, [&] { return done_ || !items_.empty(); });
    empty_wait_ns_ += elapsedNs(wait_start);
    if (items_.empty()) {
        return false;
    }
    value = items_.front();
    items_.pop_front();
    sampleDepth();
    not_full_.notify_one();
    return true;
}

void BoundedQueue::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    sampleDepth();
    done_ = true;
    not_full_.notify_all();
    not_empty_.notify_all();
}

int64_t BoundedQueue::highWater() const { return high_water_; }
int64_t BoundedQueue::fullWaitMs() const { return nsToMs(full_wait_ns_); }
int64_t BoundedQueue::emptyWaitMs() const { return nsToMs(empty_wait_ns_); }
int64_t BoundedQueue::fullWaitNs() const { return full_wait_ns_; }
int64_t BoundedQueue::emptyWaitNs() const { return empty_wait_ns_; }

int64_t BoundedQueue::averageDepth() const {
    if (window_ns_ <= 0) {
        return 0;
    }
    const double average = static_cast<double>(depth_ns_) /
                           static_cast<double>(window_ns_);
    return static_cast<int64_t>(average + 0.5);
}

int64_t BoundedQueue::elapsedNs(const Clock::time_point& start) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               Clock::now() - start).count();
}

int64_t BoundedQueue::nsToMs(int64_t ns) {
    return (ns + 500'000) / 1'000'000;
}

void BoundedQueue::sampleDepth() {
    const auto now = Clock::now();
    const int64_t delta = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              now - last_sample_).count();
    depth_ns_ += static_cast<int64_t>(last_depth_) * delta;
    window_ns_ += delta;
    last_sample_ = now;
    last_depth_ = static_cast<int64_t>(items_.size());
}

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
