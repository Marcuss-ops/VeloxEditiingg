#ifdef VELOX_ENABLE_LIBAV

#include "frame_pipeline_support.hpp"

#include "velox/services/file_utils.hpp"
#include "velox/services/media_probe.hpp"

extern "C" {
#include <libavutil/imgutils.h>
}

#include <algorithm>

namespace fs = std::filesystem;

namespace velox::media::pipeline_detail {

void FrameDeleter::operator()(AVFrame* frame) const {
    av_frame_free(&frame);
}

bool FramePool::init(int capacity, int in_width, int in_height,
                     int out_width, int out_height, bool allocate_scaled,
                     AVPixelFormat scaled_format,
                     std::string& error) {
    if (capacity < 2 || capacity > 64) {
        error = "frame pool capacity must be between 2 and 64";
        return false;
    }
    capacity_ = capacity;
    scaled_enabled_ = allocate_scaled;
    free_slots_.resize(static_cast<size_t>(capacity));
    decoded_.resize(static_cast<size_t>(capacity));
    if (scaled_enabled_) {
        scaled_.resize(static_cast<size_t>(capacity));
    }
    for (int i = 0; i < capacity; ++i) {
        decoded_[static_cast<size_t>(i)].reset(av_frame_alloc());
        if (scaled_enabled_) {
            scaled_[static_cast<size_t>(i)].reset(av_frame_alloc());
        }
        if (!decoded_[static_cast<size_t>(i)] ||
            (scaled_enabled_ && !scaled_[static_cast<size_t>(i)])) {
            error = "av_frame_alloc failed";
            return false;
        }
        free_slots_[static_cast<size_t>(i)] = i;
        if (scaled_enabled_) {
            AVFrame* scaled = scaled_[static_cast<size_t>(i)].get();
            scaled->format = scaled_format;
            scaled->width = out_width;
            scaled->height = out_height;
            if (av_frame_get_buffer(scaled, 32) < 0) {
                error = "av_frame_get_buffer (scaled slot) failed";
                return false;
            }
        }
    }
    free_head_.store(0, std::memory_order_relaxed);
    free_tail_.store(static_cast<std::size_t>(capacity), std::memory_order_release);
    peak_usage_ = 0;
    in_width_ = in_width;
    in_height_ = in_height;
    return true;
}

int FramePool::acquire() {
    // Fast path: consume one index from the SPSC free-slot ring.
    std::size_t head = free_head_.load(std::memory_order_relaxed);
    for (;;) {
        const std::size_t tail = free_tail_.load(std::memory_order_acquire);
        if (head == tail) {
            if (shutdown_.load(std::memory_order_acquire)) {
                return -1;
            }
            // Pool exhausted and not shutting down: block on the slow-path
            // CV. release() and shutdown() both notify, preserving bounded
            // backpressure without consuming a CPU core while waiting.
            std::unique_lock<std::mutex> lock(wait_mutex_);
            waiters_.fetch_add(1, std::memory_order_relaxed);
            available_.wait(lock, [&] {
                return shutdown_.load(std::memory_order_acquire) ||
                       free_head_.load(std::memory_order_relaxed) !=
                           free_tail_.load(std::memory_order_acquire);
            });
            waiters_.fetch_sub(1, std::memory_order_relaxed);
            head = free_head_.load(std::memory_order_relaxed);
            continue;
        }
        const int index = free_slots_[head % static_cast<std::size_t>(capacity_)];
        free_head_.store(head + 1, std::memory_order_release);
        const int64_t in_use = static_cast<int64_t>(capacity_) -
            static_cast<int64_t>(tail - (head + 1));
        peak_usage_ = std::max(peak_usage_, in_use);
        return index;
    }
}

void FramePool::release(int index) {
    if (index < 0 || index >= capacity_) return;
    std::unique_lock<std::mutex> exceptional_lock(release_mutex_, std::defer_lock);
    if (shutdown_.load(std::memory_order_acquire)) {
        exceptional_lock.lock();
    }
    const std::size_t tail = free_tail_.load(std::memory_order_relaxed);
    free_slots_[tail % static_cast<std::size_t>(capacity_)] = index;
    free_tail_.store(tail + 1, std::memory_order_release);
    if (waiters_.load(std::memory_order_relaxed) > 0) {
        available_.notify_one();
    }
}

void FramePool::shutdown() {
    {
        std::lock_guard<std::mutex> lock(wait_mutex_);
        shutdown_.store(true, std::memory_order_release);
    }
    available_.notify_all();
}

int FramePool::capacity() const {
    return capacity_;
}

int64_t FramePool::peakUsage() const {
    return peak_usage_;
}

AVFrame* FramePool::decoded(int index) {
    return decoded_[static_cast<size_t>(index)].get();
}

AVFrame* FramePool::scaled(int index) {
    return scaled_[static_cast<size_t>(index)].get();
}

bool publishProbedOutput(const fs::path& partial, const fs::path& target,
                         std::string& error, bool* durable_out) {
    const auto final_probe = probeMediaInProcess(partial);
    bool has_video = false;
    if (final_probe.has_value()) {
        for (const auto& stream : final_probe->streams) {
            has_video = has_video || stream.is_video;
        }
    }
    if (!final_probe.has_value() || !has_video) {
        error = "frame pipeline output probe failed (no video stream)";
        return false;
    }
    bool durable = false;
    if (!file::publishAtomic(partial, target, &error, &durable)) {
        return false;
    }
    if (durable_out != nullptr) {
        *durable_out = durable;
    }
    return true;
}

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
