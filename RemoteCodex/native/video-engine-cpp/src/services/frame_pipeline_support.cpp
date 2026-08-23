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
                     std::string& error) {
    if (capacity < 2) {
        error = "frame pool capacity must be >= 2";
        return false;
    }
    capacity_ = capacity;
    scaled_enabled_ = allocate_scaled;
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
        if (scaled_enabled_) {
            AVFrame* scaled = scaled_[static_cast<size_t>(i)].get();
            scaled->format = AV_PIX_FMT_YUV420P;
            scaled->width = out_width;
            scaled->height = out_height;
            if (av_frame_get_buffer(scaled, 32) < 0) {
                error = "av_frame_get_buffer (scaled slot) failed";
                return false;
            }
        }
        free_.push_back(i);
    }
    in_width_ = in_width;
    in_height_ = in_height;
    return true;
}

int FramePool::acquire() {
    std::unique_lock<std::mutex> lock(mutex_);
    available_.wait(lock, [&] { return shutdown_ || !free_.empty(); });
    if (free_.empty()) {
        return -1;
    }
    const int index = free_.front();
    free_.pop_front();
    ++in_use_;
    peak_usage_ = std::max<int64_t>(peak_usage_, in_use_);
    return index;
}

void FramePool::release(int index) {
    std::lock_guard<std::mutex> lock(mutex_);
    free_.push_back(index);
    --in_use_;
    available_.notify_one();
}

void FramePool::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    shutdown_ = true;
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
