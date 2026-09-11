#pragma once

#ifdef VELOX_ENABLE_LIBAV

extern "C" {
#include <libavutil/frame.h>
}

#include <chrono>
#include "frame_pipeline_queue.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace velox::media::pipeline_detail {

struct FrameDeleter {
    void operator()(AVFrame* frame) const;
};
using UniqueFrame = std::unique_ptr<AVFrame, FrameDeleter>;

class FramePool {
public:
    bool init(int capacity, int in_width, int in_height,
              int out_width, int out_height, bool allocate_scaled,
              AVPixelFormat scaled_format,
              std::string& error);
    int acquire();
    void release(int index);
    void shutdown();

    int capacity() const;
    int64_t peakUsage() const;
    AVFrame* decoded(int index);
    AVFrame* scaled(int index);

private:
    int capacity_{0};
    bool scaled_enabled_{false};
    int in_width_{0};
    int in_height_{0};
    std::vector<UniqueFrame> decoded_;
    std::vector<UniqueFrame> scaled_;

    // Free-slot ring. The decoder is the sole consumer and the encoder is
    // the sole producer, so slot admission/release uses the same SPSC
    // acquire/release discipline as the frame queues: no per-frame CAS/RMW
    // crosses cores and no heap node is created.
    std::vector<int> free_slots_;
    alignas(64) std::atomic<std::size_t> free_head_{0};
    alignas(64) std::atomic<std::size_t> free_tail_{0};
    int64_t peak_usage_{0};
    std::atomic<bool> shutdown_{false};
    std::atomic<int> waiters_{0};
    // Only the exceptional shutdown/error path can release from a second
    // stage thread. Normal encoder releases stay lock-free; shutdown turns
    // this mutex into the multi-producer safety fence.
    std::mutex release_mutex_;
    std::mutex wait_mutex_;
    std::condition_variable available_;
};

bool publishProbedOutput(const std::filesystem::path& partial,
                         const std::filesystem::path& target,
                         std::string& error, bool* durable_out);

// Single av_strerror wrapper for the frame-pipeline implementation units
// (decoder, encoder, filter, public lifecycle). The packet pipeline keeps
// its identical packet::ffmpegError in media_packet_demuxer.cpp; both exist
// so error text formatting has one definition per pipeline family instead
// of one per translation unit.
inline std::string ffmpegErrorText(int error) {
    char buffer[AV_ERROR_MAX_STRING_SIZE]{};
    av_strerror(error, buffer, sizeof(buffer));
    return buffer;
}

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
