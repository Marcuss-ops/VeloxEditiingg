#pragma once

#ifdef VELOX_ENABLE_LIBAV

extern "C" {
#include <libavutil/frame.h>
}

#include <chrono>
#include "frame_pipeline_queue.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
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
    std::deque<int> free_;
    int64_t in_use_{0};
    int64_t peak_usage_{0};
    std::mutex mutex_;
    std::condition_variable available_;
    bool shutdown_{false};
};

bool publishProbedOutput(const std::filesystem::path& partial,
                         const std::filesystem::path& target,
                         std::string& error, bool* durable_out);

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
