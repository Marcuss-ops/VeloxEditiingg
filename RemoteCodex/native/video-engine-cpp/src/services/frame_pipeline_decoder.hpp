#pragma once

#ifdef VELOX_ENABLE_LIBAV

#include "frame_pipeline_support.hpp"
#include "velox/services/media_packet_components.hpp"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
}

#include <atomic>
#include <cstdint>
#include <string>

namespace velox::media::pipeline_detail {

struct DecoderStageConfig {
    packet::Demuxer* demuxer{nullptr};
    const AVStream* input_stream{nullptr};
    int video_index{-1};
    AVCodecContext* decoder{nullptr};
    FramePool* pool{nullptr};
    BoundedQueue* render_queue{nullptr};
    int64_t source_start_us{0};
    int64_t source_end_us{0};
    int64_t stream_start_us{0};
    std::atomic<int64_t>* decoded_frames{nullptr};
    std::atomic<bool>* source_window_complete{nullptr};
};

class DecoderStage {
public:
    explicit DecoderStage(const DecoderStageConfig& config) : config_(config) {}

    bool sendPacket(AVPacket* packet, std::string& error);
    bool flush(std::string& error);

private:
    bool receiveFrames(std::string& error);
    bool acceptFrame(AVFrame* frame, int index);

    DecoderStageConfig config_;
};

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
