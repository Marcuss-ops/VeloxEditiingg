#pragma once

#ifdef VELOX_ENABLE_LIBAV

#include "frame_pipeline_support.hpp"
#include "velox/services/frame_pipeline.hpp"
#include "velox/services/media_packet_components.hpp"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
}

namespace velox::media::pipeline_detail {

struct StageConfig {
    packet::Demuxer* demuxer{nullptr};
    const AVStream* input_stream{nullptr};
    int video_index{-1};
    AVCodecContext* decoder{nullptr};
    AVCodecContext* encoder{nullptr};
    AVStream* output_stream{nullptr};
    AVFormatContext* muxer{nullptr};
    FramePool* pool{nullptr};
    BoundedQueue* render_queue{nullptr};
    BoundedQueue* encode_queue{nullptr};
    bool transform_bypass{false};
    int source_height{0};
    int64_t source_start_us{0};
    int64_t source_end_us{0};
    int64_t stream_start_us{0};
    const velox::render::FrameGraph* frame_graph{nullptr};
};

struct StageResult {
    bool success{false};
    std::string error;
    int64_t frames_decoded{0};
    int64_t frames_encoded{0};
    int64_t transform_bypass_frames{0};
    int64_t peak_pool_usage{0};
    int64_t peak_render_queue{0};
    int64_t peak_encode_queue{0};
    FramePipelineMetrics metrics;
};

StageResult runStages(const StageConfig& config);

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
