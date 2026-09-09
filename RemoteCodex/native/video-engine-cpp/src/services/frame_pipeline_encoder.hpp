#pragma once

#ifdef VELOX_ENABLE_LIBAV

#include "frame_pipeline_support.hpp"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
}

#include <atomic>
#include <cstdint>
#include <string>

namespace velox::media::pipeline_detail {

struct EncoderStageConfig {
    AVCodecContext* encoder{nullptr};
    AVStream* output_stream{nullptr};
    AVFormatContext* muxer{nullptr};
    std::atomic<int64_t>* encoded_packets{nullptr};
};

class EncoderStage {
public:
    explicit EncoderStage(const EncoderStageConfig& config) : config_(config) {}

    ~EncoderStage();

    EncoderStage(const EncoderStage&) = delete;
    EncoderStage& operator=(const EncoderStage&) = delete;

    bool sendFrame(AVFrame* frame, std::string& error);
    bool flush(std::string& error);

private:
    bool drain(std::string& error);

    EncoderStageConfig config_;
    // Scratch packet reused across every drain() call: one av_packet_alloc
    // for the whole segment instead of one per encoded frame.
    AVPacket* packet_{nullptr};
};

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
