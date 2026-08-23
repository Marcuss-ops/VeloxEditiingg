#ifdef VELOX_ENABLE_LIBAV

#include "frame_pipeline_decoder.hpp"

extern "C" {
#include <libavutil/error.h>
#include <libavutil/mathematics.h>
}

#include <string>

namespace velox::media::pipeline_detail {
namespace {

std::string ffmpegErrorText(int error) {
    char buffer[AV_ERROR_MAX_STRING_SIZE]{};
    av_strerror(error, buffer, sizeof(buffer));
    return buffer;
}

} // namespace

bool DecoderStage::sendPacket(AVPacket* packet, std::string& error) {
    const int result = avcodec_send_packet(config_.decoder, packet);
    if (result < 0) {
        error = "avcodec_send_packet failed: " + ffmpegErrorText(result);
        return false;
    }
    return receiveFrames(error);
}

bool DecoderStage::flush(std::string& error) {
    const int result = avcodec_send_packet(config_.decoder, nullptr);
    if (result < 0 && result != AVERROR_EOF) {
        error = "decoder flush failed: " + ffmpegErrorText(result);
        return false;
    }
    return receiveFrames(error);
}

bool DecoderStage::receiveFrames(std::string& error) {
    while (true) {
        const int index = config_.pool->acquire();
        if (index < 0) {
            error = "frame pool stopped while decoder was receiving frames";
            return false;
        }
        AVFrame* decoded = config_.pool->decoded(index);
        av_frame_unref(decoded);
        const int result = avcodec_receive_frame(config_.decoder, decoded);
        if (result == AVERROR(EAGAIN) || result == AVERROR_EOF) {
            config_.pool->release(index);
            return true;
        }
        if (result < 0) {
            config_.pool->release(index);
            error = "avcodec_receive_frame failed: " + ffmpegErrorText(result);
            return false;
        }
        if (!acceptFrame(decoded, index)) {
            config_.pool->release(index);
            if (config_.source_window_complete != nullptr &&
                config_.source_window_complete->load()) {
                return true;
            }
            continue;
        }
        config_.decoded_frames->fetch_add(1);
        if (!config_.render_queue->push(index)) {
            config_.pool->release(index);
            error = "decoder render queue stopped";
            return false;
        }
    }
}

bool DecoderStage::acceptFrame(AVFrame* frame, int index) {
    (void)index;
    if (config_.source_start_us <= 0 && config_.source_end_us <= 0) {
        return true;
    }
    const int64_t timestamp = frame->best_effort_timestamp;
    if (timestamp == AV_NOPTS_VALUE) {
        return true;
    }
    const int64_t frame_us = av_rescale_q(
        timestamp, config_.input_stream->time_base,
        AVRational{1, 1'000'000}) - config_.stream_start_us;
    if (frame_us < config_.source_start_us) {
        return false;
    }
    if (config_.source_end_us > 0 && frame_us >= config_.source_end_us) {
        config_.source_window_complete->store(true);
        return false;
    }
    return true;
}

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
