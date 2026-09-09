#ifdef VELOX_ENABLE_LIBAV

#include "frame_pipeline_encoder.hpp"

#include <string>

namespace velox::media::pipeline_detail {

EncoderStage::~EncoderStage() {
    if (packet_ != nullptr) {
        av_packet_free(&packet_);
    }
}

bool EncoderStage::sendFrame(AVFrame* frame, std::string& error) {
    if (packet_ == nullptr) {
        packet_ = av_packet_alloc();
        if (packet_ == nullptr) {
            error = "av_packet_alloc failed";
            return false;
        }
    }
    const int result = avcodec_send_frame(config_.encoder, frame);
    if (result < 0) {
        error = "avcodec_send_frame failed: " + ffmpegErrorText(result);
        return false;
    }
    return drain(error);
}

bool EncoderStage::flush(std::string& error) {
    if (packet_ == nullptr) {
        packet_ = av_packet_alloc();
        if (packet_ == nullptr) {
            error = "av_packet_alloc failed";
            return false;
        }
    }
    const int result = avcodec_send_frame(config_.encoder, nullptr);
    if (result < 0 && result != AVERROR_EOF) {
        error = "encoder flush failed: " + ffmpegErrorText(result);
        return false;
    }
    return drain(error);
}

bool EncoderStage::drain(std::string& error) {
    // packet_ is allocated lazily by sendFrame/flush before drain runs; the
    // same scratch packet is reused for every received packet of the segment.
    while (true) {
        av_packet_unref(packet_);
        const int result = avcodec_receive_packet(config_.encoder, packet_);
        if (result == AVERROR(EAGAIN) || result == AVERROR_EOF) {
            return true;
        }
        if (result < 0) {
            error = "avcodec_receive_packet failed: " + ffmpegErrorText(result);
            return false;
        }
        packet_->stream_index = config_.output_stream->index;
        av_packet_rescale_ts(packet_, config_.encoder->time_base,
                             config_.output_stream->time_base);
        packet_->time_base = config_.output_stream->time_base;
        if (av_interleaved_write_frame(config_.muxer, packet_) < 0) {
            error = "av_interleaved_write_frame failed";
            return false;
        }
        config_.encoded_packets->fetch_add(1);
    }
}

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
