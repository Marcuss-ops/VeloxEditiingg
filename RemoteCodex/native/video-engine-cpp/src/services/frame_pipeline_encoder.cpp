#ifdef VELOX_ENABLE_LIBAV

#include "frame_pipeline_encoder.hpp"

extern "C" {
#include <libavutil/error.h>
}

#include <memory>
#include <string>

namespace velox::media::pipeline_detail {
namespace {

struct PacketDeleter {
    void operator()(AVPacket* packet) const { av_packet_free(&packet); }
};
using UniquePacket = std::unique_ptr<AVPacket, PacketDeleter>;

std::string ffmpegErrorText(int error) {
    char buffer[AV_ERROR_MAX_STRING_SIZE]{};
    av_strerror(error, buffer, sizeof(buffer));
    return buffer;
}

} // namespace

bool EncoderStage::sendFrame(AVFrame* frame, std::string& error) {
    const int result = avcodec_send_frame(config_.encoder, frame);
    if (result < 0) {
        error = "avcodec_send_frame failed: " + ffmpegErrorText(result);
        return false;
    }
    return drain(error);
}

bool EncoderStage::flush(std::string& error) {
    const int result = avcodec_send_frame(config_.encoder, nullptr);
    if (result < 0 && result != AVERROR_EOF) {
        error = "encoder flush failed: " + ffmpegErrorText(result);
        return false;
    }
    return drain(error);
}

bool EncoderStage::drain(std::string& error) {
    UniquePacket packet(av_packet_alloc());
    if (!packet) {
        error = "av_packet_alloc failed";
        return false;
    }
    while (true) {
        const int result = avcodec_receive_packet(config_.encoder, packet.get());
        if (result == AVERROR(EAGAIN) || result == AVERROR_EOF) {
            return true;
        }
        if (result < 0) {
            error = "avcodec_receive_packet failed: " + ffmpegErrorText(result);
            return false;
        }
        packet->stream_index = config_.output_stream->index;
        av_packet_rescale_ts(packet.get(), config_.encoder->time_base,
                             config_.output_stream->time_base);
        packet->time_base = config_.output_stream->time_base;
        if (av_interleaved_write_frame(config_.muxer, packet.get()) < 0) {
            error = "av_interleaved_write_frame failed";
            return false;
        }
        av_packet_unref(packet.get());
        config_.encoded_packets->fetch_add(1);
    }
}

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
