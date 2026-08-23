// frame_pipeline.cpp — public lifecycle for the in-process AVFrame pipeline.
//
// The public contract stays in frame_pipeline.hpp. Queue/pool ownership and
// decode/render/encode coordination live in the private support/stage units.

#ifdef VELOX_ENABLE_LIBAV

#include "velox/services/frame_pipeline.hpp"

#include "frame_pipeline_stages.hpp"
#include "frame_pipeline_support.hpp"

#include "velox/services/file_utils.hpp"
#include "velox/services/media_packet_components.hpp"
#include "velox/services/media_probe.hpp"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/error.h>
#include <libavutil/opt.h>
}

#include <algorithm>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>

namespace fs = std::filesystem;
namespace packet = velox::media::packet;

namespace velox::media {
namespace {

struct CodecContextDeleter {
    void operator()(AVCodecContext* context) const {
        avcodec_free_context(&context);
    }
};
using UniqueCodecContext = std::unique_ptr<AVCodecContext, CodecContextDeleter>;

struct FormatContextDeleter {
    void operator()(AVFormatContext* context) const {
        if (context != nullptr) {
            avformat_free_context(context);
        }
    }
};
using UniqueOutputContext = std::unique_ptr<AVFormatContext, FormatContextDeleter>;

std::string ffmpegErrorText(int error) {
    char buffer[AV_ERROR_MAX_STRING_SIZE]{};
    av_strerror(error, buffer, sizeof(buffer));
    return buffer;
}

void setError(FramePipelineResult* result, const std::string& error) {
    result->success = false;
    result->error = error;
}

} // namespace

bool renderFrames(const FramePipelineConfig& config, FramePipelineResult* result) {
    FramePipelineResult local;
    if (result == nullptr) {
        result = &local;
    }
    *result = FramePipelineResult{};

    if (config.input_path.empty() || config.output_path.empty()) {
        setError(result, "frame pipeline requires input and output paths");
        return false;
    }
    std::error_code stat_error;
    if (!fs::is_regular_file(config.input_path, stat_error)) {
        setError(result, "frame pipeline input is not a regular file");
        return false;
    }
    if (config.pool_capacity < 2 || config.pool_capacity > 64) {
        setError(result, "frame pipeline pool_capacity must be in [2, 64]");
        return false;
    }
    if (config.fps_num <= 0 || config.fps_den <= 0) {
        setError(result, "frame pipeline requires positive fps_num/fps_den");
        return false;
    }
    if (config.codec.empty()) {
        setError(result, "frame pipeline requires a codec name");
        return false;
    }

    fs::path parent = config.output_path.parent_path();
    std::error_code ec;
    if (parent.empty()) {
        parent = fs::current_path(ec);
    }
    if (ec || parent.empty()) {
        setError(result, "frame pipeline cannot resolve output directory");
        return false;
    }
    fs::create_directories(parent, ec);
    if (ec) {
        setError(result, "frame pipeline cannot create output directory: " + ec.message());
        return false;
    }
    const fs::path partial = file::makePartialPath(config.output_path);
    const auto cleanup_partial = [&]() {
        std::error_code remove_error;
        fs::remove(partial, remove_error);
    };

    packet::Demuxer demuxer;
    std::string error;
    if (!demuxer.open(config.input_path, error)) {
        setError(result, "frame pipeline open input: " + error);
        return false;
    }
    const int video_index = demuxer.firstStream(AVMEDIA_TYPE_VIDEO);
    if (video_index < 0) {
        setError(result, "frame pipeline input has no video stream");
        return false;
    }
    const AVStream* input_stream = demuxer.stream(video_index);

    if (config.source_in_us < 0 || config.source_duration_us < 0 ||
        (config.source_in_us > 0 &&
         config.source_duration_us > std::numeric_limits<int64_t>::max() -
             config.source_in_us)) {
        setError(result, "frame pipeline source window is invalid");
        return false;
    }
    const int64_t source_start_us = config.source_in_us;
    const int64_t source_end_us = config.source_duration_us > 0
        ? config.source_in_us + config.source_duration_us
        : 0;
    const int64_t stream_start_us = input_stream != nullptr &&
            input_stream->start_time != AV_NOPTS_VALUE
        ? av_rescale_q(input_stream->start_time, input_stream->time_base,
                       AVRational{1, 1'000'000})
        : 0;

    const AVCodec* decoder = avcodec_find_decoder(input_stream->codecpar->codec_id);
    if (decoder == nullptr) {
        setError(result, "frame pipeline has no decoder for the input codec");
        return false;
    }
    UniqueCodecContext decoder_context(avcodec_alloc_context3(decoder));
    if (!decoder_context) {
        setError(result, "avcodec_alloc_context3 (decoder) failed");
        return false;
    }
    if (avcodec_parameters_to_context(decoder_context.get(), input_stream->codecpar) < 0) {
        setError(result, "avcodec_parameters_to_context (decoder) failed");
        return false;
    }
    if (config.decoder_threads > 0) {
        decoder_context->thread_count = config.decoder_threads;
    }
    if (avcodec_open2(decoder_context.get(), decoder, nullptr) < 0) {
        setError(result, "avcodec_open2 (decoder) failed");
        return false;
    }
    result->decoder_contexts_created = 1;

    const int src_width = decoder_context->width;
    const int src_height = decoder_context->height;
    if (src_width <= 0 || src_height <= 0) {
        setError(result, "frame pipeline input has invalid dimensions");
        return false;
    }
    const int out_width = config.width > 0 ? config.width : src_width;
    const int out_height = config.height > 0 ? config.height : src_height;
    if (out_width <= 0 || out_height <= 0) {
        setError(result, "frame pipeline output dimensions are invalid");
        return false;
    }

    const AVCodec* encoder = avcodec_find_encoder_by_name(config.codec.c_str());
    if (encoder == nullptr) {
        setError(result, "frame pipeline encoder not found: " + config.codec);
        return false;
    }
    UniqueCodecContext encoder_context(avcodec_alloc_context3(encoder));
    if (!encoder_context) {
        setError(result, "avcodec_alloc_context3 (encoder) failed");
        return false;
    }
    encoder_context->width = out_width;
    encoder_context->height = out_height;
    encoder_context->pix_fmt = AV_PIX_FMT_YUV420P;
    if (encoder->pix_fmts != nullptr && encoder->pix_fmts[0] != AV_PIX_FMT_NONE) {
        encoder_context->pix_fmt = encoder->pix_fmts[0];
    }
    encoder_context->time_base = AVRational{config.fps_den, config.fps_num};
    encoder_context->framerate = AVRational{config.fps_num, config.fps_den};
    encoder_context->gop_size = std::max(1, (config.fps_num / config.fps_den) * 2);
    encoder_context->max_b_frames = 0;
    encoder_context->bit_rate = 0;
    if (config.codec == "libx264") {
        if (!config.preset.empty()) {
            av_opt_set(encoder_context->priv_data, "preset", config.preset.c_str(), 0);
        }
        av_opt_set_int(encoder_context->priv_data, "crf", 23, 0);
    }
    if (config.encoder_threads > 0) {
        encoder_context->thread_count = config.encoder_threads;
    }
    if (avcodec_open2(encoder_context.get(), encoder, nullptr) < 0) {
        setError(result, "avcodec_open2 (encoder) failed for " + config.codec);
        return false;
    }
    result->encode_contexts_created = 1;

    const bool transform_bypass = src_width == out_width &&
        src_height == out_height && decoder_context->pix_fmt == encoder_context->pix_fmt;
    pipeline_detail::FramePool pool;
    std::string pool_error;
    if (!pool.init(config.pool_capacity, src_width, src_height,
                   out_width, out_height, !transform_bypass, pool_error)) {
        setError(result, pool_error);
        return false;
    }

    AVFormatContext* raw_mux = nullptr;
    if (avformat_alloc_output_context2(&raw_mux, nullptr, "mp4", partial.c_str()) < 0 ||
        raw_mux == nullptr) {
        cleanup_partial();
        setError(result, "avformat_alloc_output_context2 failed");
        return false;
    }
    UniqueOutputContext mux(raw_mux);
    AVStream* output_stream = avformat_new_stream(mux.get(), nullptr);
    if (output_stream == nullptr) {
        cleanup_partial();
        setError(result, "avformat_new_stream failed");
        return false;
    }
    if (avcodec_parameters_from_context(output_stream->codecpar,
                                        encoder_context.get()) < 0) {
        cleanup_partial();
        setError(result, "avcodec_parameters_from_context failed");
        return false;
    }
    output_stream->time_base = AVRational{1, 90'000};
    output_stream->avg_frame_rate = AVRational{config.fps_num, config.fps_den};
    if (avio_open(&mux->pb, partial.c_str(), AVIO_FLAG_WRITE) < 0) {
        cleanup_partial();
        setError(result, "avio_open failed");
        return false;
    }
    if (avformat_write_header(mux.get(), nullptr) < 0) {
        cleanup_partial();
        setError(result, "avformat_write_header failed");
        return false;
    }

    pipeline_detail::BoundedQueue render_queue(config.pool_capacity);
    pipeline_detail::BoundedQueue encode_queue(config.pool_capacity);
    const pipeline_detail::StageResult stages = pipeline_detail::runStages(
        pipeline_detail::StageConfig{
            &demuxer,
            input_stream,
            video_index,
            decoder_context.get(),
            encoder_context.get(),
            output_stream,
            mux.get(),
            &pool,
            &render_queue,
            &encode_queue,
            transform_bypass,
            src_height,
            source_start_us,
            source_end_us,
            stream_start_us,
            config.frame_graph,
        });

    if (!stages.success) {
        avio_closep(&mux->pb);
        cleanup_partial();
        setError(result, stages.error);
        return false;
    }

    if (stages.frames_encoded == 0) {
        av_write_trailer(mux.get());
        avio_closep(&mux->pb);
        cleanup_partial();
        setError(result, "frame pipeline encoded zero frames");
        return false;
    }
    if (av_write_trailer(mux.get()) < 0) {
        avio_closep(&mux->pb);
        cleanup_partial();
        setError(result, "av_write_trailer failed");
        return false;
    }
    avio_closep(&mux->pb);

    bool durable = false;
    if (!pipeline_detail::publishProbedOutput(
            partial, config.output_path, error, &durable)) {
        cleanup_partial();
        setError(result, error);
        return false;
    }

    result->success = true;
    result->output_durable = durable;
    result->frames_decoded = stages.frames_decoded;
    result->frames_encoded = stages.frames_encoded;
    result->zero_copy_decoded_frames = stages.frames_decoded;
    result->transform_bypass_frames = stages.transform_bypass_frames;
    result->peak_pool_usage = stages.peak_pool_usage;
    result->peak_render_queue = stages.peak_render_queue;
    result->peak_encode_queue = stages.peak_encode_queue;
    result->pipeline_metrics = stages.metrics;
    return true;
}

} // namespace velox::media

#else // !VELOX_ENABLE_LIBAV

#include "velox/services/frame_pipeline.hpp"

namespace velox::media {

bool renderFrames(const FramePipelineConfig&, FramePipelineResult* result) {
    if (result != nullptr) {
        result->success = false;
        result->error = "frame pipeline requires VELOX_ENABLE_LIBAV=ON";
    }
    return false;
}

} // namespace velox::media

#endif // VELOX_ENABLE_LIBAV
