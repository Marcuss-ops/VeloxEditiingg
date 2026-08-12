// frame_pipeline.cpp — Phase-3 in-process AVFrame producer-consumer
// pipeline for jobs that genuinely require encoding.
//
//   Decoder thread  -> BoundedQueue<frame>  -> Render thread (sws)  ->
//   BoundedQueue<frame> -> Encoder thread (single persistent AVCodecContext)
//                                                   |
//                                                   v
//                                         one MP4 muxer (atomic publish)
//
// Memory is bounded structurally: a fixed pool of pre-allocated AVFrames is
// the only storage, and both hand-off queues are capped at the pool size, so
// the producer blocks (backpressure) once the pipeline is full. Exactly one
// encoder AVCodecContext is created for the whole output — the counter is
// exposed so the invariant is observable and testable.
//
// This entry point (renderFrames / --render-frames) is an explicit opt-in
// used ONLY by jobs that need encoding. Copy-only jobs keep the zero-spawn
// packet path (muxCopyOnly); legacy jobs keep the FFmpeg CLI path.

#ifdef VELOX_ENABLE_LIBAV

#include "velox/services/frame_pipeline.hpp"

#include "velox/services/file_utils.hpp"
#include "velox/services/media_packet_components.hpp"
#include "velox/services/media_probe.hpp"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
#include <libavutil/imgutils.h>
#include <libavutil/opt.h>
#include <libswscale/swscale.h>
}

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
namespace packet = velox::media::packet;

namespace velox::media {
namespace {

struct FrameDeleter {
    void operator()(AVFrame* frame) const { av_frame_free(&frame); }
};
using UniqueFrame = std::unique_ptr<AVFrame, FrameDeleter>;

struct CodecContextDeleter {
    void operator()(AVCodecContext* context) const { avcodec_free_context(&context); }
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

struct PacketDeleter {
    void operator()(AVPacket* packet) const { av_packet_free(&packet); }
};
using UniquePacket = std::unique_ptr<AVPacket, PacketDeleter>;

struct SwsContextDeleter {
    void operator()(SwsContext* context) const {
        if (context != nullptr) {
            sws_freeContext(context);
        }
    }
};
using UniqueSwsContext = std::unique_ptr<SwsContext, SwsContextDeleter>;

std::string ffmpegErrorText(int error) {
    char buffer[AV_ERROR_MAX_STRING_SIZE]{};
    av_strerror(error, buffer, sizeof(buffer));
    return buffer;
}

// Thread-safe bounded FIFO used for both hand-off queues. push/pop block for
// backpressure; after shutdown() a drained queue reports empty (pop false).
//
// The queue also keeps producer-consumer health accounting (§25): how long
// producers waited on a FULL queue (backpressure, full_wait_ns_) and
// consumers waited on an EMPTY queue (starvation, empty_wait_ns_), plus a
// time-weighted average depth so the receipt can show how far the pipeline
// ran from empty (starvation risk) or full (backpressure risk).
class BoundedQueue {
public:
    explicit BoundedQueue(int capacity) : capacity_(capacity) {}

    bool push(int value) {
        std::unique_lock<std::mutex> lock(mutex_);
        const auto wait_start = Clock::now();
        not_full_.wait(lock, [&] {
            return done_ || static_cast<int>(items_.size()) < capacity_;
        });
        full_wait_ns_ += elapsedNs(wait_start);
        if (done_) {
            return false;
        }
        items_.push_back(value);
        high_water_ = std::max<int64_t>(high_water_,
                                        static_cast<int64_t>(items_.size()));
        sampleDepth();
        not_empty_.notify_one();
        return true;
    }

    bool pop(int& value) {
        std::unique_lock<std::mutex> lock(mutex_);
        const auto wait_start = Clock::now();
        not_empty_.wait(lock, [&] { return done_ || !items_.empty(); });
        empty_wait_ns_ += elapsedNs(wait_start);
        if (items_.empty()) {
            return false;
        }
        value = items_.front();
        items_.pop_front();
        sampleDepth();
        not_full_.notify_one();
        return true;
    }

    void shutdown() {
        std::lock_guard<std::mutex> lock(mutex_);
        sampleDepth();
        done_ = true;
        not_full_.notify_all();
        not_empty_.notify_all();
    }

    int64_t highWater() const { return high_water_; }

    // Producer wait on a full queue (backpressure), rounded to ms.
    int64_t fullWaitMs() const { return nsToMs(full_wait_ns_); }

    // Consumer wait on an empty queue (starvation), rounded to ms.
    int64_t emptyWaitMs() const { return nsToMs(empty_wait_ns_); }

    // Raw nanosecond waits. The ratios are derived from these BEFORE the
    // ms rounding so a sub-ms render cannot collapse a truthful fraction
    // into an artificial 1.0/0.0.
    int64_t fullWaitNs() const { return full_wait_ns_; }
    int64_t emptyWaitNs() const { return empty_wait_ns_; }

    // Time-weighted average depth (0 when the queue never carried items
    // for a measurable window). Rounded to the nearest integer depth.
    int64_t averageDepth() const {
        if (window_ns_ <= 0) {
            return 0;
        }
        const double avg = static_cast<double>(depth_ns_) /
                           static_cast<double>(window_ns_);
        return static_cast<int64_t>(avg + 0.5);
    }

private:
    using Clock = std::chrono::steady_clock;

    static int64_t elapsedNs(const Clock::time_point& start) {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   Clock::now() - start)
            .count();
    }

    static int64_t nsToMs(int64_t ns) {
        return (ns + 500'000) / 1'000'000;
    }

    // Accumulates (depth × elapsed) so the average is time-weighted. Must
    // be called with mutex_ held (push/pop/shutdown already hold it).
    void sampleDepth() {
        const auto now = Clock::now();
        const int64_t dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               now - last_sample_)
                               .count();
        depth_ns_ += static_cast<int64_t>(last_depth_) * dt;
        window_ns_ += dt;
        last_sample_ = now;
        last_depth_ = static_cast<int64_t>(items_.size());
    }

    int capacity_;
    std::deque<int> items_;
    mutable std::mutex mutex_;
    std::condition_variable not_full_;
    std::condition_variable not_empty_;
    bool done_{false};
    int64_t high_water_{0};

    // Health accounting (ns).
    int64_t full_wait_ns_{0};
    int64_t empty_wait_ns_{0};
    int64_t depth_ns_{0};
    int64_t window_ns_{0};
    Clock::time_point last_sample_{Clock::now()};
    int64_t last_depth_{0};
};

// Bounded AVFrame pool. `capacity` slots are pre-allocated at the output
// size/format; each slot also carries a lazily-sized decode buffer so the
// decoder thread can av_frame_copy into it. A slot is owned by exactly one
// stage at a time (decoder fills -> render scales -> encoder consumes ->
// released), so per-slot access needs no extra synchronization.
class FramePool {
public:
    bool init(int capacity, int in_width, int in_height,
              int out_width, int out_height, std::string& error) {
        if (capacity < 2) {
            error = "frame pool capacity must be >= 2";
            return false;
        }
        capacity_ = capacity;
        decoded_.resize(static_cast<size_t>(capacity));
        scaled_.resize(static_cast<size_t>(capacity));
        for (int i = 0; i < capacity; ++i) {
            decoded_[static_cast<size_t>(i)].reset(av_frame_alloc());
            scaled_[static_cast<size_t>(i)].reset(av_frame_alloc());
            if (!decoded_[static_cast<size_t>(i)] || !scaled_[static_cast<size_t>(i)]) {
                error = "av_frame_alloc failed";
                return false;
            }
            AVFrame* scaled = scaled_[static_cast<size_t>(i)].get();
            scaled->format = AV_PIX_FMT_YUV420P;
            scaled->width = out_width;
            scaled->height = out_height;
            if (av_frame_get_buffer(scaled, 32) < 0) {
                error = "av_frame_get_buffer (scaled slot) failed";
                return false;
            }
            free_.push_back(i);
        }
        in_width_ = in_width;
        in_height_ = in_height;
        return true;
    }

    // Blocks until a slot is free; returns -1 after shutdown().
    int acquire() {
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

    void release(int index) {
        std::lock_guard<std::mutex> lock(mutex_);
        free_.push_back(index);
        --in_use_;
        available_.notify_one();
    }

    void shutdown() {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_ = true;
        available_.notify_all();
    }

    int capacity() const { return capacity_; }
    int64_t peakUsage() const { return peak_usage_; }

    AVFrame* decoded(int index) { return decoded_[static_cast<size_t>(index)].get(); }
    AVFrame* scaled(int index) { return scaled_[static_cast<size_t>(index)].get(); }

    // Lazily allocates (or reallocates) the slot's decode buffer to match
    // the source geometry/format.
    bool ensureDecodedBuffer(int index, int width, int height,
                             AVPixelFormat format, std::string& error) {
        AVFrame* frame = decoded_[static_cast<size_t>(index)].get();
        if (frame->data[0] == nullptr || frame->width != width ||
            frame->height != height || frame->format != format) {
            av_frame_unref(frame);
            frame->format = format;
            frame->width = width;
            frame->height = height;
            if (av_frame_get_buffer(frame, 32) < 0) {
                error = "av_frame_get_buffer (decode slot) failed";
                return false;
            }
        }
        return true;
    }

private:
    int capacity_{0};
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

bool publishProbedOutput(const fs::path& partial, const fs::path& target,
                         std::string& error, bool* durableOut) {
    // The pipeline writes the whole output before publishing it; a final
    // in-process probe guarantees the artifact is a probeable MP4 with a
    // video stream before any rename happens.
    const auto finalProbe = probeMediaInProcess(partial);
    bool hasVideo = false;
    if (finalProbe.has_value()) {
        for (const auto& stream : finalProbe->streams) {
            hasVideo = hasVideo || stream.is_video;
        }
    }
    if (!finalProbe.has_value() || !hasVideo) {
        error = "frame pipeline output probe failed (no video stream)";
        return false;
    }
    bool durable = false;
    if (!file::publishAtomic(partial, target, &error, &durable)) {
        return false;
    }
    if (durableOut != nullptr) {
        *durableOut = durable;
    }
    return true;
}

} // namespace

bool renderFrames(const FramePipelineConfig& config, FramePipelineResult* result) {
    FramePipelineResult local;
    if (result == nullptr) {
        result = &local;
    }
    *result = FramePipelineResult{};

    if (config.input_path.empty() || config.output_path.empty()) {
        result->error = "frame pipeline requires input and output paths";
        return false;
    }
    std::error_code statError;
    if (!fs::is_regular_file(config.input_path, statError)) {
        result->error = "frame pipeline input is not a regular file";
        return false;
    }
    if (config.pool_capacity < 2 || config.pool_capacity > 64) {
        result->error = "frame pipeline pool_capacity must be in [2, 64]";
        return false;
    }
    if (config.fps_num <= 0 || config.fps_den <= 0) {
        result->error = "frame pipeline requires positive fps_num/fps_den";
        return false;
    }
    if (config.codec.empty()) {
        result->error = "frame pipeline requires a codec name";
        return false;
    }

    fs::path parent = config.output_path.parent_path();
    std::error_code ec;
    if (parent.empty()) {
        parent = fs::current_path(ec);
    }
    if (ec || parent.empty()) {
        result->error = "frame pipeline cannot resolve output directory";
        return false;
    }
    fs::create_directories(parent, ec);
    if (ec) {
        result->error = "frame pipeline cannot create output directory: " + ec.message();
        return false;
    }
    const fs::path partial = file::makePartialPath(config.output_path);
    auto cleanupPartial = [&]() {
        std::error_code remove_error;
        fs::remove(partial, remove_error);
    };

    // ── Input demux + decode ───────────────────────────────────────────
    packet::Demuxer demuxer;
    std::string error;
    if (!demuxer.open(config.input_path, error)) {
        result->error = "frame pipeline open input: " + error;
        return false;
    }
    const int video_index = demuxer.firstStream(AVMEDIA_TYPE_VIDEO);
    if (video_index < 0) {
        result->error = "frame pipeline input has no video stream";
        return false;
    }
    const AVStream* input_stream = demuxer.stream(video_index);

    if (config.source_in_us < 0 || config.source_duration_us < 0 ||
        (config.source_in_us > 0 &&
         config.source_duration_us > std::numeric_limits<int64_t>::max() -
             config.source_in_us)) {
        result->error = "frame pipeline source window is invalid";
        return false;
    }
    const int64_t source_start_us = config.source_in_us;
    const int64_t source_end_us = config.source_duration_us > 0
        ? config.source_in_us + config.source_duration_us
        : 0;

    const AVCodec* decoder = avcodec_find_decoder(input_stream->codecpar->codec_id);
    if (decoder == nullptr) {
        result->error = "frame pipeline has no decoder for the input codec";
        return false;
    }
    UniqueCodecContext dec_ctx(avcodec_alloc_context3(decoder));
    if (!dec_ctx) {
        result->error = "avcodec_alloc_context3 (decoder) failed";
        return false;
    }
    if (avcodec_parameters_to_context(dec_ctx.get(), input_stream->codecpar) < 0) {
        result->error = "avcodec_parameters_to_context (decoder) failed";
        return false;
    }
    if (avcodec_open2(dec_ctx.get(), decoder, nullptr) < 0) {
        result->error = "avcodec_open2 (decoder) failed";
        return false;
    }
    result->decoder_contexts_created = 1;

    const int src_width = dec_ctx->width;
    const int src_height = dec_ctx->height;
    if (src_width <= 0 || src_height <= 0) {
        result->error = "frame pipeline input has invalid dimensions";
        return false;
    }
    const int out_width = config.width > 0 ? config.width : src_width;
    const int out_height = config.height > 0 ? config.height : src_height;
    if (out_width <= 0 || out_height <= 0) {
        result->error = "frame pipeline output dimensions are invalid";
        return false;
    }

    // ── Bounded pool + render scaler ───────────────────────────────────
    FramePool pool;
    if (!pool.init(config.pool_capacity, src_width, src_height,
                   out_width, out_height, error)) {
        result->error = error;
        return false;
    }
    UniqueSwsContext sws(sws_getContext(
        src_width, src_height, dec_ctx->pix_fmt,
        out_width, out_height, AV_PIX_FMT_YUV420P,
        SWS_BILINEAR, nullptr, nullptr, nullptr));
    if (!sws) {
        result->error = "sws_getContext failed";
        return false;
    }

    // ── Encoder: exactly one persistent AVCodecContext ─────────────────
    const AVCodec* encoder = avcodec_find_encoder_by_name(config.codec.c_str());
    if (encoder == nullptr) {
        result->error = "frame pipeline encoder not found: " + config.codec;
        return false;
    }
    UniqueCodecContext enc_ctx(avcodec_alloc_context3(encoder));
    if (!enc_ctx) {
        result->error = "avcodec_alloc_context3 (encoder) failed";
        return false;
    }
    enc_ctx->width = out_width;
    enc_ctx->height = out_height;
    enc_ctx->pix_fmt = AV_PIX_FMT_YUV420P;
    if (encoder->pix_fmts != nullptr && encoder->pix_fmts[0] != AV_PIX_FMT_NONE) {
        enc_ctx->pix_fmt = encoder->pix_fmts[0];
    }
    enc_ctx->time_base = AVRational{config.fps_den, config.fps_num};
    enc_ctx->framerate = AVRational{config.fps_num, config.fps_den};
    enc_ctx->gop_size = std::max(1, (config.fps_num / config.fps_den) * 2);
    enc_ctx->max_b_frames = 0;
    enc_ctx->bit_rate = 0;
    if (config.codec == "libx264") {
        if (!config.preset.empty()) {
            av_opt_set(enc_ctx->priv_data, "preset", config.preset.c_str(), 0);
        }
        av_opt_set_int(enc_ctx->priv_data, "crf", 23, 0);
    }
    if (avcodec_open2(enc_ctx.get(), encoder, nullptr) < 0) {
        result->error = "avcodec_open2 (encoder) failed for " + config.codec;
        return false;
    }
    result->encode_contexts_created = 1;

    // ── Muxer (writes the partial beside the target) ───────────────────
    AVFormatContext* raw_mux = nullptr;
    if (avformat_alloc_output_context2(&raw_mux, nullptr, "mp4", partial.c_str()) < 0 ||
        raw_mux == nullptr) {
        cleanupPartial();
        result->error = "avformat_alloc_output_context2 failed";
        return false;
    }
    UniqueOutputContext mux(raw_mux);
    AVStream* out_stream = avformat_new_stream(mux.get(), nullptr);
    if (out_stream == nullptr) {
        cleanupPartial();
        result->error = "avformat_new_stream failed";
        return false;
    }
    if (avcodec_parameters_from_context(out_stream->codecpar, enc_ctx.get()) < 0) {
        cleanupPartial();
        result->error = "avcodec_parameters_from_context failed";
        return false;
    }
    out_stream->time_base = enc_ctx->time_base;
    out_stream->avg_frame_rate = AVRational{config.fps_num, config.fps_den};
    if (avio_open(&mux->pb, partial.c_str(), AVIO_FLAG_WRITE) < 0) {
        cleanupPartial();
        result->error = "avio_open failed";
        return false;
    }
    if (avformat_write_header(mux.get(), nullptr) < 0) {
        cleanupPartial();
        result->error = "avformat_write_header failed";
        return false;
    }

    // ── Producer-consumer stages ───────────────────────────────────────
    BoundedQueue render_queue(config.pool_capacity);
    BoundedQueue encode_queue(config.pool_capacity);
    std::atomic<bool> failed{false};
    std::mutex error_mutex;
    std::string stage_error;
    std::atomic<int64_t> decoded_frames{0};
    std::atomic<int64_t> encoded_packets{0};
    std::atomic<bool> source_window_complete{false};

    const auto failStage = [&](const std::string& message) {
        std::lock_guard<std::mutex> lock(error_mutex);
        if (!failed.exchange(true)) {
            stage_error = message;
        }
        render_queue.shutdown();
        encode_queue.shutdown();
        pool.shutdown();
    };

    UniquePacket input_packet(av_packet_alloc());
    UniqueFrame scratch(av_frame_alloc());
    if (!input_packet || !scratch) {
        cleanupPartial();
        result->error = "av_packet_alloc / av_frame_alloc failed";
        return false;
    }

    const auto drainDecoded = [&]() -> bool {
        while (true) {
            const int rc = avcodec_receive_frame(dec_ctx.get(), scratch.get());
            if (rc == AVERROR(EAGAIN) || rc == AVERROR_EOF) {
                return true;
            }
            if (rc < 0) {
                failStage("avcodec_receive_frame failed: " + ffmpegErrorText(rc));
                av_frame_unref(scratch.get());
                return false;
            }
            if (source_start_us > 0 || source_end_us > 0) {
                const int64_t frame_timestamp = scratch->best_effort_timestamp;
                if (frame_timestamp != AV_NOPTS_VALUE) {
                    const int64_t frame_us = av_rescale_q(
                        frame_timestamp, input_stream->time_base,
                        AVRational{1, 1'000'000});
                    if (frame_us < source_start_us) {
                        av_frame_unref(scratch.get());
                        continue;
                    }
                    if (source_end_us > 0 && frame_us >= source_end_us) {
                        source_window_complete.store(true);
                        av_frame_unref(scratch.get());
                        return true;
                    }
                }
            }
            const int index = pool.acquire();
            if (index < 0) {
                av_frame_unref(scratch.get());
                return false;
            }
            if (!pool.ensureDecodedBuffer(index, src_width, src_height,
                                          dec_ctx->pix_fmt, error)) {
                failStage(error);
                pool.release(index);
                av_frame_unref(scratch.get());
                return false;
            }
            AVFrame* slot = pool.decoded(index);
            if (av_frame_copy_props(slot, scratch.get()) < 0 ||
                av_frame_copy(slot, scratch.get()) < 0) {
                failStage("av_frame_copy failed");
                pool.release(index);
                av_frame_unref(scratch.get());
                return false;
            }
            av_frame_unref(scratch.get());
            decoded_frames.fetch_add(1);
            if (!render_queue.push(index)) {
                pool.release(index);
                return false;
            }
        }
    };

    std::thread decode_thread([&]() {
        bool eof = false;
        while (!failed.load()) {
            if (!eof) {
                if (!demuxer.readFrame(*input_packet, eof, error)) {
                    failStage("demux read failed: " + error);
                    break;
                }
                if (!eof) {
                    if (input_packet->stream_index != video_index) {
                        av_packet_unref(input_packet.get());
                        continue;
                    }
                    if (avcodec_send_packet(dec_ctx.get(), input_packet.get()) < 0) {
                        failStage("avcodec_send_packet failed");
                        av_packet_unref(input_packet.get());
                        break;
                    }
                    av_packet_unref(input_packet.get());
                }
            }
            if (!drainDecoded()) {
                break;
            }
            if (source_window_complete.load()) {
                break;
            }
            if (eof) {
                avcodec_send_packet(dec_ctx.get(), nullptr);
                if (!drainDecoded()) {
                    break;
                }
                break;
            }
        }
        if (!failed.load()) {
            render_queue.push(-1);
        }
    });

    // §25 producer-consumer health accounting. The render thread is the
    // Render Producer (scales + pushes the encoder queue); the encode
    // thread is the Encoder Consumer. busy/wait are wall-clock ms; the
    // ratios below are derived once at the end.
    std::atomic<int64_t> producer_busy_ns{0};
    std::atomic<int64_t> producer_elapsed_ns{0};
    std::atomic<int64_t> consumer_elapsed_ns{0};

    std::thread render_thread([&]() {
        const auto thread_start = std::chrono::steady_clock::now();
        int64_t frame_index = 0;
        int index = 0;
        while (!failed.load() && render_queue.pop(index)) {
            if (index < 0) {
                encode_queue.push(-1);
                break;
            }
            AVFrame* src = pool.decoded(index);
            AVFrame* dst = pool.scaled(index);
            const auto scale_start = std::chrono::steady_clock::now();
            sws_scale(sws.get(), src->data, src->linesize, 0, src_height,
                      dst->data, dst->linesize);
            producer_busy_ns.fetch_add(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - scale_start)
                    .count());
            dst->pts = frame_index++;
            dst->pict_type = AV_PICTURE_TYPE_NONE;
            if (!encode_queue.push(index)) {
                pool.release(index);
                break;
            }
        }
        producer_elapsed_ns.store(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - thread_start)
                .count());
    });

    UniquePacket out_packet(av_packet_alloc());
    const auto drainEncoded = [&]() -> bool {
        while (true) {
            const int rc = avcodec_receive_packet(enc_ctx.get(), out_packet.get());
            if (rc == AVERROR(EAGAIN) || rc == AVERROR_EOF) {
                return true;
            }
            if (rc < 0) {
                failStage("avcodec_receive_packet failed: " + ffmpegErrorText(rc));
                return false;
            }
            out_packet->stream_index = 0;
            out_packet->time_base = enc_ctx->time_base;
            if (av_interleaved_write_frame(mux.get(), out_packet.get()) < 0) {
                failStage("av_interleaved_write_frame failed");
                av_packet_unref(out_packet.get());
                return false;
            }
            av_packet_unref(out_packet.get());
            encoded_packets.fetch_add(1);
        }
    };

    std::thread encode_thread([&]() {
        const auto thread_start = std::chrono::steady_clock::now();
        int index = 0;
        while (!failed.load() && encode_queue.pop(index)) {
            if (index < 0) {
                break;
            }
            if (avcodec_send_frame(enc_ctx.get(), pool.scaled(index)) < 0) {
                failStage("avcodec_send_frame failed");
                pool.release(index);
                break;
            }
            if (!drainEncoded()) {
                pool.release(index);
                break;
            }
            pool.release(index);
        }
        consumer_elapsed_ns.store(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - thread_start)
                .count());
    });

    decode_thread.join();
    render_thread.join();
    encode_thread.join();

    // Flush the persistent encoder after every frame has been sent.
    if (!failed.load()) {
        avcodec_send_frame(enc_ctx.get(), nullptr);
        drainEncoded();
    }

    const bool stageOk = !failed.load();
    if (stageOk) {
        if (av_write_trailer(mux.get()) < 0) {
            failStage("av_write_trailer failed");
        }
    }
    avio_closep(&mux->pb);

    if (!failed.load() && encoded_packets.load() == 0) {
        // An input that decoded no frames must never publish an empty
        // artifact as success.
        cleanupPartial();
        result->error = "frame pipeline encoded zero frames";
        return false;
    }

    if (!failed.load()) {
        bool durable = false;
        if (!publishProbedOutput(partial, config.output_path, error, &durable)) {
            cleanupPartial();
            result->error = error;
            return false;
        }
        result->output_durable = durable;
    } else {
        cleanupPartial();
        result->error = stage_error;
        return false;
    }

    result->success = true;
    result->frames_decoded = decoded_frames.load();
    result->frames_encoded = encoded_packets.load();
    result->peak_pool_usage = pool.peakUsage();
    result->peak_render_queue = render_queue.highWater();
    result->peak_encode_queue = encode_queue.highWater();

    // ── §25 producer-consumer health metrics ─────────────────────────
    // The render thread is the producer, the encode thread the consumer;
    // the encode queue is the single bounded hand-off queue between them.
    // Producer wait = blocked on an empty render queue (decoder behind)
    // OR on a full encode queue (backpressure). Consumer wait = blocked
    // on an empty encode queue (encoder starvation). The ratios are
    // derived from the RAW nanosecond waits BEFORE ms rounding so a
    // sub-ms render cannot collapse a truthful fraction into an
    // artificial 1.0/0.0.
    const int64_t producer_busy_ns_ = producer_busy_ns.load();
    const int64_t producer_wait_ns =
        render_queue.emptyWaitNs() + encode_queue.fullWaitNs();
    const int64_t consumer_wait_ns = encode_queue.emptyWaitNs();
    const int64_t consumer_elapsed_ns_ = consumer_elapsed_ns.load();
    int64_t consumer_busy_ns = consumer_elapsed_ns_ - consumer_wait_ns;
    if (consumer_busy_ns < 0) {
        consumer_busy_ns = 0;
    }

    auto safeRatio = [](int64_t numerator, int64_t denominator) -> double {
        if (denominator <= 0) {
            return 0.0;
        }
        const double value = static_cast<double>(numerator) /
                             static_cast<double>(denominator);
        return value > 1.0 ? 1.0 : value;
    };
    const int64_t producer_total_ns = producer_busy_ns_ + producer_wait_ns;
    const int64_t consumer_total_ns = consumer_busy_ns + consumer_wait_ns;
    const int64_t producer_busy_ms =
        (producer_busy_ns_ + 500'000) / 1'000'000;
    const int64_t producer_wait_ms =
        render_queue.emptyWaitMs() + encode_queue.fullWaitMs();
    const int64_t consumer_wait_ms = encode_queue.emptyWaitMs();
    const int64_t consumer_elapsed_ms =
        (consumer_elapsed_ns_ + 500'000) / 1'000'000;
    int64_t consumer_busy_ms = consumer_elapsed_ms - consumer_wait_ms;
    if (consumer_busy_ms < 0) {
        consumer_busy_ms = 0;
    }

    result->pipeline_metrics = FramePipelineMetrics{
        producer_busy_ms,
        producer_wait_ms,
        consumer_busy_ms,
        consumer_wait_ms,
        encode_queue.averageDepth(),
        encode_queue.highWater(),
        encode_queue.emptyWaitMs(),
        encode_queue.fullWaitMs(),
        safeRatio(producer_wait_ns, producer_total_ns),
        safeRatio(consumer_wait_ns, consumer_total_ns),
        safeRatio(encode_queue.fullWaitNs(), producer_total_ns),
    };
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
