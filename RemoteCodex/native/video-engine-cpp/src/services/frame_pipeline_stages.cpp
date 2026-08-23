#ifdef VELOX_ENABLE_LIBAV

#include "frame_pipeline_stages.hpp"

#include "frame_pipeline_compositor.hpp"
#include "frame_pipeline_decoder.hpp"
#include "frame_pipeline_encoder.hpp"
#include "frame_pipeline_filter.hpp"

extern "C" {
#include <libavcodec/avcodec.h>
}

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace velox::media::pipeline_detail {

StageResult runStages(const StageConfig& config) {
    StageResult result;
    if (config.demuxer == nullptr || config.input_stream == nullptr ||
        config.decoder == nullptr || config.encoder == nullptr ||
        config.output_stream == nullptr || config.muxer == nullptr ||
        config.pool == nullptr || config.render_queue == nullptr ||
        config.encode_queue == nullptr || config.video_index < 0) {
        result.error = "frame pipeline stage configuration is incomplete";
        return result;
    }

    BoundedQueue& render_queue = *config.render_queue;
    BoundedQueue& encode_queue = *config.encode_queue;
    FramePool& pool = *config.pool;

    std::atomic<bool> failed{false};
    std::mutex error_mutex;
    std::string stage_error;
    std::atomic<int64_t> decoded_frames{0};
    std::atomic<int64_t> encoded_packets{0};
    std::atomic<int64_t> bypass_frames{0};
    std::atomic<int64_t> producer_busy_ns{0};
    std::atomic<int64_t> consumer_elapsed_ns{0};
    std::atomic<bool> source_window_complete{false};

    const auto fail_stage = [&](const std::string& message) {
        std::lock_guard<std::mutex> lock(error_mutex);
        if (!failed.exchange(true)) {
            stage_error = message;
        }
        render_queue.shutdown();
        encode_queue.shutdown();
        pool.shutdown();
    };

    FilterChain filter_chain;
    std::string stage_error_detail;
    if (!filter_chain.init(FilterBackend::Cpu, *config.decoder, *config.encoder,
                           pool, stage_error_detail)) {
        result.error = stage_error_detail;
        return result;
    }
    CompositorStage compositor(CompositorBackend::Cpu);
    EncoderStage encoder_stage(EncoderStageConfig{
        config.encoder, config.output_stream, config.muxer, &encoded_packets});
    DecoderStage decoder_stage(DecoderStageConfig{
        config.demuxer,
        config.input_stream,
        config.video_index,
        config.decoder,
        config.pool,
        config.render_queue,
        config.source_start_us,
        config.source_end_us,
        config.stream_start_us,
        &decoded_frames,
        &source_window_complete,
    });

    std::unique_ptr<AVPacket, void (*)(AVPacket*)> input_packet(
        av_packet_alloc(), [](AVPacket* packet) { av_packet_free(&packet); });
    if (!input_packet) {
        result.error = "av_packet_alloc failed";
        return result;
    }

    std::thread decode_thread([&]() {
        bool eof = false;
        std::string read_error;
        while (!failed.load()) {
            if (!eof) {
                if (!config.demuxer->readFrame(*input_packet, eof, read_error)) {
                    fail_stage("demux read failed: " + read_error);
                    break;
                }
                if (!eof) {
                    if (input_packet->stream_index != config.video_index) {
                        av_packet_unref(input_packet.get());
                        continue;
                    }
                    std::string decoder_error;
                    if (!decoder_stage.sendPacket(input_packet.get(), decoder_error)) {
                        fail_stage(decoder_error);
                        av_packet_unref(input_packet.get());
                        break;
                    }
                    av_packet_unref(input_packet.get());
                }
            }
            if (source_window_complete.load() || eof) {
                if (eof && !failed.load()) {
                    std::string decoder_error;
                    if (!decoder_stage.flush(decoder_error)) {
                        fail_stage(decoder_error);
                    }
                }
                break;
            }
        }
        if (!failed.load()) {
            render_queue.push(-1);
        }
    });

    std::thread render_thread([&]() {
        const auto thread_start = std::chrono::steady_clock::now();
        int64_t frame_index = 0;
        int index = 0;
        std::string render_error;
        while (!failed.load() && render_queue.pop(index)) {
            if (index < 0) {
                encode_queue.push(-1);
                break;
            }
            AVFrame* source = pool.decoded(index);
            int64_t frame_cpu_busy_ns = 0;
            AVFrame* rendered = filter_chain.apply(
                source, index, pool, config.source_height,
                frame_cpu_busy_ns, render_error);
            producer_busy_ns.fetch_add(frame_cpu_busy_ns);
            if (rendered == nullptr) {
                fail_stage(render_error);
                pool.release(index);
                break;
            }
            if (filter_chain.bypass()) {
                bypass_frames.fetch_add(1);
            }
            if (!compositor.apply(rendered, frame_index, config.frame_graph, render_error)) {
                fail_stage("frame graph apply failed: " + render_error);
                pool.release(index);
                break;
            }
            rendered->pts = frame_index++;
            rendered->pict_type = AV_PICTURE_TYPE_NONE;
            if (!encode_queue.push(index)) {
                pool.release(index);
                break;
            }
        }
        (void)thread_start;
    });

    std::thread encode_thread([&]() {
        const auto thread_start = std::chrono::steady_clock::now();
        int index = 0;
        while (!failed.load() && encode_queue.pop(index)) {
            if (index < 0) {
                break;
            }
            AVFrame* frame = filter_chain.bypass()
                ? pool.decoded(index)
                : pool.scaled(index);
            std::string encoder_error;
            if (!encoder_stage.sendFrame(frame, encoder_error)) {
                fail_stage(encoder_error);
                pool.release(index);
                break;
            }
            pool.release(index);
        }
        if (!failed.load()) {
            std::string encoder_error;
            if (!encoder_stage.flush(encoder_error)) {
                fail_stage(encoder_error);
            }
        }
        consumer_elapsed_ns.store(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - thread_start).count());
    });

    decode_thread.join();
    render_thread.join();
    encode_thread.join();

    if (failed.load()) {
        result.error = stage_error;
        return result;
    }

    result.success = true;
    result.frames_decoded = decoded_frames.load();
    result.frames_encoded = encoded_packets.load();
    result.transform_bypass_frames = bypass_frames.load();
    result.peak_pool_usage = pool.peakUsage();
    result.peak_render_queue = render_queue.highWater();
    result.peak_encode_queue = encode_queue.highWater();

    const int64_t producer_busy_ns_value = producer_busy_ns.load();
    const int64_t producer_wait_ns =
        render_queue.emptyWaitNs() + encode_queue.fullWaitNs();
    const int64_t consumer_wait_ns = encode_queue.emptyWaitNs();
    const int64_t consumer_elapsed_ns_value = consumer_elapsed_ns.load();
    int64_t consumer_busy_ns = consumer_elapsed_ns_value - consumer_wait_ns;
    if (consumer_busy_ns < 0) {
        consumer_busy_ns = 0;
    }
    const auto safe_ratio = [](int64_t numerator, int64_t denominator) -> double {
        if (denominator <= 0) {
            return 0.0;
        }
        const double value = static_cast<double>(numerator) /
                             static_cast<double>(denominator);
        return value > 1.0 ? 1.0 : value;
    };
    const int64_t producer_total_ns = producer_busy_ns_value + producer_wait_ns;
    const int64_t consumer_total_ns = consumer_busy_ns + consumer_wait_ns;
    const int64_t producer_busy_ms =
        (producer_busy_ns_value + 500'000) / 1'000'000;
    const int64_t producer_wait_ms =
        render_queue.emptyWaitMs() + encode_queue.fullWaitMs();
    const int64_t consumer_wait_ms = encode_queue.emptyWaitMs();
    const int64_t consumer_elapsed_ms =
        (consumer_elapsed_ns_value + 500'000) / 1'000'000;
    int64_t consumer_busy_ms = consumer_elapsed_ms - consumer_wait_ms;
    if (consumer_busy_ms < 0) {
        consumer_busy_ms = 0;
    }
    result.metrics = FramePipelineMetrics{
        producer_busy_ms,
        producer_wait_ms,
        consumer_busy_ms,
        consumer_wait_ms,
        encode_queue.averageDepth(),
        encode_queue.highWater(),
        encode_queue.emptyWaitMs(),
        encode_queue.fullWaitMs(),
        safe_ratio(producer_wait_ns, producer_total_ns),
        safe_ratio(consumer_wait_ns, consumer_total_ns),
        safe_ratio(encode_queue.fullWaitNs(), producer_total_ns),
    };
    return result;
}

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
