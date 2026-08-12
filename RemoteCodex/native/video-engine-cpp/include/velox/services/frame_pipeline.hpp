#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

// This header is the value-types-only public surface (the OFF-build fallback
// and RenderEngine include it). The LibAV-aware producer-consumer pipeline
// behind renderFrames() lives in frame_pipeline.cpp and is compiled
// exclusively when VELOX_ENABLE_LIBAV=ON.
//
// The frame pipeline is the Phase-3 encode path: decode/render/encode run on
// three threads over a bounded pool of pre-allocated AVFrames with one
// persistent encoder AVCodecContext for the whole output. It is an explicit
// opt-in entry point for jobs that genuinely require encoding — the
// zero-spawn copy-only packet path (muxCopyOnly) and the legacy FFmpeg CLI
// path are never routed through it.
namespace velox::media {

struct FramePipelineConfig {
    std::filesystem::path input_path;
    std::filesystem::path output_path;

    // Output geometry. 0 = keep the source width/height (scaling is the
    // only render transform implemented so far).
    int width{0};
    int height{0};

    // Rational output frame rate.
    int fps_num{30};
    int fps_den{1};

    std::string codec{"libx264"};
    std::string preset{"medium"};

    // Bounded AVFrame pool capacity: the maximum number of frames in flight
    // between the decode, render and encode stages. Backpressure is
    // structural — the producer blocks when the pool is exhausted, so a
    // long source never grows memory beyond this bound.
    int pool_capacity{8};
};

// FramePipelineMetrics is the producer-consumer health report of the
// Phase-3 encode pipeline:
//
//   Decoder → FramePool → Render Producer → BoundedQueue → Encoder
//   Consumer → Muxer
//
//   producer_busy_ms  — render producer actively scaling frames (sws)
//   producer_wait_ms  — producer blocked on a hand-off queue: waiting for
//                       input from the decoder (render queue empty) or for
//                       the encoder queue to drain (backpressure)
//   consumer_busy_ms  — encoder consumer sending frames + writing packets
//   consumer_wait_ms  — consumer blocked on an EMPTY encoder queue
//                       (encoder starvation)
//   queue_depth_avg   — time-weighted average depth of the encoder queue
//   queue_depth_max   — high-water mark of the encoder queue (<= pool
//                       capacity by construction)
//   queue_empty_ms    — encoder queue empty while the consumer waited
//   queue_full_ms     — encoder queue full while the producer waited
//
//   producer_stall_ratio     = producer_wait / (busy + wait)
//   encoder_starvation_ratio = consumer_wait / (busy + wait)
//   backpressure_ratio       = queue_full / (busy + wait)  [producer]
struct FramePipelineMetrics {
    int64_t producer_busy_ms{0};
    int64_t producer_wait_ms{0};
    int64_t consumer_busy_ms{0};
    int64_t consumer_wait_ms{0};
    int64_t queue_depth_avg{0};
    int64_t queue_depth_max{0};
    int64_t queue_empty_ms{0};
    int64_t queue_full_ms{0};
    double producer_stall_ratio{0.0};
    double encoder_starvation_ratio{0.0};
    double backpressure_ratio{0.0};
};

struct FramePipelineResult {
    bool success{false};
    // True only when the output rename committed and the parent directory
    // fsync completed.
    bool output_durable{false};
    std::string error;

    int64_t frames_decoded{0};
    int64_t frames_encoded{0};

    // The pipeline must create exactly one encoder context for the whole
    // output; this counter exists so the invariant is observable and
    // testable.
    int64_t encode_contexts_created{0};
    int64_t decoder_contexts_created{0};

    // Observed high-water marks for the bounded structures.
    int64_t peak_pool_usage{0};
    int64_t peak_render_queue{0};
    int64_t peak_encode_queue{0};

    // Phase-3 producer-consumer health metrics (§25): how the decode/render
    // producer and the encoder consumer spent their time, how deep the
    // bounded hand-off queue ran, and how much of the producer's wall time
    // was backpressure (queue full) versus the consumer's starvation
    // (queue empty). All wait/busy values are wall-clock milliseconds; the
    // ratios are dimensionless floats in [0, 1]. Zero means the stage never
    // ran (or the metrics were not collected on a legacy build).
    FramePipelineMetrics pipeline_metrics;
};

// Decodes `input_path`, renders each frame (scale/pad to the output size),
// encodes with one persistent AVCodecContext and muxes into an MP4 that is
// published atomically (fsync + rename). Never spawns ffmpeg/ffprobe.
bool renderFrames(const FramePipelineConfig& config, FramePipelineResult* result = nullptr);

} // namespace velox::media
