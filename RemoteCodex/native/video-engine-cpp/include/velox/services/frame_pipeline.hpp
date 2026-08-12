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
};

// Decodes `input_path`, renders each frame (scale/pad to the output size),
// encodes with one persistent AVCodecContext and muxes into an MP4 that is
// published atomically (fsync + rename). Never spawns ffmpeg/ffprobe.
bool renderFrames(const FramePipelineConfig& config, FramePipelineResult* result = nullptr);

} // namespace velox::media
