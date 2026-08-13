#pragma once
#include "velox/core/metrics.hpp"
#include "velox/plan/render_plan.hpp"
#include "velox/services/ffmpeg_progress_parser.hpp"
#include "velox/services/frame_pipeline.hpp"
#include "velox/telemetry/emitter.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <utility>

namespace velox::core {

struct RenderResult {
    bool success{false};
    std::string error;
    std::string output_path;
};

// RenderEngine — runs a RenderPlan to completion.
//
// F5 surface:
//   - setProgressCallback(cb) wires a per-block progress reporter
//     from the FFmpeg child process. The callback is invoked at the
//     end of every parsed "block", which is once per ~1s of encode
//     progress. The callback runs synchronously on the rendering
//     thread; pass a thread-safe functor if you fanout.
//   - The engine also accumulates typed counters (framesEncoded,
//     encodePasses, tempBytesWritten, durationSeconds) on the
//     render thread, exposed via the atomic getters below. The
//     final block's values populate the <output>.progress.json
//     sidecar, which worker-agent-go reads back post-hoc.
class RenderEngine {
public:
    RenderEngine() = default;

    // Set the FFmpeg progress reporter. Pass nullptr to detach.
    void setProgressCallback(services::ProgressCallback cb);

    // Counter accessors (thread-safe; relaxed ordering is sufficient
    // because the only observer is the sidecar writer at finalize).
    int64_t framesEncoded() const { return frames_encoded_.load(); }
    int64_t framesDecoded() const { return frames_decoded_.load(); }
    int64_t framesComposited() const { return frames_composited_.load(); }
    int64_t encodePasses() const { return encode_passes_.load(); }
    int64_t tempBytesWritten() const { return temp_bytes_written_.load(); }
    double durationSeconds() const { return duration_seconds_.load(); }
    const std::string& concatMode() const { return concat_mode_; }

    // Last FFmpeg progress snapshot from the most recent encode pass.
    // Used by the sidecar writer to populate fps / speed / frame / time.
    services::EngineProgress lastEncodeProgress() const { return last_progress_; }

    // Phase-level metric accumulator. Reset on every render() call;
    // snapshots are read by emitSidecar().
    EngineMetrics& metrics() { return metrics_; }
    const EngineMetrics& metrics() const { return metrics_; }

    // Block-1 event recorder. Reset on every render() call; the drained
    // snapshot populates the sidecar `phases[]` array. Exposed so the
    // caller can pre-register events or attach identity metadata before
    // render() runs.
    telemetry::PhaseRecorder& recorder() { return recorder_; }
    const telemetry::PhaseRecorder& recorder() const { return recorder_; }

    // Esegue il rendering completo del RenderPlan dato
    RenderResult render(const plan::RenderPlan& plan);

    // Builds the complete progress sidecar JSON without writing it. This
    // keeps the wire-format serializer directly testable and lets callers
    // inspect phase_ms, segments[] and phases[] as one payload.
    std::string sidecarJson(const std::string& output_path) const;

private:
    void emitSidecar(const std::string& output_path) const;
    void recordFramePipeline(const media::FramePipelineResult& result);

    // Copy-only packet path: a strict zero-spawn contract. It stages video
    // sources in place, resolves a single FINAL_AUDIO_COPY track, and muxes
    // the final MP4 directly through the in-process LibAV muxer with no
    // per-segment MP4s and no FFmpeg segment/concat/mux work.
    RenderResult renderCopyOnly(
        const plan::RenderPlan& plan,
        const std::filesystem::path& workDir,
        const std::filesystem::path& outPath,
        RenderResult& result,
        const std::function<RenderResult(const std::string&)>& failRender,
        const std::chrono::steady_clock::time_point& renderStart);

    // Mixed renderer: resolve each video segment independently against the
    // canonical output profile (PACKET_COPY for compatible sources,
    // NATIVE_TRANSCODE for the rest) and assemble them through the single
    // packet mux. Returns std::nullopt when the path falls back to the
    // legacy loop; otherwise the final result (success or failure).
    std::optional<RenderResult> renderMixed(
        const plan::RenderPlan& plan,
        const std::filesystem::path& workDir,
        const std::filesystem::path& outPath,
        RenderResult& result,
        const std::function<RenderResult(const std::string&)>& failRender);

    class SidecarGuard {
    public:
        SidecarGuard(const RenderEngine* engine, std::string output_path)
            : engine_(engine), output_path_(std::move(output_path)) {}
        ~SidecarGuard();

        SidecarGuard(const SidecarGuard&) = delete;
        SidecarGuard& operator=(const SidecarGuard&) = delete;

    private:
        const RenderEngine* engine_;
        std::string output_path_;
    };

    services::ProgressCallback progress_cb_;
    std::atomic<int64_t> frames_encoded_{0};
    std::atomic<int64_t> frames_decoded_{0};
    std::atomic<int64_t> frames_composited_{0};
    std::atomic<int64_t> encode_passes_{0};
    std::atomic<int64_t> temp_bytes_written_{0};
    std::atomic<double> duration_seconds_{0.0};
    std::atomic<bool> output_durable_{false};
    std::string concat_mode_{"reencode"};
    services::EngineProgress last_progress_{};
    EngineMetrics metrics_;
    telemetry::PhaseRecorder recorder_;
    media::FramePipelineMetrics frame_pipeline_metrics_{};
    int64_t frame_pipeline_runs_{0};
};

} // namespace velox::core
