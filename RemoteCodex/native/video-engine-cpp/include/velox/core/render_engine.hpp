#pragma once
#include "velox/core/metrics.hpp"
#include "velox/plan/render_plan.hpp"
#include "velox/services/ffmpeg_progress_parser.hpp"
#include "velox/services/frame_pipeline.hpp"
#include "velox/services/media_packet_pipeline.hpp"
#include "velox/services/segment_execution.hpp"
#include "velox/telemetry/emitter.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>
#include <utility>
#include <vector>

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
    // Segment accounting for the zero-render release gate (plan §16):
    // copySegments() is the number of video segments stream-copied into the
    // packet mux; transcodeSegments() is the number that were re-encoded
    // (always 0 on the copy-only/mixed packet paths — any nonzero value is a
    // hidden-transcode regression).
    int64_t copySegments() const { return copy_segments_.load(); }
    int64_t transcodeSegments() const { return transcode_segments_.load(); }

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
    void resetRenderState();
    bool createRenderWorkDir(const std::filesystem::path& base,
                             std::filesystem::path& work_dir,
                             std::string& error);
    std::string publishRenderOutput(const std::filesystem::path& partial,
                                    const std::filesystem::path& output);
    bool concatenateTimelineSegments(
        const std::vector<std::filesystem::path>& segment_paths,
        const std::filesystem::path& video_only,
        const std::filesystem::path& work_dir,
        RenderResult& result,
        const std::function<RenderResult(const std::string&)>& fail_render);
    std::vector<std::pair<std::filesystem::path, const plan::AudioTrack*>>
    downloadAudioTracks(const plan::RenderPlan& plan,
                        const std::filesystem::path& work_dir);
    bool finalizeAudioTracks(
        const plan::RenderPlan& plan,
        const std::filesystem::path& work_dir,
        const std::filesystem::path& out_path,
        const std::filesystem::path& video_for_mux,
        const std::chrono::steady_clock::time_point& render_start,
        const std::function<std::string(const std::filesystem::path&)>& publish_output,
        const std::function<void(const std::filesystem::path&)>& track_partial,
        RenderResult& result,
        const std::function<RenderResult(const std::string&)>& fail_render);
    bool renderLegacyTimeline(
        const plan::RenderPlan& plan,
        const std::filesystem::path& work_dir,
        const std::chrono::steady_clock::time_point& render_start,
        std::vector<std::filesystem::path>& segment_paths,
        double& total_duration_seconds,
        RenderResult& result,
        const std::function<RenderResult(const std::string&)>& fail_render);


    // Subtitle burn stage: download the first subtitle track and burn it into
    // the concatenated video. When the plan carries no subtitle track,
    // videoForMux is left as the untouched input. Returns false after setting
    // result.error and invoking failRender; the caller must return result.
    // On success videoForMux points to the file the mux stage should consume.
    bool burnSubtitles(
        const plan::RenderPlan& plan,
        const std::filesystem::path& workDir,
        const std::filesystem::path& outPath,
        const std::filesystem::path& videoOnly,
        const std::chrono::steady_clock::time_point& renderStart,
        RenderResult& result,
        const std::function<RenderResult(const std::string&)>& failRender,
        const std::function<void(const std::filesystem::path&)>& trackPartial,
        std::filesystem::path& videoForMux);

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

    // Mixed renderer: the copy-only assembly path. Resolve each video
    // segment independently against the canonical output profile; only
    // PACKET_COPY segments are accepted and assembled through the single
    // packet mux. Any Reject decision (transform, legacy feature,
    // non-keyframe-safe trim, incompatible media signature) fails the job
    // deterministically via failRender — the mixed path never re-encodes and
    // never falls back to the legacy loop.
    RenderResult renderMixed(
        const plan::RenderPlan& plan,
        const std::filesystem::path& workDir,
        const std::filesystem::path& outPath,
        RenderResult& result,
        const std::function<RenderResult(const std::string&)>& failRender);

    // Mixed renderer helper: stages the single final audio track and enforces
    // the FINAL_AUDIO_COPY contract (audio stays copy-only; anything that
    // would need a re-encode fails closed). Returns false on failure with
    // result.error and error_code populated for failRender.
    bool resolveMixedFinalAudio(
        const plan::RenderPlan& plan,
        const std::filesystem::path& workDir,
        double total_duration,
        media::CopyOnlyMuxRequest& request,
        RenderResult& result,
        std::string& error_code);

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
    std::atomic<int64_t> copy_segments_{0};
    std::atomic<int64_t> transcode_segments_{0};
    std::atomic<double> duration_seconds_{0.0};
    std::atomic<bool> output_durable_{false};
    std::atomic<int64_t> trailer_to_publish_us_{0};
    std::string concat_mode_{"reencode"};
    services::EngineProgress last_progress_{};
    EngineMetrics metrics_;
    telemetry::PhaseRecorder recorder_;
    media::FramePipelineMetrics frame_pipeline_metrics_{};
    int64_t frame_pipeline_runs_{0};
};

} // namespace velox::core
