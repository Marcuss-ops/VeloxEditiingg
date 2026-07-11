#pragma once
#include "velox/core/metrics.hpp"
#include "velox/plan/render_plan.hpp"
#include "velox/services/ffmpeg_progress_parser.hpp"

#include <atomic>
#include <cstdint>
#include <string>

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

    // Esegue il rendering completo del RenderPlan dato
    RenderResult render(const plan::RenderPlan& plan);

private:
    void emitSidecar(const std::string& output_path) const;

    services::ProgressCallback progress_cb_;
    std::atomic<int64_t> frames_encoded_{0};
    std::atomic<int64_t> encode_passes_{0};
    std::atomic<int64_t> temp_bytes_written_{0};
    std::atomic<double> duration_seconds_{0.0};
    std::string concat_mode_{"reencode"};
    services::EngineProgress last_progress_{};
    EngineMetrics metrics_;
};

} // namespace velox::core
