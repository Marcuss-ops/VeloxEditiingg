#pragma once

// phase_recorder.hpp — observability chain / block 1: C++ engine-side
// event recorder.
//
// Mirrors the worker-side Go recorder (worker-agent-go/internal/telemetry):
// a thread-safe accumulator of execution events whose drained snapshot
// populates the `phases[]` array of the <output>.progress.json sidecar.
// worker-agent-go reads the sidecar back post-hoc and maps the array
// into TaskResult.phase_timings (proto field 20) for the master's
// task_execution_events table.
//
// Clock contract (same as the Go recorder):
//   - Duration is measured on a MONOTONIC clock (steady_clock) so wall
//     jumps never distort phase durations.
//   - started_at / completed_at are ISO-8601 UTC wall stamps for
//     cross-host correlation only.
//
// Event index: each event carries a per-origin event_index incrementing
// from 0; the master guards UNIQUE(attempt_id, origin, event_index).
//
// Origin / scope are CLOSED enums mirroring the task_execution_events
// CHECK constraints; non-canonical values are coerced to engine/attempt
// so a drained stream can never be rejected by the master.
#include "velox/telemetry/catalog_generated.hpp"

#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

namespace velox::telemetry {

// Closed vocabularies are aliases to the generated language-neutral catalog;
// this header owns no second literal list.
inline constexpr const char* kOriginMaster = catalog::kOriginMaster.data();
inline constexpr const char* kOriginWorker = catalog::kOriginWorker.data();
inline constexpr const char* kOriginEngine = catalog::kOriginEngine.data();
inline constexpr const char* kOriginFFmpeg = catalog::kOriginFFmpeg.data();
inline constexpr const char* kOriginUpload = catalog::kOriginUpload.data();
inline constexpr const char* kOriginValidation = catalog::kOriginValidation.data();

inline constexpr const char* kScopeJob = catalog::kScopeJob.data();
inline constexpr const char* kScopeTask = catalog::kScopeTask.data();
inline constexpr const char* kScopeAttempt = catalog::kScopeAttempt.data();
inline constexpr const char* kScopeSegment = catalog::kScopeSegment.data();
inline constexpr const char* kScopeAudioTrack = catalog::kScopeAudioTrack.data();
inline constexpr const char* kScopeSubtitleTrack = catalog::kScopeSubtitleTrack.data();
inline constexpr const char* kScopeArtifact = catalog::kScopeArtifact.data();

// ── Status vocabulary (shared with task_phase_timings) ─────────────────────
inline constexpr const char* kStatusOk = "ok";
inline constexpr const char* kStatusFailed = "failed";

// IsCanonicalOrigin / IsCanonicalScope report membership in the closed
// enums above. Empty strings are NOT canonical.
bool IsCanonicalOrigin(const std::string& s);
bool IsCanonicalScope(const std::string& s);
// IsCanonicalEvent exposes the generated catalog lookup to C++ producers.
// PhaseRecorder itself remains compatibility-tolerant for legacy sidecar
// event names; new producers should gate component/action pairs with this
// predicate so unknown facts are detectable before recording.
bool IsCanonicalEvent(const std::string& component, const std::string& action);
// Validates a complete JSON object using the recorder's dependency-free
// parser. Used by sidecar compatibility tests and metadata guards.
bool IsValidJsonObject(const std::string& s);

// PhaseEvent is one immutable execution event. JSON-serialized into the
// sidecar `phases[]` array by the recorder's ToJson helper (or by the
// caller via PhaseEvent::AppendJson).
struct PhaseEvent {
    std::string origin;
    std::string scope;
    std::string component;
    std::string action;
    std::string phase;
    std::string event_type;   // "started" | "completed" | "failed" | "progress" | ...
    std::string event_name;   // free-form short label (e.g. "encode_segment_4")
    int64_t event_index{0};   // per-(origin) sequence number from 0
    std::string started_at;   // ISO-8601 UTC
    std::string completed_at; // ISO-8601 UTC
    int64_t duration_ms{0};
    std::string status;       // "ok" | "failed"
    std::string error_code;
    std::string error_message;
    int64_t bytes_in{0};
    int64_t bytes_out{0};
    int64_t frames{0};
    int32_t segment_index{-1};
    std::string track_kind;
    int32_t track_index{-1};
    double started_offset_ms{0};
    double finished_offset_ms{0};
    double cpu_ms{0};
    double queue_wait_ms{0};
    int64_t frames_in{0};
    int64_t frames_out{0};
    std::string metadata_json; // pre-serialized JSON object, embedded verbatim

    // AppendJson appends this event as one sidecar `phases[]` element
    // (JSON object, no surrounding brackets) to `out`.
    void AppendJson(std::ostringstream& out) const;
};

// PhaseRecorder accumulates PhaseEvent entries for one render() call.
// Thread-safe; Reset() at the top of render(), Snapshot() read by the
// sidecar writer at finalize.
class PhaseRecorder {
public:
    PhaseRecorder() = default;

    // Reset clears all events and per-origin index counters. Call at the
    // top of every render() call.
    void Reset();

    // Begin opens a timed event and returns a token for Complete/Abort.
    // Non-canonical origin/scope are coerced to engine/attempt.
    int64_t Begin(std::string origin, std::string scope, std::string component,
                  std::string action, std::string phase,
                  std::string event_type = "", std::string event_name = "");

    // Complete finalizes a token with counters and status. Safe on an
    // unknown or already-finalized token.
    void Complete(int64_t token, int64_t bytes_in, int64_t bytes_out, int64_t frames,
                  const std::string& status, const std::string& error_code = "",
                  const std::string& error_message = "");

    // Abort finalizes a token as failed. Safe on unknown tokens.
    void Abort(int64_t token, const std::string& error_code = "",
               const std::string& error_message = "");

    // SetMetadataJSON attaches a pre-serialized JSON object to an inflight
    // event. Invalid/empty metadata is ignored so the sidecar remains valid.
    void SetMetadataJSON(int64_t token, std::string metadata_json);

    // SetDetailedMetrics attaches segment/track identity, offsets and
    // resource counters to an inflight event before it is finalized.
    void SetDetailedMetrics(int64_t token, int32_t segment_index,
                            std::string track_kind, int32_t track_index,
                            double started_offset_ms, double finished_offset_ms,
                            double cpu_ms, double queue_wait_ms,
                            int64_t frames_in, int64_t frames_out);

    // Emit records a point-in-time event (no duration). Non-canonical
    // origin/scope are coerced to engine/attempt.
    void Emit(std::string origin, std::string scope, std::string component,
              std::string action, std::string phase, const std::string& status,
              const std::string& event_type = "", const std::string& error_code = "",
              const std::string& error_message = "");

    // Snapshot returns a copy of all finalized events in insertion order.
    std::vector<PhaseEvent> Snapshot() const;

    // Count returns the number of finalized events (0 when empty).
    size_t Count() const;

private:
    struct Inflight {
        std::chrono::steady_clock::time_point start_mono;
        std::string started_at; // ISO-8601 UTC wall stamp at Begin
        PhaseEvent partial;
        bool active{false};
    };

    void record(PhaseEvent ev);
    static std::string UtcNowIso8601();

    mutable std::mutex mu_;
    std::vector<PhaseEvent> events_;
    std::map<int64_t, Inflight> inflight_;
    std::map<std::string, int64_t> indexes_;
    int64_t next_token_{0};
};

// ScopedPhase is the RAII wrapper for PhaseRecorder: begins on
// construction, completes (or aborts) on destruction. Movable, not
// copyable — lives on the stack inside the phase scope.
class ScopedPhase {
public:
    ScopedPhase(PhaseRecorder& recorder, std::string origin, std::string scope,
                std::string component, std::string action, std::string phase,
                std::string event_type = "", std::string event_name = "");
    ~ScopedPhase();

    // Complete finalizes the phase as success; Abort finalizes it as
    // failed. Calling both is fine — only the first takes effect.
    void Complete(int64_t bytes_in = 0, int64_t bytes_out = 0, int64_t frames = 0,
                  const std::string& status = kStatusOk,
                  const std::string& error_code = "",
                  const std::string& error_message = "");
    void Abort(const std::string& error_code = "", const std::string& error_message = "");
    void SetMetadataJSON(std::string metadata_json);
    void SetDetailedMetrics(int32_t segment_index, std::string track_kind,
                            int32_t track_index, double started_offset_ms,
                            double finished_offset_ms, double cpu_ms,
                            double queue_wait_ms, int64_t frames_in,
                            int64_t frames_out);

    ScopedPhase(ScopedPhase&& other) noexcept;
    ScopedPhase& operator=(ScopedPhase&& other) noexcept;
    ScopedPhase(const ScopedPhase&) = delete;
    ScopedPhase& operator=(const ScopedPhase&) = delete;

private:
    void finish();
    PhaseRecorder* recorder_;
    int64_t token_;
    bool done_{false};
};

} // namespace velox::telemetry
