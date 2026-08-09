// tests/test_phase_recorder.cpp — block-1 telemetry phase recorder tests.
//
// Exercises PhaseRecorder (Begin/Complete/Abort/Emit, per-origin event
// indexes, canonical enum coercion) and the ScopedPhase RAII helper
// (completion on scope exit, abort on failure, move semantics).
//
// No gtest dependency — this project doesn't ship GoogleTest. Each
// sub-test prints PASS/FAIL with a clear assertion line. Exit code 0
// iff all tests pass.
//
// Run via the binary velox_phase_recorder_tests.

#include "velox/core/render_engine.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/telemetry/phase_recorder.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace vt = velox::telemetry;

namespace {

int g_pass = 0;
int g_fail = 0;

#define EXPECT(cond, msg)                                                  \
    do {                                                                   \
        if (!(cond)) {                                                     \
            std::cerr << "FAIL " << __FILE__ << ":" << __LINE__            \
                      << ": " << msg << " (expected: " << #cond << ")\n";  \
            ++g_fail;                                                      \
            return;                                                        \
        }                                                                  \
    } while (0)

#define EXPECT_EQ_INT(actual, expected)                                    \
    do {                                                                   \
        auto _a = (actual);                                                \
        auto _e = (expected);                                              \
        if (_a != _e) {                                                    \
            std::cerr << "FAIL " << __FILE__ << ":" << __LINE__            \
                      << ": int mismatch (got=" << _a                       \
                      << " want=" << _e << ")\n";                          \
            ++g_fail;                                                      \
            return;                                                        \
        }                                                                  \
    } while (0)

#define EXPECT_EQ_STR(actual, expected)                                    \
    do {                                                                   \
        auto _a = (actual);                                                \
        auto _e = (expected);                                              \
        if (_a != _e) {                                                    \
            std::cerr << "FAIL " << __FILE__ << ":" << __LINE__            \
                      << ": str mismatch (got=\"" << _a                     \
                      << "\" want=\"" << _e << "\")\n";                    \
            ++g_fail;                                                      \
            return;                                                        \
        }                                                                  \
    } while (0)

#define SUBCASE(name)                                                      \
    do {                                                                   \
        std::cerr << "── sub-case: " << name << " ──\n";                   \
        ++g_pass;                                                          \
    } while (0)

void testBeginComplete() {
    SUBCASE("Begin/Complete measures monotonic duration + UTC stamps");
    vt::PhaseRecorder r;
    int64_t tok = r.Begin(vt::kOriginEngine, vt::kScopeAttempt, "engine", "render", "render");
    EXPECT(tok >= 0, "token must be non-negative");
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    r.Complete(tok, 0, 0, 0, vt::kStatusOk);

    auto events = r.Snapshot();
    EXPECT_EQ_INT(static_cast<int>(events.size()), 1);
    const auto& e = events[0];
    EXPECT_EQ_STR(e.origin, "engine");
    EXPECT_EQ_STR(e.scope, "attempt");
    EXPECT_EQ_STR(e.component, "engine");
    EXPECT_EQ_STR(e.status, "ok");
    EXPECT_EQ_STR(e.event_type, "completed");
    EXPECT_EQ_INT(e.event_index, 0);
    EXPECT(e.duration_ms >= 1, "duration_ms must be >= 1 after sleep");
    EXPECT(!e.started_at.empty(), "started_at must be set");
    EXPECT(!e.completed_at.empty(), "completed_at must be set");
    EXPECT_EQ_INT(static_cast<int>(e.started_at.back()), 'Z'); // UTC suffix
}

void testPerOriginIndexes() {
    SUBCASE("event_index increments per origin");
    vt::PhaseRecorder r;
    r.Emit(vt::kOriginWorker, vt::kScopeAttempt, "runner", "a", "", "ok");
    r.Emit(vt::kOriginEngine, vt::kScopeAttempt, "ffmpeg", "b", "", "ok");
    r.Emit(vt::kOriginWorker, vt::kScopeAttempt, "runner", "c", "", "ok");

    auto events = r.Snapshot();
    EXPECT_EQ_INT(static_cast<int>(events.size()), 3);
    EXPECT_EQ_INT(events[0].event_index, 0);
    EXPECT_EQ_INT(events[1].event_index, 0);
    EXPECT_EQ_INT(events[2].event_index, 1);
}

void testAbortAndNormalize() {
    SUBCASE("Abort yields failed event; non-canonical values coerced");
    vt::PhaseRecorder r;
    int64_t tok = r.Begin("bogus", "nope", "ffmpeg", "encode", "encode");
    r.Abort(tok, "EIO", "disk full");
    auto events = r.Snapshot();
    EXPECT_EQ_INT(static_cast<int>(events.size()), 1);
    EXPECT_EQ_STR(events[0].origin, "engine"); // coerced
    EXPECT_EQ_STR(events[0].scope, "attempt"); // coerced
    EXPECT_EQ_STR(events[0].status, "failed");
    EXPECT_EQ_STR(events[0].event_type, "failed");
    EXPECT_EQ_STR(events[0].error_code, "EIO");
    EXPECT_EQ_STR(events[0].error_message, "disk full");
}

void testUnknownTokenNoop() {
    SUBCASE("Complete/Abort on unknown token is a no-op");
    vt::PhaseRecorder r;
    r.Complete(999, 0, 0, 0, "ok");
    r.Abort(998, "x", "y");
    EXPECT_EQ_INT(static_cast<int>(r.Snapshot().size()), 0);
}

void testScopedPhaseRaii() {
    SUBCASE("ScopedPhase completes on scope exit, aborts on failure");
    vt::PhaseRecorder r;
    {
        vt::ScopedPhase okPhase(r, vt::kOriginEngine, vt::kScopeSegment,
                                "ffmpeg", "encode_segment_0", "encode");
        // destructor completes as ok
    }
    {
        vt::ScopedPhase failPhase(r, vt::kOriginEngine, vt::kScopeSegment,
                                  "ffmpeg", "encode_segment_1", "encode");
        failPhase.Abort("encode_failed", "boom");
    }
    auto events = r.Snapshot();
    EXPECT_EQ_INT(static_cast<int>(events.size()), 2);
    EXPECT_EQ_STR(events[0].status, "ok");
    EXPECT_EQ_STR(events[1].status, "failed");
    EXPECT_EQ_STR(events[1].error_code, "encode_failed");
    // Per-origin indexes contiguous across both events.
    EXPECT_EQ_INT(events[0].event_index, 0);
    EXPECT_EQ_INT(events[1].event_index, 1);
}

void testScopedPhaseMove() {
    SUBCASE("ScopedPhase move transfers ownership; moved-from is inert");
    vt::PhaseRecorder r;
    {
        vt::ScopedPhase a(r, vt::kOriginEngine, vt::kScopeAttempt, "engine", "render", "render");
        vt::ScopedPhase b = std::move(a); // a is now inert
    }
    auto events = r.Snapshot();
    EXPECT_EQ_INT(static_cast<int>(events.size()), 1);
    EXPECT_EQ_STR(events[0].status, "ok");
}

void testAppendJson() {
    SUBCASE("AppendJson emits a complete phases[] element");
    vt::PhaseRecorder r;
    int64_t tok = r.Begin(vt::kOriginEngine, vt::kScopeAttempt, "engine", "render", "render",
                          "completed", "render_v1");
    r.Complete(tok, 100, 200, 30, vt::kStatusOk);

    auto events = r.Snapshot();
    EXPECT_EQ_INT(static_cast<int>(events.size()), 1);
    std::ostringstream out;
    events[0].AppendJson(out);
    std::string json = out.str();
    EXPECT(std::strstr(json.c_str(), "\"origin\":\"engine\"") != nullptr, "origin in json");
    EXPECT(std::strstr(json.c_str(), "\"event_index\":0") != nullptr, "event_index in json");
    EXPECT(std::strstr(json.c_str(), "\"duration_ms\":") != nullptr, "duration_ms in json");
    EXPECT(std::strstr(json.c_str(), "\"bytes_in\":100") != nullptr, "bytes_in in json");
    EXPECT(std::strstr(json.c_str(), "\"bytes_out\":200") != nullptr, "bytes_out in json");
    EXPECT(std::strstr(json.c_str(), "\"frames\":30") != nullptr, "frames in json");
    EXPECT(std::strstr(json.c_str(), "\"started_at\":") != nullptr, "started_at in json");
}

void testAppendJsonMetadataValidationAndDetailedFields() {
    SUBCASE("AppendJson omits invalid metadata and keeps detailed fields");
    vt::PhaseRecorder r;
    int64_t token = r.Begin(vt::kOriginEngine, vt::kScopeSegment,
                            "engine", "encode", "encode");
    r.SetMetadataJSON(token, "{\"codec\":]");
    r.SetDetailedMetrics(token, 3, "video", 1, 1.25, 8.5, 42.0, 2.0, 31, 30);
    r.Complete(token, 100, 200, 30, vt::kStatusOk);
    auto events = r.Snapshot();
    EXPECT_EQ_INT(static_cast<int>(events.size()), 1);
    std::ostringstream out;
    events[0].AppendJson(out);
    const std::string invalidJson = out.str();
    EXPECT(std::strstr(invalidJson.c_str(), "\"metadata\"") == nullptr,
           "invalid metadata omitted");
    EXPECT(std::strstr(invalidJson.c_str(), "\"segment_index\":3") != nullptr,
           "segment index emitted");
    EXPECT(std::strstr(invalidJson.c_str(), "\"track_kind\":\"video\"") != nullptr,
           "track kind emitted");
    EXPECT(std::strstr(invalidJson.c_str(), "\"frames_in\":31") != nullptr,
           "frames in emitted");

    vt::PhaseRecorder validRecorder;
    int64_t validToken = validRecorder.Begin(vt::kOriginEngine, vt::kScopeSegment,
                                             "engine", "encode", "encode");
    validRecorder.SetMetadataJSON(validToken, "{\"codec\":\"h264\",\"stream\":{\"index\":0},\"tags\":[\"main\"]}");
    validRecorder.Complete(validToken, 0, 0, 0, vt::kStatusOk);
    std::ostringstream validOut;
    validRecorder.Snapshot()[0].AppendJson(validOut);
    EXPECT(std::strstr(validOut.str().c_str(), "\"metadata\":{\"codec\":\"h264\",\"stream\":{\"index\":0},\"tags\":[\"main\"]}") != nullptr,
           "valid nested metadata emitted");
}

void testAppendJsonEscapesStrings() {
    SUBCASE("AppendJson escapes detailed event strings so phases[] remains valid JSON");
    vt::PhaseRecorder r;
    int64_t token = r.Begin(vt::kOriginEngine, vt::kScopeSegment,
                            "engine", "encode", "encode", "", "segment\"0");
    r.Abort(token, "E\\\"IO", "line 1\nline 2");
    auto events = r.Snapshot();
    EXPECT_EQ_INT(static_cast<int>(events.size()), 1);
    std::ostringstream out;
    events[0].AppendJson(out);
    const std::string json = out.str();
    EXPECT(std::strstr(json.c_str(), "segment\\\"0") != nullptr, "event_name quote escaped");
    EXPECT(std::strstr(json.c_str(), "E\\\\\\\"IO") != nullptr, "error_code quote escaped");
    EXPECT(std::strstr(json.c_str(), "line 1\\nline 2") != nullptr, "error newline escaped");

    vt::PhaseRecorder controls;
    int64_t controlToken = controls.Begin(vt::kOriginEngine, vt::kScopeAttempt,
                                           "engine", "render", "render", "", "controls");
    controls.Abort(controlToken, "E\\b\\f", "control\b\f\x01");
    std::ostringstream controlOut;
    controls.Snapshot()[0].AppendJson(controlOut);
    const std::string controlJson = controlOut.str();
    EXPECT(std::strstr(controlJson.c_str(), "E\\\\b\\\\f") != nullptr,
           "backspace and form-feed escapes emitted");
    EXPECT(std::strstr(controlJson.c_str(), "control\\b\\f\\u0001") != nullptr,
           "ASCII control characters emitted as JSON escapes");
}

void testCompleteSidecarSchema() {
    SUBCASE("sidecarJson keeps phase_ms, segments[] and phases[] in one payload");
    velox::core::RenderEngine engine;
    engine.metrics().addMs("encode", 12.5);
    velox::core::SegmentTiming segment;
    segment.index = 2;
    segment.source_type = "video";
    segment.total_ms = 20.0;
    segment.codec = "h264";
    segment.preset = "fast";
    segment.status = vt::kStatusOk;
    segment.started_offset_ms = 1.5;
    segment.finished_offset_ms = 21.5;
    segment.worker_slot = 3;
    segment.cpu_threads = 4;
    segment.parallel_group = "scene-2";
    engine.metrics().addSegment(segment);

    int64_t token = engine.recorder().Begin(
        vt::kOriginEngine, vt::kScopeSegment, "engine.encode", "frame_submit", "encode");
    engine.recorder().SetDetailedMetrics(token, 2, "video", 0, 1.5, 21.0, 18.0, 0.5, 120, 118);
    engine.recorder().Complete(token, 1000, 2000, 118, vt::kStatusOk);

    int64_t tempToken = engine.recorder().Begin(
        vt::kOriginEngine, vt::kScopeAttempt, "worker.temp", "create", "prepare");
    engine.recorder().Complete(tempToken, 0, 0, 0, vt::kStatusOk);
    int64_t assetToken = engine.recorder().Begin(
        vt::kOriginEngine, vt::kScopeSegment, "worker.asset", "transfer", "download");
    engine.recorder().Complete(assetToken, 1000, 1000, 0, vt::kStatusOk);
    int64_t concatToken = engine.recorder().Begin(
        vt::kOriginEngine, vt::kScopeAttempt, "engine", "concat", "composite");
    engine.recorder().Complete(concatToken, 1000, 2000, 120, vt::kStatusOk);
    int64_t audioToken = engine.recorder().Begin(
        vt::kOriginEngine, vt::kScopeAudioTrack, "engine.audio", "mix", "audio");
    engine.recorder().Complete(audioToken, 300, 400, 0, vt::kStatusOk);
    int64_t muxToken = engine.recorder().Begin(
        vt::kOriginEngine, vt::kScopeAudioTrack, "engine.mux", "audio", "encode");
    engine.recorder().Complete(muxToken, 300, 400, 0, vt::kStatusOk);
    int64_t subtitleToken = engine.recorder().Begin(
        vt::kOriginValidation, vt::kScopeSubtitleTrack, "subtitle", "burn_in", "subtitle");
    engine.recorder().Complete(subtitleToken, 0, 0, 0, vt::kStatusOk);
    int64_t qualityToken = engine.recorder().Begin(
        vt::kOriginValidation, vt::kScopeArtifact, "quality", "ffprobe", "quality");
    engine.recorder().Complete(qualityToken, 0, 0, 0, vt::kStatusOk);
    int64_t failedAssetToken = engine.recorder().Begin(
        vt::kOriginWorker, vt::kScopeArtifact, "worker.asset", "download", "download");
    engine.recorder().Complete(failedAssetToken, 512, 0, 0, vt::kStatusFailed, "EIO", "disk full");
    engine.recorder().Emit(
        vt::kOriginWorker, vt::kScopeAttempt, "attempt", "retry", "retry", vt::kStatusOk);

    const std::string json = engine.sidecarJson("/tmp/render-output.mp4");
    EXPECT(json.size() > 0, "sidecar JSON is not empty");
    EXPECT(json.front() == '{' && json.back() == '}', "sidecar JSON is object-shaped");
    EXPECT(vt::IsValidJsonObject(json), "complete sidecar JSON parses as an object");
    EXPECT(std::strstr(json.c_str(), "\"phase_ms\":{\"encode\":12.5}") != nullptr,
           "phase_ms summary emitted");
    EXPECT(std::strstr(json.c_str(), "\"segments\":[") != nullptr,
           "segments array emitted");
    EXPECT(std::strstr(json.c_str(), "\"codec\":\"h264\"") != nullptr,
           "segment codec emitted");
    EXPECT(std::strstr(json.c_str(), "\"parallel_group\":\"scene-2\"") != nullptr,
           "segment parallelism emitted");
    EXPECT(std::strstr(json.c_str(), "\"phases\":[") != nullptr,
           "phases array emitted");
    EXPECT(std::strstr(json.c_str(), "\"component\":\"engine.encode\"") != nullptr,
           "detailed phase emitted");
    EXPECT(std::strstr(json.c_str(), "\"component\":\"worker.temp\"") != nullptr,
           "temp phase emitted");
    EXPECT(std::strstr(json.c_str(), "\"component\":\"worker.asset\"") != nullptr,
           "asset phase emitted");
    EXPECT(std::strstr(json.c_str(), "\"action\":\"concat\"") != nullptr,
           "concat phase emitted");
    EXPECT(std::strstr(json.c_str(), "\"component\":\"engine.mux\"") != nullptr,
           "mux phase emitted");
    EXPECT(std::strstr(json.c_str(), "\"segment_index\":2") != nullptr,
           "detailed segment index emitted");
    EXPECT(std::strstr(json.c_str(), "\"cpu_ms\":18") != nullptr,
           "detailed cpu timing emitted");
    EXPECT(std::strstr(json.c_str(), "\"frames_in\":120") != nullptr,
           "detailed frame counters emitted");
    EXPECT(std::strstr(json.c_str(), "\"observability\":{") != nullptr,
           "observability rollup emitted");
    EXPECT(std::strstr(json.c_str(), "\"audio\":{\"events\":2") != nullptr,
           "audio rollup includes mix and mux events");
    EXPECT(std::strstr(json.c_str(), "\"subtitle\":{\"events\":1") != nullptr,
           "subtitle rollup emitted");
    EXPECT(std::strstr(json.c_str(), "\"quality\":{\"events\":1") != nullptr,
           "quality rollup emitted");
    EXPECT(std::strstr(json.c_str(), "\"retry\":{\"count\":1}") != nullptr,
           "retry rollup emitted");
    EXPECT(std::strstr(json.c_str(), "\"wasted_download_bytes\":512") != nullptr,
           "wasted download rollup emitted");
}

void testRenderEngineIntegration() {
    SUBCASE("RenderEngine records real phases and writes a compatible sidecar");
    namespace fs = std::filesystem;
    const auto stem = std::string("velox_phase_integration_") +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const fs::path output = fs::temp_directory_path() / (stem + ".mp4");
    const fs::path sidecar = fs::path(output.string() + ".progress.json");
    struct Cleanup {
        fs::path output;
        fs::path sidecar;
        ~Cleanup() {
            std::error_code ec;
            fs::remove(sidecar, ec);
            fs::remove(output, ec);
        }
    } cleanup{output, sidecar};

    velox::plan::RenderPlan plan;
    plan.job_id = "phase-recorder-integration";
    plan.canvas = {64, 64, 5};
    plan.timeline.push_back({velox::plan::ColorSource{"#112233"}, 0.2, false, {"stretch", false}});
    plan.timeline.back().scene_id = "scene-test";
    plan.output_path = output.string();

    velox::core::RenderEngine engine;
    ::setenv("VELOX_FFMPEG_DECODE_TELEMETRY", "1", 1);
    const auto result = engine.render(plan);
    ::unsetenv("VELOX_FFMPEG_DECODE_TELEMETRY");
    EXPECT(result.success, "RenderEngine color render must succeed (error=" + result.error + ")");
    EXPECT(fs::exists(output), "render output must exist");
    EXPECT(fs::exists(sidecar), "SidecarGuard must write the progress sidecar");
    EXPECT(engine.framesEncoded() > 0, "FFmpeg progress must report encoded frames");
    EXPECT(engine.framesDecoded() > 0, "showinfo must report decoded frames");
    EXPECT(engine.framesComposited() > 0, "FFmpeg progress must report composited frames");

    const std::string json = velox::file::readFile(sidecar.string());
    EXPECT(!json.empty(), "integration sidecar must not be empty");
    EXPECT(vt::IsValidJsonObject(json), "integration sidecar must be valid JSON");
    EXPECT(std::strstr(json.c_str(), "\"phase_ms\":{") != nullptr,
           "integration sidecar keeps phase_ms");
    EXPECT(std::strstr(json.c_str(), "\"segments\":[") != nullptr,
           "integration sidecar keeps segments");
    EXPECT(std::strstr(json.c_str(), "\"scene_id\":\"scene-test\"") != nullptr,
           "integration sidecar preserves scene identity");
    EXPECT(std::strstr(json.c_str(), "\"phases\":[") != nullptr,
           "integration sidecar emits phases");
    EXPECT(std::strstr(json.c_str(), "\"action\":\"render\"") != nullptr,
           "real render phase emitted");
    EXPECT(std::strstr(json.c_str(), "\"component\":\"worker.temp\"") != nullptr,
           "real temp phase emitted");
    EXPECT(std::strstr(json.c_str(), "\"component\":\"ffmpeg\"") != nullptr,
           "real encode phase emitted");
    EXPECT(std::strstr(json.c_str(), "\"action\":\"concat\"") != nullptr,
           "real concat phase emitted");
}

void testReset() {
    SUBCASE("Reset clears events and re-starts indexes at 0");
    vt::PhaseRecorder r;
    r.Emit(vt::kOriginEngine, vt::kScopeAttempt, "engine", "a", "", "ok");
    EXPECT_EQ_INT(static_cast<int>(r.Count()), 1);
    r.Reset();
    EXPECT_EQ_INT(static_cast<int>(r.Count()), 0);
    r.Emit(vt::kOriginEngine, vt::kScopeAttempt, "engine", "b", "", "ok");
    auto events = r.Snapshot();
    EXPECT_EQ_INT(static_cast<int>(events.size()), 1);
    EXPECT_EQ_INT(events[0].event_index, 0);
}

void testCanonicalEnums() {
    SUBCASE("closed enum membership predicates");
    EXPECT(vt::IsCanonicalOrigin("engine"), "engine is canonical");
    EXPECT(vt::IsCanonicalOrigin("worker"), "worker is canonical");
    EXPECT(!vt::IsCanonicalOrigin("bogus"), "bogus is not canonical");
    EXPECT(!vt::IsCanonicalOrigin(""), "empty is not canonical");
    EXPECT(vt::IsCanonicalScope("attempt"), "attempt is canonical");
    EXPECT(vt::IsCanonicalScope("segment"), "segment is canonical");
    EXPECT(!vt::IsCanonicalScope("nope"), "nope is not canonical");
}

} // namespace

int main() {
    std::cerr << "running phase_recorder tests\n";

    testBeginComplete();
    testPerOriginIndexes();
    testAbortAndNormalize();
    testUnknownTokenNoop();
    testScopedPhaseRaii();
    testScopedPhaseMove();
    testAppendJson();
    testAppendJsonMetadataValidationAndDetailedFields();
    testAppendJsonEscapesStrings();
    testCompleteSidecarSchema();
    testRenderEngineIntegration();
    testReset();
    testCanonicalEnums();

    std::cerr << "\nsummary: pass=" << g_pass << " fail=" << g_fail << "\n";
    return g_fail == 0 ? 0 : 1;
}
