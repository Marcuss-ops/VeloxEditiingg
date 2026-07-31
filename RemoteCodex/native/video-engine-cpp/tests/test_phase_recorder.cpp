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

#include "velox/telemetry/phase_recorder.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
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
    testReset();
    testCanonicalEnums();

    std::cerr << "\nsummary: pass=" << g_pass << " fail=" << g_fail << "\n";
    return g_fail == 0 ? 0 : 1;
}
