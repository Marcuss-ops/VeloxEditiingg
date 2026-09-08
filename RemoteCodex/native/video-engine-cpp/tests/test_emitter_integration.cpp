// tests/test_emitter_integration.cpp — render-engine integration + gate tests (split block 3).
#include "test_emitter_common.h"

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
    EXPECT(engine.framesComposited() == 0,
           "a render without a frame graph must report zero composited frames");

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

void testCopyOnlyTelemetryDoesNotClaimVideoEncoding() {
    SUBCASE("copy-only clips report stream copy rather than encoded frames");
    namespace fs = std::filesystem;
    const auto stem = std::string("velox_copy_only_telemetry_") +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const fs::path input = fs::temp_directory_path() / (stem + "_input.mp4");
    const fs::path output = fs::temp_directory_path() / (stem + "_output.mp4");
    const fs::path sidecar = fs::path(output.string() + ".progress.json");
    struct Cleanup {
        fs::path input;
        fs::path output;
        fs::path sidecar;
        ~Cleanup() {
            std::error_code ec;
            fs::remove(input, ec);
            fs::remove(output, ec);
            fs::remove(sidecar, ec);
        }
    } cleanup{input, output, sidecar};

    const std::string inputCmd =
        "ffmpeg -y -hide_banner -loglevel error -f lavfi -i "
        "testsrc=size=64x64:rate=5:duration=0.4 -an -c:v libx264 "
        "-pix_fmt yuv420p -t 0.4 " + velox::file::shellQuote(input.string());
    EXPECT(velox::file::runCommand(inputCmd), "compatible copy-only fixture must be created");

    velox::plan::RenderPlan plan;
    plan.job_id = "copy-only-telemetry";
    plan.canvas = {64, 64, 5};
    plan.copy_only = true;
    plan.timeline.push_back({velox::plan::VideoSource{input.string(), "fixture"}, 0.4, false, {}, "scene-copy"});
    plan.output_path = output.string();

    velox::core::RenderEngine engine;
    const auto result = engine.render(plan);
    EXPECT(result.success, "copy-only fixture render must succeed (error=" + result.error + ")");
    EXPECT(engine.framesEncoded() == 0, "copy-only render must report zero encoded video frames");
    EXPECT(engine.framesComposited() == 0, "copy-only render must report zero composited video frames");
    EXPECT(engine.encodePasses() == 0, "copy-only render must report zero encode passes");

    const std::string json = velox::file::readFile(sidecar.string());
    EXPECT(std::strstr(json.c_str(), "\"frames\":0") != nullptr,
           "copy-only sidecar must report zero encoded frames");
    EXPECT(std::strstr(json.c_str(), "\"encode_passes\":0") != nullptr,
           "copy-only sidecar must report zero encode passes");
    EXPECT(std::strstr(json.c_str(), "\"component\":\"ffmpeg\"") == nullptr,
           "copy-only sidecar must not emit an engine.encode event");
}

void testReset() {
    SUBCASE("Reset clears events and re-starts indexes at 0");
    vt::PhaseRecorder r;
    r.Emit(vt::kOriginEngine, vt::kScopeAttempt, "engine", "render", "", "ok");
    EXPECT_EQ_INT(static_cast<int>(r.Count()), 1);
    r.Reset();
    EXPECT_EQ_INT(static_cast<int>(r.Count()), 0);
    r.Emit(vt::kOriginEngine, vt::kScopeAttempt, "engine", "render", "", "ok");
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

    SUBCASE("generated language-neutral catalog parity");
    EXPECT(vt::catalog::kSchemaVersion == 1, "catalog schema version");
    EXPECT(vt::catalog::kEvents.size() > 100, "generated catalog is complete");
    EXPECT(vt::catalog::IsCatalogEvent("engine.encode", "setup"),
           "known engine event is registered");
    EXPECT(!vt::catalog::IsCatalogEvent("engine", "invented"),
           "unknown event is rejected by generated catalog");
    const auto* encode = vt::catalog::FindEvent("engine.encode", "setup");
    EXPECT(encode != nullptr, "known event descriptor is findable");
    EXPECT_EQ_STR(std::string(encode->unit), "milliseconds");
    EXPECT_EQ_STR(std::string(encode->aggregation), "sum");
    EXPECT_EQ_STR(std::string(encode->cardinality), "per_segment");
    EXPECT_EQ_STR(std::string(encode->owner), "encoder");
}

void testFailClosedGate() {
    SUBCASE("non-canonical component/action is dropped, not recorded");
    vt::PhaseRecorder r;
    int64_t dropped = r.Begin(vt::kOriginEngine, vt::kScopeAttempt, "engine", "invented", "render");
    EXPECT_EQ_INT(dropped, -1);
    r.Complete(dropped, 0, 0, 0, vt::kStatusOk); // no-op on dropped token
    r.Emit(vt::kOriginEngine, vt::kScopeAttempt, "engine", "invented", "render", vt::kStatusOk);
    EXPECT_EQ_INT(static_cast<int>(r.Count()), 0);

    SUBCASE("newly-registered mux events are canonical and recorded");
    EXPECT(vt::IsCanonicalEvent("engine", "packet_mux"), "packet_mux registered");
    EXPECT(vt::IsCanonicalEvent("engine", "concat"), "concat registered");
    EXPECT(vt::IsCanonicalEvent("engine", "mixed_packet_mux"), "mixed_packet_mux registered");
    int64_t mux = r.Begin(vt::kOriginEngine, vt::kScopeAttempt, "engine", "packet_mux", "finalize");
    r.Complete(mux, 0, 0, 0, vt::kStatusOk);
    EXPECT_EQ_INT(static_cast<int>(r.Count()), 1);
}
