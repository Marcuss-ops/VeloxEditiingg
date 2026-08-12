// test_render_plan_v2.cpp
//
// CompiledRenderPlanV2 receiver tests.
//
// The V2 document carries integer timeline placement (frames) and source
// trimming (microseconds): TimelineStartFrame/FrameCount (int64) place the
// segment on the output timeline, SourceInUS/SourceDurationUS (int64) trim
// the source, and DurationUS (int64) is the total. Floating seconds are
// forbidden; the C++ parser fails closed when a V1 float timing key is
// present. The worker injects resolved local asset paths in a runtime
// "bindings" object so the canonical V2 document stays path-free.
//
// Part 1: parser unit tests (pure, no media processes).
// Part 2: a full V2 envelope through RenderEngine::render() under a
// sentinel PATH whose ffmpeg/ffprobe fail — zero spawns, concat_mode
// packet_copy, zero temp bytes, exact int64 duration, FINAL_AUDIO_COPY.

#include "velox/core/render_engine.hpp"
#include "velox/plan/render_plan.hpp"
#include "velox/plan/render_plan_parser.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/io_counters.hpp"
#include "velox/services/media_probe.hpp"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>

namespace fs = std::filesystem;
using velox::plan::RenderPlan;
using velox::plan::parseRenderPlan;

namespace {

int failures = 0;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << "\n";
        ++failures;
    }
}

bool contains(const std::string& haystack, const std::string& needle) {
    return haystack.find(needle) != std::string::npos;
}

std::string uniqueStem() {
    return "velox_v2_plan_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

bool makeVideo(const fs::path& output, const std::string& size) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote(
                "testsrc=size=" + size + ":rate=5:duration=1.2")
            << " -an -c:v libx264 -preset ultrafast -pix_fmt yuv420p -r 5 "
            << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

bool makeAudio(const fs::path& output) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i "
            << velox::file::shellQuote("sine=frequency=440:sample_rate=48000")
            << " -t 2.0 -c:a aac -ar 48000 -ac 2 "
            << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

// Builds a CompiledRenderPlanV2 envelope: canonical V2 document + worker
// runtime envelope (job_id, output_path, bindings).
std::string v2Envelope(const std::string& jobId,
                       const std::string& outputPath,
                       const std::string& clipAPath,
                       const std::string& clipBPath,
                       const std::string& audioPath,
                       const std::string& audioMode = "FINAL_AUDIO_COPY",
                       const std::string& extraSegmentField = "",
                       int64_t firstSourceDurationUS = 800'000) {
    std::ostringstream json;
    json << "{\"plan_version\":2,"
         << "\"job_id\":\"" << jobId << "\","
         << "\"output_path\":\"" << outputPath << "\","
         << "\"duration_us\":1600000,"
         << "\"output\":{\"container\":\"mp4\",\"video_codec\":\"h264\","
         << "\"width\":64,\"height\":64,\"fps_num\":5,\"fps_den\":1},"
         << "\"final_audio\":{\"mode\":\"" << audioMode << "\","
         << "\"asset_id\":\"audio-a\",\"duration_us\":1600000},"
         << "\"video_tracks\":[{\"track_id\":\"main\",\"segments\":["
         << "{\"segment_id\":\"s0\",\"asset_id\":\"clip-a\","
         << "\"timeline_start_frame\":0,\"frame_count\":4,"
         << "\"source_in_us\":0,\"source_duration_us\":" << firstSourceDurationUS
         << extraSegmentField << "},"
         << "{\"segment_id\":\"s1\",\"asset_id\":\"clip-b\","
         << "\"timeline_start_frame\":4,\"frame_count\":4,"
         << "\"source_in_us\":0,\"source_duration_us\":800000}"
         << "]}],"
         << "\"bindings\":{\"clip-a\":\"" << clipAPath << "\","
         << "\"clip-b\":\"" << clipBPath << "\","
         << "\"audio-a\":\"" << audioPath << "\"}}";
    return json.str();
}

void testParserUnit(const fs::path& clipA, const fs::path& clipB,
                    const fs::path& audio) {
    std::cerr << "── parser unit: int64 frames/us are the source of truth ──\n";

    const std::string valid = v2Envelope("v2-unit", "/tmp/v2-unit-out.mp4",
                                         clipA.string(), clipB.string(),
                                         audio.string());
    auto plan = parseRenderPlan(valid);
    expect(plan.has_value(), "valid V2 envelope parses");
    if (!plan) return;

    expect(plan->version == velox::plan::kRenderPlanVersionV2,
           "plan version is 2");
    expect(plan->copy_only, "V2 plans are copy-only packet contracts");
    expect(plan->canvas.width == 64 && plan->canvas.height == 64,
           "canvas dimensions parsed from output block");
    expect(plan->canvas.fps_num == 5 && plan->canvas.fps_den == 1 &&
               plan->canvas.fps == 5,
           "rational frame rate parsed exactly (fps_num/fps_den)");
    expect(plan->timeline.size() == 2, "two segments parsed from video_tracks");

    const auto& first = plan->timeline[0];
    expect(first.duration_us == 800'000,
           "segment duration_us is the exact int64 trim (800000), got " +
               std::to_string(first.duration_us));
    expect(first.source_in_us == 0 && first.source_duration_us == 800'000,
           "source window in microseconds");
    expect(first.timeline_start_frame == 0 && first.frame_count == 4,
           "timeline placement in frames");
    expect(first.duration_seconds == 0.0,
           "V2 segment carries no float duration_seconds");
    const auto* video = std::get_if<velox::plan::VideoSource>(&first.source);
    expect(video != nullptr, "segment source is a bound video path");
    if (video != nullptr) {
        expect(video->url == clipA.string(),
               "asset_id resolved through the bindings map to the local path");
    }

    expect(plan->audio_tracks.size() == 1, "final_audio becomes one track");
    if (plan->audio_tracks.size() == 1) {
        const auto& track = plan->audio_tracks[0];
        expect(track.source_url == audio.string(),
               "final audio resolved through bindings");
        expect(track.duration_us == 1'600'000,
               "final audio duration in microseconds");
        expect(track.start_offset_us == 0 && track.start_time_offset == 0.0,
               "final audio starts at zero, no float offset");
    }

    std::cerr << "── parser unit: float seconds rejected in V2 ──\n";
    const std::string floatSegment = v2Envelope(
        "v2-float", "/tmp/v2-float.mp4", clipA.string(), clipB.string(),
        audio.string(), "FINAL_AUDIO_COPY", ",\"duration_seconds\":0.8");
    expect(!parseRenderPlan(floatSegment).has_value(),
           "V2 segment carrying float duration_seconds fails closed");

    std::cerr << "── parser unit: non-contiguous placement rejected ──\n";
    // A copy-only packet pipeline appends segments sequentially; a gap in
    // timeline_start_frame would silently shift the timeline, so it must be
    // rejected instead of repaired.
    const std::string gapped =
        "{\"plan_version\":2,\"job_id\":\"v2-gap\",\"output_path\":\"/tmp/gap.mp4\","
        "\"duration_us\":1600000,\"output\":{\"width\":64,\"height\":64,"
        "\"fps_num\":5,\"fps_den\":1},\"video_tracks\":[{\"segments\":["
        "{\"segment_id\":\"s0\",\"asset_id\":\"a\",\"timeline_start_frame\":0,"
        "\"frame_count\":4,\"source_in_us\":0,\"source_duration_us\":800000},"
        "{\"segment_id\":\"s1\",\"asset_id\":\"b\",\"timeline_start_frame\":5,"
        "\"frame_count\":4,\"source_in_us\":0,\"source_duration_us\":800000}]}],"
        "\"bindings\":{\"a\":\"" + clipA.string() + "\",\"b\":\"" + clipB.string() + "\"}}";
    expect(!parseRenderPlan(gapped).has_value(),
           "gap in timeline_start_frame fails closed (no silent shift)");

    std::cerr << "── parser unit: frame-exactness mismatch rejected ──\n";
    // 4 frames @ 5 fps = 800 ms; a 1200 ms source window disagrees with the
    // frame_count by more than one frame and must fail closed.
    const std::string mismatched = v2Envelope(
        "v2-frame", "/tmp/v2-frame.mp4", clipA.string(), clipB.string(),
        audio.string(), "FINAL_AUDIO_COPY", "", 1'200'000);
    expect(!parseRenderPlan(mismatched).has_value(),
           "source_duration_us that disagrees with frame_count fails closed");

    std::cerr << "── parser unit: non-FINAL_AUDIO_COPY audio rejected ──\n";
    const std::string mixedAudio = v2Envelope(
        "v2-mix", "/tmp/v2-mix.mp4", clipA.string(), clipB.string(),
        audio.string(), "AUDIO_MIX");
    expect(!parseRenderPlan(mixedAudio).has_value(),
           "final_audio mode other than FINAL_AUDIO_COPY fails closed");

    std::cerr << "── parser unit: V1 regression ──\n";
    std::ostringstream v1;
    v1 << "{\"version\":1,\"job_id\":\"v1-reg\",\"output_path\":\"/tmp/v1.mp4\","
       << "\"canvas\":{\"width\":64,\"height\":64,\"fps\":5},\"copy_only\":true,"
       << "\"timeline\":[{\"source\":{\"type\":\"video\",\"url\":\""
       << clipA.string()
       << "\"},\"duration_seconds\":0.8}],\"audio_tracks\":[]}";
    auto v1Plan = parseRenderPlan(v1.str());
    expect(v1Plan.has_value(), "V1 plan still parses");
    if (v1Plan) {
        expect(v1Plan->version == velox::plan::kRenderPlanVersionV1,
               "V1 version preserved");
        expect(v1Plan->timeline.size() == 1 &&
                   v1Plan->timeline[0].duration_seconds == 0.8,
               "V1 float duration preserved for the legacy path");
    }
}

void testRenderLevel(const fs::path& root, const fs::path& clipA,
                     const fs::path& clipB, const fs::path& audio) {
    std::cerr << "── render-level: V2 envelope through RenderEngine::render ──\n";

    const fs::path sentinelBin = root / "sentinel-bin";
    std::error_code ec;
    fs::create_directory(sentinelBin, ec);
    const fs::path ffmpegTouched = root / "ffmpeg-invoked";
    const fs::path ffprobeTouched = root / "ffprobe-invoked";
    expect(velox::file::writeFile(
        sentinelBin / "ffmpeg",
        "#!/bin/sh\ntouch " + velox::file::shellQuote(ffmpegTouched.string()) +
            "\nexit 1\n"),
        "ffmpeg sentinel can be written");
    expect(velox::file::writeFile(
        sentinelBin / "ffprobe",
        "#!/bin/sh\ntouch " + velox::file::shellQuote(ffprobeTouched.string()) +
            "\nexit 1\n"),
        "ffprobe sentinel can be written");
    fs::permissions(sentinelBin / "ffmpeg",
                    fs::perms::owner_read | fs::perms::owner_write |
                        fs::perms::owner_exec,
                    fs::perm_options::replace, ec);
    fs::permissions(sentinelBin / "ffprobe",
                    fs::perms::owner_read | fs::perms::owner_write |
                        fs::perms::owner_exec,
                    fs::perm_options::replace, ec);

    const fs::path planFile = root / "plan-v2.json";
    const fs::path output = root / "v2-output.mp4";
    const std::string envelope = v2Envelope(
        "v2-render", output.string(), clipA.string(), clipB.string(),
        audio.string());
    expect(velox::file::writeFile(planFile, envelope),
           "V2 envelope can be written");

    const char* previousPath = std::getenv("PATH");
    const bool hadPath = previousPath != nullptr;
    const std::string previousPathValue = hadPath ? previousPath : "";
    setenv("PATH", sentinelBin.c_str(), 1);

    auto plan = parseRenderPlan(velox::file::readFile(planFile.string()));
    velox::core::RenderEngine engine;
    const velox::core::RenderResult result =
        plan.has_value() ? engine.render(*plan)
                         : velox::core::RenderResult{};

    if (hadPath) {
        setenv("PATH", previousPathValue.c_str(), 1);
    } else {
        unsetenv("PATH");
    }

    expect(plan.has_value(), "V2 envelope parses before render");
    expect(result.success, "V2 copy-only render succeeds");
    if (!result.success) {
        std::cerr << "render error: " << result.error << "\n";
    }
    expect(engine.concatMode() == "packet_copy",
           "V2 render uses the in-process packet mux, actual=\"" +
               engine.concatMode() + "\"");
    expect(engine.tempBytesWritten() == 0,
           "zero temp bytes (no segment_N.mp4 / video_only.mp4), actual=" +
               std::to_string(engine.tempBytesWritten()));
    expect(engine.encodePasses() == 0, "zero encode passes");
    expect(engine.framesEncoded() == 0 && engine.framesDecoded() == 0,
           "zero frames encoded/decoded (stream copy)");
    expect(engine.durationSeconds() > 1.5 && engine.durationSeconds() < 1.7,
           "duration derived from the int64 duration_us total (1.6 s)");

    const auto& io = velox::services::ioCounters();
    expect(io.file_copy_count.load() == 0,
           "zero file copies (assets opened in place), actual=" +
               std::to_string(io.file_copy_count.load()));
    expect(io.asset_bytes_copied.load() == 0, "zero asset bytes copied");
    expect(io.input_open_count.load() >= 3,
           "assets opened directly by libavformat, actual=" +
               std::to_string(io.input_open_count.load()));

    expect(!fs::exists(ffmpegTouched), "V2 render never executed ffmpeg");
    expect(!fs::exists(ffprobeTouched), "V2 render never executed ffprobe");
    expect(fs::exists(output), "V2 output is published");

    const auto probe = velox::media::probeMediaInProcess(output);
    expect(probe.has_value(), "V2 output can be probed in-process");
    if (probe.has_value()) {
        bool hasVideo = false;
        bool hasAudio = false;
        for (const auto& stream : probe->streams) {
            hasVideo = hasVideo || stream.is_video;
            hasAudio = hasAudio || stream.is_audio;
        }
        expect(hasVideo, "V2 output contains a video stream");
        expect(hasAudio, "V2 output contains the FINAL_AUDIO_COPY stream");
    }

    const std::string sidecar =
        velox::file::readFile(output.string() + ".progress.json");
    expect(!sidecar.empty(), "V2 sidecar is written");
    if (!sidecar.empty()) {
        expect(contains(sidecar, "\"concat_mode\":\"packet_copy\""),
               "V2 sidecar reports packet_copy");
        expect(contains(sidecar, "\"temp_bytes\":0"),
               "V2 sidecar reports zero temp bytes");
        expect(contains(sidecar, "\"final_mux_audio_mode\":\"COPY\""),
               "V2 sidecar reports FINAL_AUDIO_COPY");
        expect(contains(sidecar, "\"decision_reason\":\"verified_final_mix\""),
               "V2 sidecar reports the verified copy decision");
    }
}

} // namespace

int main() {
    const fs::path root = fs::temp_directory_path() / uniqueStem();
    std::error_code ec;
    fs::create_directories(root, ec);
    expect(!ec, "temporary directory can be created");
    if (ec) return 1;

    struct Cleanup {
        fs::path root;
        ~Cleanup() {
            std::error_code ec;
            fs::remove_all(root, ec);
        }
    } cleanup{root};

    // Fixtures are generated BEFORE the sentinel PATH is installed; the
    // render itself must never spawn media processes.
    const fs::path clipA = root / "clip-a.mp4";
    const fs::path clipB = root / "clip-b.mp4";
    const fs::path audio = root / "audio.m4a";
    expect(makeVideo(clipA, "64x64"), "clip A fixture can be created");
    expect(makeVideo(clipB, "64x64"), "clip B fixture can be created");
    expect(makeAudio(audio), "audio fixture can be created");

    testParserUnit(clipA, clipB, audio);
    testRenderLevel(root, clipA, clipB, audio);

    std::cerr << "summary: fail=" << failures << "\n";
    return failures == 0 ? 0 : 1;
}
