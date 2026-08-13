// test_render_mixed.cpp
//
// Render-level proof of the mixed renderer contract:
//
//   mixed render (VELOX_ENABLE_LIBAV=ON)
//     PACKET_COPY segments   → stream-copied into the packet mux
//     NATIVE_TRANSCODE       → normalized through the native FramePipeline,
//                              then VERIFIED against the canonical profile
//                              (mediaSignaturesCompatible(produced, canonical))
//                              before the mux concatenates it — fail closed.
//
// The packet mux concatenates raw H.264 packets, so a copied range and a
// produced transcode must carry byte-identical SPS/PPS (extradata). The
// canonical source is therefore itself FramePipeline-normalized: the same
// encoder settings produce the same extradata, which is the realistic
// "already-normalized asset" the mixed renderer is designed around.
//
// Part 1 (positive): a FramePipeline-normalized canonical 1080p30 source
// resolves to PACKET_COPY and a non-canonical 720p source resolves to
// NATIVE_TRANSCODE; both assemble through the single packet mux and the
// output is canonical-profile compatible.
//
// Part 2 (negative): a mixed plan whose canvas does not match the canonical
// profile produces a non-canonical transcode, so the produced-segment gate
// fails closed and no output is published.
//
// The engine is exercised through RenderEngine::render() under a sentinel
// PATH whose ffmpeg/ffprobe fail immediately — the mixed path (probe,
// FramePipeline, packet mux) must execute entirely in-process.

#include "velox/core/canonical_video_profile.hpp"
#include "velox/core/render_engine.hpp"
#include "velox/plan/render_plan.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/media_probe.hpp"
#include "velox/services/segment_execution.hpp"
#include "velox/services/segment_execution_libav.hpp"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>

namespace fs = std::filesystem;
using velox::plan::RenderPlan;
using velox::plan::TransformSpec;
using velox::plan::VideoSource;

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
    return "velox_render_mixed_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

bool makeVideo(const fs::path& output, const std::string& size, int fps) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote(
                "testsrc=size=" + size + ":rate=" + std::to_string(fps) +
                ":duration=1.0")
            << " -an -c:v libx264 -preset ultrafast -pix_fmt yuv420p -r " << fps
            << " " << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

// Normalizes a source into the canonical profile with the FramePipeline so
// its SPS/PPS matches a later in-render transcode (identical encoder knobs).
bool normalizeCanonical(const fs::path& input, const fs::path& output) {
    velox::media::FramePipelineConfig config;
    config.input_path = input;
    config.output_path = output;
    config.width = 1920;
    config.height = 1080;
    config.fps_num = 30;
    config.fps_den = 1;
    config.source_duration_us = 1'000'000;
    config.codec = "libx264";
    config.preset = "medium";
    velox::media::FramePipelineResult result;
    return velox::media::renderFrames(config, &result);
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

    // ── Fixtures (system ffmpeg BEFORE the sentinel PATH is installed). ──
    const fs::path nonCanonicalClip = root / "non-canonical.mp4";
    const fs::path canonicalClip = root / "canonical.mp4";
    const fs::path output = root / "mixed-output.mp4";
    const fs::path rejectedOutput = root / "mixed-rejected.mp4";
    expect(makeVideo(nonCanonicalClip, "1280x720", 30),
           "non-canonical 720p30 fixture can be created");
    expect(normalizeCanonical(nonCanonicalClip, canonicalClip),
           "canonical fixture can be FramePipeline-normalized");

    // ── Sentinel PATH: any ffmpeg/ffprobe spawn fails hard. ─────────────
    const fs::path sentinelBin = root / "sentinel-bin";
    fs::create_directory(sentinelBin, ec);
    const fs::path ffmpegTouched = root / "ffmpeg-invoked";
    const fs::path ffprobeTouched = root / "ffprobe-invoked";
    expect(velox::file::writeFile(
        sentinelBin / "ffmpeg",
        "#!/bin/sh\ntouch " + velox::file::shellQuote(ffmpegTouched.string()) + "\nexit 1\n"),
        "ffmpeg sentinel can be written");
    expect(velox::file::writeFile(
        sentinelBin / "ffprobe",
        "#!/bin/sh\ntouch " + velox::file::shellQuote(ffprobeTouched.string()) + "\nexit 1\n"),
        "ffprobe sentinel can be written");
    fs::permissions(sentinelBin / "ffmpeg",
                    fs::perms::owner_read | fs::perms::owner_write | fs::perms::owner_exec,
                    fs::perm_options::replace, ec);
    fs::permissions(sentinelBin / "ffprobe",
                    fs::perms::owner_read | fs::perms::owner_write | fs::perms::owner_exec,
                    fs::perm_options::replace, ec);

    // ── Positive plan: canonical copy + non-canonical transcode. ───────
    const double segmentDuration = 0.4;
    RenderPlan plan;
    plan.version = 1;
    plan.job_id = "mixed-positive";
    plan.canvas = {1920, 1080, 30};
    plan.mixed = true;
    plan.output_path = output.string();
    plan.timeline = {
        {VideoSource{canonicalClip.string(), ""}, segmentDuration, false,
         TransformSpec{"cover", false}, ""},
        {VideoSource{nonCanonicalClip.string(), ""}, segmentDuration, false,
         TransformSpec{"cover", true}, ""},
    };

    // ── Negative plan: canvas outside the canonical profile, so the
    //    transcode output cannot match canonical and must fail closed. ──
    RenderPlan rejectedPlan;
    rejectedPlan.version = 1;
    rejectedPlan.job_id = "mixed-rejected";
    rejectedPlan.canvas = {1280, 720, 30};
    rejectedPlan.mixed = true;
    rejectedPlan.output_path = rejectedOutput.string();
    rejectedPlan.timeline = {
        {VideoSource{nonCanonicalClip.string(), ""}, segmentDuration, false,
         TransformSpec{"cover", true}, ""},
    };

    const char* previousPath = std::getenv("PATH");
    const bool hadPath = previousPath != nullptr;
    const std::string previousPathValue = hadPath ? previousPath : "";
    setenv("PATH", sentinelBin.c_str(), 1);

    velox::core::RenderEngine engine;
    const velox::core::RenderResult result = engine.render(plan);
    velox::core::RenderEngine rejectedEngine;
    const velox::core::RenderResult rejected = rejectedEngine.render(rejectedPlan);

    if (hadPath) {
        setenv("PATH", previousPathValue.c_str(), 1);
    } else {
        unsetenv("PATH");
    }

    // ── Positive assertions. ────────────────────────────────────────────
    expect(result.success, "mixed render succeeds (copy + transcode)");
    if (!result.success) {
        std::cerr << "render error: " << result.error << "\n";
    }
    expect(engine.concatMode() == "mixed_packet",
           "mixed render assembles through the packet mux, actual=\"" +
               engine.concatMode() + "\"");
    expect(engine.framesEncoded() > 0,
           "mixed render transcoded at least one segment");
    expect(engine.encodePasses() == 1,
           "exactly one transcode pass for the single non-canonical segment");
    expect(engine.durationSeconds() > 0.79 && engine.durationSeconds() < 0.81,
           "mixed output covers the copy + transcode timeline (0.8 s)");
    expect(!fs::exists(ffmpegTouched), "mixed render never executed ffmpeg");
    expect(!fs::exists(ffprobeTouched), "mixed render never executed ffprobe");
    expect(fs::exists(output), "mixed output is published");

    // The assembled output must be canonical-profile compatible: both the
    // stream-copied range and the produced transcode resolve to the same
    // canonical identity.
    const auto canonical = velox::core::mediaSignatureFromCanonicalProfile(
        velox::core::canonicalVideoProfileV1());
    velox::media::SegmentProbe outProbe;
    std::string outError;
    expect(velox::media::probeSegmentForExecution(
               output, 0, velox::media::MediaKind::Video, &outProbe, &outError),
           "mixed output can be probed in-process");
    expect(velox::media::mediaSignaturesCompatible(outProbe.signature, canonical),
           "mixed output is canonical-profile compatible");

    // ── Negative assertions. ────────────────────────────────────────────
    expect(!rejected.success,
           "non-canonical produced segment fails closed");
    expect(contains(rejected.error, "canonical-profile compatible"),
           "rejection identifies the produced-segment canonical mismatch, actual=\"" +
               rejected.error + "\"");
    expect(!fs::exists(rejectedOutput),
           "rejected mixed render does not publish output");

    return failures == 0 ? 0 : 1;
}
