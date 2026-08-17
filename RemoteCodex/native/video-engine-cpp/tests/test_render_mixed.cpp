// test_render_mixed.cpp
//
// Render-level proof of the mixed renderer copy-only contract:
//
//   mixed render (VELOX_ENABLE_LIBAV=ON)
//     PACKET_COPY segments   → stream-copied into the packet mux
//     REJECT                 → the job FAILS deterministically
//                              (segment_execution_rejected) with the exact
//                              resolver reason — the mixed path never
//                              re-encodes and never falls back to the
//                              legacy loop.
//
// The assembly path is copy-only: a successful mixed render MUST satisfy
//   frames_encoded == 0
//   encode_passes == 0
//   packet_copy_segments == total_segments
// and a rejected segment MUST fail the job with frames_encoded == 0.
//
// Part 1 (positive): three FramePipeline-normalized canonical 1080p30
// sources resolve to PACKET_COPY and assemble through the single packet mux;
// the output is canonical-profile compatible and zero frames were encoded.
//
// Part 2 (negative): a mixed plan whose timeline contains a non-canonical
// 720p source is REJECTED with reason "media signature mismatch: width"; the
// job fails, no output is published, and no frame was encoded (the engine
// never tries to repair the segment).
//
// Part 3 (negative, same contract): an HEVC (H.265) segment at canonical
// resolution is REJECTED with "media signature mismatch: codec_id"; a
// canonical segment trimmed at a non-keyframe source_in_us is REJECTED with
// "source window is not keyframe-safe for packet copy". Both fail the job
// with frames_encoded == 0 — the engine never re-encodes to repair them.
//
// The engine is exercised through RenderEngine::render() under a sentinel
// PATH whose ffmpeg/ffprobe fail immediately — the mixed path (probe,
// packet mux) must execute entirely in-process.

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
            << " -an -c:v libx264 -preset medium -profile:v high -level:v 4.0"
            << " -pix_fmt yuv420p -r " << fps
            << " " << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

// H.265/HEVC source at the same canonical resolution/fps: the ONLY field that
// differs from the canonical profile is codec_id, so the resolver must report
// "media signature mismatch: codec_id" (codec is checked before any other
// video field).
bool makeHevcVideo(const fs::path& output, const std::string& size, int fps) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote(
                "testsrc=size=" + size + ":rate=" + std::to_string(fps) +
                ":duration=1.0")
            << " -an -c:v libx265 -preset medium -tag:v hvc1"
            << " -pix_fmt yuv420p -r " << fps
            << " " << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

// Normalizes a source into the canonical profile with the FramePipeline so
// its SPS/PPS matches the canonical identity (identical encoder knobs).
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
    const fs::path hevcClip = root / "hevc.mp4";
    const fs::path output = root / "mixed-output.mp4";
    const fs::path rejectedOutput = root / "mixed-rejected.mp4";
    const fs::path hevcRejectedOutput = root / "mixed-hevc-rejected.mp4";
    const fs::path keyframeRejectedOutput = root / "mixed-keyframe-rejected.mp4";
    expect(makeVideo(nonCanonicalClip, "1280x720", 30),
           "non-canonical 720p30 fixture can be created");
    expect(normalizeCanonical(nonCanonicalClip, canonicalClip),
           "canonical fixture can be FramePipeline-normalized");
    expect(makeHevcVideo(hevcClip, "1920x1080", 30),
           "HEVC 1080p30 fixture can be created");

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

    // ── Positive plan: three canonical, keyframe-safe segments. All must
    //    resolve to PACKET_COPY and assemble with zero encode work. ──────
    const double segmentDuration = 0.3;
    RenderPlan plan;
    plan.version = 1;
    plan.job_id = "mixed-positive";
    plan.canvas = {1920, 1080, 30};
    plan.mixed = true;
    plan.output_path = output.string();
    plan.timeline = {
        {VideoSource{canonicalClip.string(), ""}, segmentDuration, false,
         TransformSpec{"cover", false}, ""},
        {VideoSource{canonicalClip.string(), ""}, segmentDuration, false,
         TransformSpec{"cover", false}, ""},
        {VideoSource{canonicalClip.string(), ""}, segmentDuration, false,
         TransformSpec{"cover", false}, ""},
    };

    // ── Negative plan: one non-canonical 720p segment in the timeline. The
    //    copy-only resolver REJECTS it ("media signature mismatch: width")
    //    and the job must fail — no re-encode, no output. ────────────────
    RenderPlan rejectedPlan;
    rejectedPlan.version = 1;
    rejectedPlan.job_id = "mixed-rejected";
    rejectedPlan.canvas = {1920, 1080, 30};
    rejectedPlan.mixed = true;
    rejectedPlan.output_path = rejectedOutput.string();
    rejectedPlan.timeline = {
        {VideoSource{canonicalClip.string(), ""}, segmentDuration, false,
         TransformSpec{"cover", false}, ""},
        {VideoSource{nonCanonicalClip.string(), ""}, segmentDuration, false,
         TransformSpec{"cover", false}, ""},
    };

    // ── HEVC negative plan: a canonical-resolution H.265 segment. The
    //    resolver rejects on codec_id (checked before any other video
    //    field), so the exact reason is deterministic. ──────────────────
    RenderPlan hevcPlan;
    hevcPlan.version = 1;
    hevcPlan.job_id = "mixed-hevc-rejected";
    hevcPlan.canvas = {1920, 1080, 30};
    hevcPlan.mixed = true;
    hevcPlan.output_path = hevcRejectedOutput.string();
    hevcPlan.timeline = {
        {VideoSource{canonicalClip.string(), ""}, segmentDuration, false,
         TransformSpec{"cover", false}, ""},
        {VideoSource{hevcClip.string(), ""}, segmentDuration, false,
         TransformSpec{"cover", false}, ""},
    };

    // ── Non-keyframe trim negative plan: the canonical clip is 1.0 s at
    //    30 fps with a single IDR at frame 0 (libx264 medium, GOP 250), so
    //    trimming at source_in_us=500000 (0.5 s) starts on a non-keyframe.
    //    The copy-only path must reject, never re-encode to fix the trim.
    RenderPlan keyframePlan;
    keyframePlan.version = 1;
    keyframePlan.job_id = "mixed-keyframe-rejected";
    keyframePlan.canvas = {1920, 1080, 30};
    keyframePlan.mixed = true;
    keyframePlan.output_path = keyframeRejectedOutput.string();
    keyframePlan.timeline = {
        {VideoSource{canonicalClip.string(), ""}, segmentDuration, false,
         TransformSpec{"cover", false}, "", 0, 500000},
    };

    const char* previousPath = std::getenv("PATH");
    const bool hadPath = previousPath != nullptr;
    const std::string previousPathValue = hadPath ? previousPath : "";
    setenv("PATH", sentinelBin.c_str(), 1);

    velox::core::RenderEngine engine;
    const velox::core::RenderResult result = engine.render(plan);
    velox::core::RenderEngine rejectedEngine;
    const velox::core::RenderResult rejected = rejectedEngine.render(rejectedPlan);
    velox::core::RenderEngine hevcEngine;
    const velox::core::RenderResult hevcRejected = hevcEngine.render(hevcPlan);
    velox::core::RenderEngine keyframeEngine;
    const velox::core::RenderResult keyframeRejected = keyframeEngine.render(keyframePlan);

    if (hadPath) {
        setenv("PATH", previousPathValue.c_str(), 1);
    } else {
        unsetenv("PATH");
    }

    // ── Positive assertions: copy-only success with zero encode. ────────
    expect(result.success, "mixed render succeeds (all-canonical copy-only)");
    if (!result.success) {
        std::cerr << "render error: " << result.error << "\n";
    }
    expect(engine.concatMode() == "mixed_packet",
           "mixed render assembles through the packet mux, actual=\"" +
               engine.concatMode() + "\"");
    expect(engine.framesEncoded() == 0,
           "copy-only mixed render encodes zero frames");
    expect(engine.framesDecoded() == 0,
           "copy-only mixed render decodes zero frames");
    expect(engine.encodePasses() == 0,
           "copy-only mixed render runs zero encode passes");
    expect(engine.tempBytesWritten() == 0,
           "copy-only mixed render writes no intermediate files");
    expect(engine.durationSeconds() > 0.89 && engine.durationSeconds() < 0.91,
           "mixed output covers the full 0.9 s timeline");
    expect(!fs::exists(ffmpegTouched), "mixed render never executed ffmpeg");
    expect(!fs::exists(ffprobeTouched), "mixed render never executed ffprobe");
    expect(fs::exists(output), "mixed output is published");

    // The assembled output must be canonical-profile compatible: every
    // stream-copied range resolves to the same canonical identity.
    const auto canonical = velox::core::mediaSignatureFromCanonicalProfile(
        velox::core::canonicalVideoProfileV1());
    velox::media::SegmentProbe outProbe;
    std::string outError;
    expect(velox::media::probeSegmentForExecution(
               output, 0, velox::media::MediaKind::Video, &outProbe, &outError),
           "mixed output can be probed in-process");
    expect(velox::media::mediaSignaturesCompatible(outProbe.signature, canonical),
           "mixed output is canonical-profile compatible");

    // ── Negative assertions: the job fails, the worker process stays alive. ──
    expect(!rejected.success,
           "non-canonical segment fails the mixed job deterministically");
    expect(contains(rejected.error, "segment_execution_rejected"),
           "rejection carries the segment_execution_rejected code, actual=\"" +
               rejected.error + "\"");
    expect(contains(rejected.error, "media signature mismatch: width"),
           "rejection identifies the exact mismatched field, actual=\"" +
               rejected.error + "\"");
    expect(rejectedEngine.framesEncoded() == 0,
           "rejected segment is never repaired by re-encoding");
    expect(rejectedEngine.encodePasses() == 0,
           "rejected segment runs zero encode passes");
    expect(!fs::exists(rejectedOutput),
           "rejected mixed render does not publish output");

    // ── HEVC negative assertions: codec_id mismatch, zero encode work. ──
    expect(!hevcRejected.success,
           "HEVC segment fails the mixed job deterministically");
    expect(contains(hevcRejected.error, "segment_execution_rejected"),
           "HEVC rejection carries the segment_execution_rejected code, actual=\"" +
               hevcRejected.error + "\"");
    expect(contains(hevcRejected.error, "media signature mismatch: codec_id"),
           "HEVC rejection identifies the codec_id mismatch, actual=\"" +
               hevcRejected.error + "\"");
    expect(hevcEngine.framesEncoded() == 0,
           "HEVC segment is never repaired by re-encoding");
    expect(hevcEngine.encodePasses() == 0,
           "HEVC segment runs zero encode passes");
    expect(!fs::exists(hevcRejectedOutput),
           "HEVC rejected render does not publish output");

    // ── Non-keyframe trim assertions: reject, never re-encode. ───────────
    expect(!keyframeRejected.success,
           "non-keyframe-safe trim fails the mixed job deterministically");
    expect(contains(keyframeRejected.error, "segment_execution_rejected"),
           "keyframe rejection carries the segment_execution_rejected code, actual=\"" +
               keyframeRejected.error + "\"");
    expect(contains(keyframeRejected.error,
                    "source window is not keyframe-safe for packet copy"),
           "keyframe rejection identifies the non-keyframe-safe trim, actual=\"" +
               keyframeRejected.error + "\"");
    expect(keyframeEngine.framesEncoded() == 0,
           "non-keyframe trim is never repaired by re-encoding");
    expect(keyframeEngine.encodePasses() == 0,
           "non-keyframe trim runs zero encode passes");
    expect(!fs::exists(keyframeRejectedOutput),
           "keyframe rejected render does not publish output");

    return failures == 0 ? 0 : 1;
}
