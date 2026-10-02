// test_render_mixed.cpp
//
// Render-level proof that mixed source videos are normalized before assembly.
// Unique inputs are decoded/encoded once into the canonical H.264 profile,
// then reused for every timeline occurrence. Canonical, non-canonical, HEVC,
// and non-keyframe-trimmed inputs must produce decodable output.

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
    const auto& profile = velox::core::canonicalVideoProfileV1();
    velox::media::FramePipelineConfig config;
    config.input_path = input;
    config.output_path = output;
    config.width = profile.width;
    config.height = profile.height;
    config.fps_num = profile.fps_num;
    config.fps_den = profile.fps_den;
    config.source_duration_us = 1'000'000;
    config.codec = profile.codec;
    config.preset = profile.preset;
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
    const fs::path implicitLegacyOutput = root / "implicit-legacy-rejected.mp4";
    const auto& canonicalProfile = velox::core::canonicalVideoProfileV1();
    const int canonicalFps = canonicalProfile.fps_num / canonicalProfile.fps_den;
    expect(makeVideo(nonCanonicalClip, "1280x720", canonicalFps),
           "non-canonical fixture can be created");
    expect(normalizeCanonical(nonCanonicalClip, canonicalClip),
           "canonical fixture can be FramePipeline-normalized");
    expect(makeHevcVideo(hevcClip, "1920x1080", canonicalFps),
           "HEVC fixture can be created");

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
    // Keep each segment on an exact frame boundary for the canonical rate.
    const double segmentDuration = 0.5;
    RenderPlan plan;
    plan.version = 1;
    plan.job_id = "mixed-positive";
    plan.canvas = {canonicalProfile.width, canonicalProfile.height,
                   canonicalProfile.fps_num / canonicalProfile.fps_den};
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
    rejectedPlan.canvas = {canonicalProfile.width, canonicalProfile.height,
                           canonicalProfile.fps_num / canonicalProfile.fps_den};
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
    hevcPlan.canvas = {canonicalProfile.width, canonicalProfile.height,
                       canonicalProfile.fps_num / canonicalProfile.fps_den};
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
    keyframePlan.canvas = {canonicalProfile.width, canonicalProfile.height,
                           canonicalProfile.fps_num / canonicalProfile.fps_den};
    keyframePlan.mixed = true;
    keyframePlan.output_path = keyframeRejectedOutput.string();
    keyframePlan.timeline = {
        {VideoSource{canonicalClip.string(), ""}, segmentDuration, false,
         TransformSpec{"cover", false}, "", 0, 500000},
    };

    // A video plan without an explicit packet mode must fail before the old
    // timeline renderer can turn it into a silent full encode.
    RenderPlan implicitLegacyPlan;
    implicitLegacyPlan.version = 1;
    implicitLegacyPlan.job_id = "implicit-legacy-rejected";
    implicitLegacyPlan.canvas = {canonicalProfile.width, canonicalProfile.height,
                                 canonicalProfile.fps_num / canonicalProfile.fps_den};
    implicitLegacyPlan.output_path = implicitLegacyOutput.string();
    implicitLegacyPlan.timeline = {
        {VideoSource{canonicalClip.string(), ""}, segmentDuration, false,
         TransformSpec{"cover", false}, ""},
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
    velox::core::RenderEngine implicitLegacyEngine;
    const velox::core::RenderResult implicitLegacyRejected =
        implicitLegacyEngine.render(implicitLegacyPlan);

    if (hadPath) {
        setenv("PATH", previousPathValue.c_str(), 1);
    } else {
        unsetenv("PATH");
    }

    // A repeated source is normalized once, then reused by all three cuts.
    expect(result.success, "mixed render normalizes and assembles canonical sources");
    if (!result.success) std::cerr << "render error: " << result.error << "\n";
    expect(engine.concatMode() == "mixed_packet", "mixed render uses the packet mux");
    expect(engine.framesEncoded() > 0, "mixed render encodes normalized source frames");
    expect(engine.framesDecoded() > 0, "mixed render decodes source frames");
    expect(engine.encodePasses() == 1, "repeated source is normalized once");
    expect(engine.copySegments() == static_cast<int64_t>(plan.timeline.size()),
           "normalized timeline cuts are packet copied");
    expect(engine.transcodeSegments() == 0, "normalized timeline cuts remain packet copied");
    expect(engine.normalizedSources() == 1, "one unique source was normalized");
    expect(engine.tempBytesWritten() > 0, "normalized source intermediate is recorded");
    expect(engine.durationSeconds() > 1.49 && engine.durationSeconds() < 1.51,
           "mixed output covers the full 1.5 s timeline");
    expect(!fs::exists(ffmpegTouched), "mixed render keeps normalization in-process");
    expect(!fs::exists(ffprobeTouched), "mixed render does not invoke ffprobe");
    expect(fs::exists(output), "mixed output is published");
    const std::string sidecar = velox::file::readFile(output.string() + ".progress.json");
    expect(contains(sidecar, "\"concat_mode\":\"mixed_packet\""),
           "mixed sidecar records packet assembly");
    expect(contains(sidecar, "\"copy_segments\":3"),
           "mixed sidecar records all timeline cuts");
    expect(contains(sidecar, "\"transcode_segments\":0"),
           "mixed sidecar records no timeline segment re-encodes");
    expect(contains(sidecar, "\"normalized_sources\":1"),
           "mixed sidecar records one unique normalized source");
    expect(contains(sidecar, "\"segments_total\":3"),
           "mixed sidecar records total segment count");
    expect(contains(sidecar, "\"segments_packet_copy\":3"),
           "mixed sidecar records normalized packet-copy cuts");
    expect(contains(sidecar, "\"output_durable\":true"),
           "mixed sidecar confirms durable atomic publication");

    const auto canonical = velox::core::mediaSignatureFromCanonicalProfile(
        velox::core::canonicalVideoProfileV1());
    for (const auto& path : {output, rejectedOutput, hevcRejectedOutput, keyframeRejectedOutput}) {
        velox::media::SegmentProbe probe;
        std::string error;
        expect(velox::media::probeSegmentForExecution(
                   path, 0, velox::media::MediaKind::Video, &probe, &error),
               "normalized mixed output can be probed: " + path.filename().string());
        std::string reason;
        expect(velox::media::mediaSignaturesCompatible(probe.signature, canonical, &reason),
               "normalized output matches canonical profile: " + reason);
        expect(velox::file::runCommand("ffmpeg -hide_banner -loglevel error -i " +
                   velox::file::shellQuote(path.string()) + " -f null -"),
               "normalized output decodes without H.264 reference errors: " +
                   path.filename().string());
    }

    expect(rejected.success, "non-canonical source is normalized");
    expect(rejectedEngine.framesEncoded() > 0, "non-canonical source is re-encoded");
    expect(fs::exists(rejectedOutput), "normalized non-canonical output is published");
    expect(hevcRejected.success, "HEVC source is normalized to H.264");
    expect(hevcEngine.framesEncoded() > 0, "HEVC source is re-encoded");
    expect(fs::exists(hevcRejectedOutput), "normalized HEVC output is published");
    expect(keyframeRejected.success, "non-keyframe trim is decoded before normalization");
    expect(keyframeEngine.framesEncoded() > 0, "trimmed source window is re-encoded");
    expect(fs::exists(keyframeRejectedOutput), "normalized trimmed output is published");

    expect(!implicitLegacyRejected.success,
           "video plan without an explicit packet mode fails closed");
    expect(contains(implicitLegacyRejected.error, "video_renderer_mode_required"),
           "implicit legacy plan reports the explicit renderer-mode error, actual=\"" +
               implicitLegacyRejected.error + "\"");
    expect(implicitLegacyEngine.encodePasses() == 0,
           "implicit legacy plan runs zero encode passes");
    expect(!fs::exists(implicitLegacyOutput),
           "implicit legacy plan does not publish output");

    return failures == 0 ? 0 : 1;
}
