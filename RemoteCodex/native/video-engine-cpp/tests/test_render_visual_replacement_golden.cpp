// test_render_visual_replacement_golden.cpp
//
// Golden end-to-end proof of the visual_replacement copy-only path (plan
// §11–§16): a 120 s synthetic BASE (solid red) timeline receives one 5 s
// prepared replacement (solid green) at 60→65 s, plus a 120 s final audio
// track. The copy-only packet mux must assemble
//
//     BASE red 0→60
//     PREPARED green 60→65
//     BASE red 65→120
//
// without decoding or re-encoding a single frame, keep the final audio
// continuous, and publish a valid MP4 whose frames flip red → green → red at
// the exact boundaries. The sentinel PATH proves the whole render stays
// in-process (zero ffmpeg/ffprobe spawns); after the PATH is restored the
// test drives the system ffmpeg/ffprobe to extract boundary frames and run
// the full-decode validation, exactly as an operator would.
//
// The one test that certifies the feature (§16): 120 s base + prepared
// 60→65 + final audio 0→120 + warm cache must produce, simultaneously,
//    59.9 s = BASE (red)
//    60.1 s = PREPARED (green)
//    64.9 s = PREPARED (green)
//    65.1 s = BASE (red)
//    audio continuous, ffprobe valid, full-decode valid
//    cache copies = 0, frames_encoded = 0, frames_decoded = 0
//    byte-stable across 3 warm re-renders
//    transcode started = never.

#include "velox/core/render_engine.hpp"
#include "velox/plan/render_plan.hpp"
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
    return "velox_visual_replacement_golden_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

// makeSolidVideo encodes a solid-color, video-only canonical clip: 1080p30,
// H.264 high/4.0, yuv420p, GOP 150 (5 s), no B-frames, closed GOP. The 5 s
// keyframe interval is what makes the 60 s and 65 s trim points
// keyframe-safe for the packet copy path.
bool makeSolidVideo(const fs::path& output, const std::string& colorHex, double durationSec) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote(
                "color=0x" + colorHex + ":s=1920x1080:r=30:d=" + std::to_string(durationSec))
            << " -an -c:v libx264 -preset ultrafast -profile:v high -level:v 4.0"
            << " -pix_fmt yuv420p -r 30"
            << " -g 150 -keyint_min 150 -sc_threshold 0 -bf 0"
            << " " << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

// makeFinalAudio encodes the continuous 440 Hz final audio (AAC 48 kHz stereo)
// covering the full timeline.
bool makeFinalAudio(const fs::path& output, double durationSec) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote("sine=frequency=440:sample_rate=48000")
            << " -t " << durationSec << " -c:a aac -ar 48000 -ac 2 "
            << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

struct RGB {
    unsigned char r{0};
    unsigned char g{0};
    unsigned char b{0};
};

// extractBoundaryPixel decodes one frame at `seconds` into a 1x1 raw RGB24
// frame (averaging the whole frame) so a solid-color boundary check never
// needs human inspection. Returns false when the extraction fails.
bool extractBoundaryPixel(const fs::path& video, double seconds, RGB* out) {
    const fs::path raw = video.parent_path() / "boundary-pixel.rgb";
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -ss " << seconds
            << " -i " << velox::file::shellQuote(video.string())
            << " -frames:v 1 -vf scale=1:1 -f rawvideo -pix_fmt rgb24 "
            << velox::file::shellQuote(raw.string());
    if (!velox::file::runCommand(command.str())) {
        return false;
    }
    const std::string bytes = velox::file::readFile(raw);
    if (bytes.size() < 3) {
        return false;
    }
    out->r = static_cast<unsigned char>(bytes[0]);
    out->g = static_cast<unsigned char>(bytes[1]);
    out->b = static_cast<unsigned char>(bytes[2]);
    return true;
}

bool isRed(const RGB& c) {
    return c.r > 200 && c.g < 60 && c.b < 60;
}

bool isGreen(const RGB& c) {
    return c.g > 200 && c.r < 60 && c.b < 60;
}

// ffprobeSignature returns the video codec name + audio codec name + format
// duration for the final artifact, so the test can assert the exact
// canonical signature without parsing a JSON tree.
std::string ffprobeSignature(const fs::path& video) {
    std::ostringstream command;
    command << "ffprobe -v error"
            << " -show_entries stream=codec_type,codec_name,width,height,pix_fmt,r_frame_rate"
            << " -show_entries format=duration"
            << " -of default=noprint_wrappers=1 "
            << velox::file::shellQuote(video.string());
    return velox::file::captureCommandOutput(command.str());
}

} // namespace

int main() {
    // VELOX_GOLDEN_EVIDENCE_DIR pins the fixture/output directory so the
    // zero-render CI gate can re-read the sidecar after this test exits.
    // When unset, a fresh temp directory is used and cleaned up.
    fs::path root;
    bool keepEvidence = false;
    if (const char* evidenceDir = std::getenv("VELOX_GOLDEN_EVIDENCE_DIR");
        evidenceDir != nullptr && evidenceDir[0] != '\0') {
        root = fs::path(evidenceDir);
        keepEvidence = true;
    } else {
        root = fs::temp_directory_path() / uniqueStem();
    }
    std::error_code ec;
    fs::create_directories(root, ec);
    expect(!ec, "temporary directory can be created");
    if (ec) return 1;

    struct Cleanup {
        fs::path root;
        bool keep;
        ~Cleanup() {
            if (keep) return;
            std::error_code ec;
            fs::remove_all(root, ec);
        }
    } cleanup{root, keepEvidence};

    // ── Fixtures (system ffmpeg BEFORE the sentinel PATH is installed). ──
    const fs::path baseRed = root / "base-red.mp4";
    const fs::path preparedGreen = root / "prepared-green.mp4";
    const fs::path finalAudio = root / "final-audio.m4a";
    const fs::path output = root / "golden-output.mp4";
    const fs::path warmOutput = root / "golden-output-warm.mp4";
    const fs::path warmOutput2 = root / "golden-output-warm2.mp4";
    const fs::path warmOutput3 = root / "golden-output-warm3.mp4";
    expect(makeSolidVideo(baseRed, "FF0000", 120.0),
           "red BASE fixture can be created (120 s)");
    expect(makeSolidVideo(preparedGreen, "00FF00", 5.0),
           "green PREPARED fixture can be created (5 s)");
    expect(makeFinalAudio(finalAudio, 120.0),
           "final audio fixture can be created (120 s)");

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

    // ── Copy-only V2 plan: BASE/PREPARED/BASE + final audio. ───────────
    // Timeline (frames @30 fps):
    //   red 0→60      source 0,  duration 60 s (frames 0..1800)
    //   green 60→65   source 0,  duration  5 s (frames 1800..1950)
    //   red 65→120    source 65 s, duration 55 s (frames 1950..3600)
    // The 65 s source trim is keyframe-safe because the red base was encoded
    // with a 5 s GOP (keyframes at 0, 5, ..., 60, 65, ...).
    velox::plan::RenderPlan plan;
    plan.version = velox::plan::kRenderPlanVersionV2;
    plan.job_id = "visual-replacement-golden";
    plan.canvas = {1920, 1080, 30};
    plan.copy_only = true;
    plan.output_path = output.string();
    plan.timeline = {
        {velox::plan::VideoSource{baseRed.string(), ""}, 0.0, false, {}, "base-prefix",
         60'000'000, 0, 60'000'000, 0, 1800},
        {velox::plan::VideoSource{preparedGreen.string(), ""}, 0.0, false, {}, "replacement",
         5'000'000, 0, 5'000'000, 1800, 150},
        {velox::plan::VideoSource{baseRed.string(), ""}, 0.0, false, {}, "base-suffix",
         55'000'000, 65'000'000, 55'000'000, 1950, 1650},
    };
    plan.audio_tracks = {
        {finalAudio.string(), 1.0, 0.0, 120.0, "music", false, 0, 120'000'000},
    };

    const char* previousPath = std::getenv("PATH");
    const bool hadPath = previousPath != nullptr;
    const std::string previousPathValue = hadPath ? previousPath : "";
    setenv("PATH", sentinelBin.c_str(), 1);

    velox::core::RenderEngine engine;
    const velox::core::RenderResult result = engine.render(plan);

    // ── Warm-cache determinism (§14/§15): re-render the SAME plan three
    //    times to distinct paths. The assets are already local/cache-bound,
    //    so every warm run must not copy or download anything (cache_miss=0,
    //    download_bytes=0) and must produce byte-identical output.
    struct WarmRun {
        std::string jobID;
        fs::path output;
        velox::core::RenderResult result;
        int64_t fileCopies{0};
        int64_t assetBytesCopied{0};
    };
    WarmRun warmRuns[] = {
        {"visual-replacement-golden-warm-1", warmOutput, {}},
        {"visual-replacement-golden-warm-2", warmOutput2, {}},
        {"visual-replacement-golden-warm-3", warmOutput3, {}},
    };
    for (auto& run : warmRuns) {
        velox::plan::RenderPlan warmPlan = plan;
        warmPlan.job_id = run.jobID;
        warmPlan.output_path = run.output.string();
        velox::core::RenderEngine warmEngine;
        run.result = warmEngine.render(warmPlan);
        const auto& io = velox::services::ioCounters();
        run.fileCopies = io.file_copy_count.load();
        run.assetBytesCopied = io.asset_bytes_copied.load();
    }

    if (hadPath) {
        setenv("PATH", previousPathValue.c_str(), 1);
    } else {
        unsetenv("PATH");
    }

    // ── Render success + zero-render invariants (§16). ─────────────────
    expect(result.success, "golden render succeeds");
    if (!result.success) {
        std::cerr << "render error: " << result.error << "\n";
    }
    expect(engine.concatMode() == "packet_copy",
           "concat_mode is packet_copy, actual=\"" + engine.concatMode() + "\"");
    expect(engine.framesEncoded() == 0, "zero frames encoded");
    expect(engine.framesDecoded() == 0, "zero frames decoded");
    expect(engine.encodePasses() == 0, "zero encode passes");
    expect(engine.tempBytesWritten() == 0, "zero intermediate files written");
    expect(engine.copySegments() == 3,
           "copy_segments == 3 (BASE/PREPARED/BASE), actual=" +
               std::to_string(engine.copySegments()));
    expect(engine.transcodeSegments() == 0,
           "transcode_segments == 0 (no hidden transcode), actual=" +
               std::to_string(engine.transcodeSegments()));
    expect(engine.durationSeconds() > 119.0 && engine.durationSeconds() < 121.0,
           "output duration is ≈120 s");
    expect(!fs::exists(ffmpegTouched), "golden render never executed ffmpeg");
    expect(!fs::exists(ffprobeTouched), "golden render never executed ffprobe");
    expect(fs::exists(output), "golden output is published");

    // ── Zero-render release gate read straight from the sidecar (plan
    //    §16): the CI gate re-reads these exact fields and fails on any
    //    nonzero transcode. ──────────────────────────────────────────────
    {
        const std::string sidecar = velox::file::readFile(output.string() + ".progress.json");
        expect(!sidecar.empty(), "golden sidecar is written");
        expect(contains(sidecar, "\"copy_segments\":3"),
               "sidecar reports copy_segments=3");
        expect(contains(sidecar, "\"transcode_segments\":0"),
               "sidecar reports transcode_segments=0");
        expect(contains(sidecar, "\"frames\":0"),
               "sidecar reports frames_encoded=0");
        expect(contains(sidecar, "\"frames_decoded\":0"),
               "sidecar reports frames_decoded=0");
        expect(contains(sidecar, "\"encode_passes\":0"),
               "sidecar reports encode_passes=0");
        expect(contains(sidecar, "\"concat_mode\":\"packet_copy\""),
               "sidecar reports concat_mode=packet_copy");
    }

    // ── In-process probe: valid video + audio, verified duration. ──────
    const auto probe = velox::media::probeMediaInProcess(output);
    expect(probe.has_value(), "final output can be probed in-process");
    if (probe.has_value()) {
        expect(probe->duration_verified, "final output duration is verified");
        bool hasVideo = false;
        bool hasAudio = false;
        for (const auto& stream : probe->streams) {
            hasVideo = hasVideo || stream.is_video;
            hasAudio = hasAudio || stream.is_audio;
        }
        expect(hasVideo, "final output contains a video stream");
        expect(hasAudio, "final output contains the audio stream");
        expect(probe->duration_seconds > 119.0 && probe->duration_seconds < 121.0,
               "probed duration is ≈120 s (audio continuous to the end)");
    }

    // ── Visual boundaries: extract 1x1 averaged pixels at the exact
    //    transition points and assert red → green → red. ────────────────
    {
        struct Boundary {
            double seconds;
            bool red;
            bool green;
            const char* label;
        };
        const Boundary boundaries[] = {
            {59.9, true, false, "59.9 s = BASE red"},
            {60.1, false, true, "60.1 s = PREPARED green"},
            {64.9, false, true, "64.9 s = PREPARED green"},
            {65.1, true, false, "65.1 s = BASE red"},
        };
        for (const auto& b : boundaries) {
            RGB pixel;
            if (!extractBoundaryPixel(output, b.seconds, &pixel)) {
                expect(false, std::string("boundary frame extract failed at ") + b.label);
                continue;
            }
            if (b.red) {
                expect(isRed(pixel),
                       std::string(b.label) + " (r=" + std::to_string(pixel.r) +
                           " g=" + std::to_string(pixel.g) + " b=" + std::to_string(pixel.b) + ")");
            }
            if (b.green) {
                expect(isGreen(pixel),
                       std::string(b.label) + " (r=" + std::to_string(pixel.r) +
                           " g=" + std::to_string(pixel.g) + " b=" + std::to_string(pixel.b) + ")");
            }
        }
    }

    // ── ffprobe signature: canonical H.264 + AAC, 1080p, 30 fps. ───────
    {
        const std::string signature = ffprobeSignature(output);
        expect(!signature.empty(), "ffprobe produced a readable signature");
        expect(contains(signature, "codec_name=h264"), "video codec is h264");
        expect(contains(signature, "codec_name=aac"), "audio codec is aac");
        expect(contains(signature, "width=1920"), "video width is 1920");
        expect(contains(signature, "height=1080"), "video height is 1080");
        expect(contains(signature, "pix_fmt=yuv420p"), "pixel format is yuv420p");
        expect(contains(signature, "r_frame_rate=30/1"), "frame rate is 30/1");
        if (!signature.empty()) {
            std::cerr << "[golden] ffprobe signature:\n" << signature;
        }
    }

    // ── Full decode validation: -xerror + null sink must exit 0 with no
    //    decode error, corrupt packet, non-monotonic DTS or invalid NAL. ──
    {
        std::ostringstream command;
        command << "ffmpeg -v error -xerror"
                << " -i " << velox::file::shellQuote(output.string())
                << " -f null -";
        expect(velox::file::runCommand(command.str()),
               "full decode validation exits 0 (no decode error)");
    }

    // ── Audio continuity: no silence in the whole track (a dropped or
    //    duplicated replacement audio would leave a silent gap). ─────────
    {
        std::ostringstream command;
        command << "ffmpeg -hide_banner -i " << velox::file::shellQuote(output.string())
                << " -map 0:a:0 -af silencedetect=noise=-50dB:d=0.5 -f null -";
        const std::string silence = velox::file::captureCommandOutput(command.str() + " 2>&1");
        expect(!contains(silence, "silence_start"),
               "final audio is continuous (no silence detected)");
    }

    // ── Warm cache (§14): every warm re-render succeeds with zero copies
    //    (cache_miss=0) and zero asset bytes (download_bytes=0). ─────────
    for (const auto& run : warmRuns) {
        expect(run.result.success, run.jobID + " re-render succeeds");
        if (!run.result.success) {
            std::cerr << run.jobID << " render error: " << run.result.error << "\n";
        }
        expect(run.fileCopies == 0,
               run.jobID + " performs zero file copies (cache_miss=0), actual=" +
                   std::to_string(run.fileCopies));
        expect(run.assetBytesCopied == 0,
               run.jobID + " copies zero asset bytes (download_bytes=0), actual=" +
                   std::to_string(run.assetBytesCopied));
    }

    // ── Determinism (§15): cold + 3 warm outputs are byte-identical. ────
    {
        const auto shaOf = [](const fs::path& p) {
            std::ostringstream cmd;
            cmd << "sha256sum " << velox::file::shellQuote(p.string());
            const std::string out = velox::file::captureCommandOutput(cmd.str());
            // sha256sum prints "<hash>  <path>\n"; the path differs between
            // outputs, so compare only the leading 64-hex digest.
            return out.substr(0, 64);
        };
        const std::string coldSHA = shaOf(output);
        expect(!coldSHA.empty(), "cold output has a non-empty SHA-256 digest");
        for (const auto& run : warmRuns) {
            const std::string warmSHA = shaOf(run.output);
            expect(!warmSHA.empty() && warmSHA == coldSHA,
                   run.jobID + " is byte-identical to the cold output");
            if (!warmSHA.empty() && warmSHA != coldSHA) {
                std::cerr << "cold sha: " << coldSHA << "\n"
                          << run.jobID << " sha: " << warmSHA << "\n";
            }
        }
    }

    std::cerr << "summary: fail=" << failures << "\n";
    return failures == 0 ? 0 : 1;
}
