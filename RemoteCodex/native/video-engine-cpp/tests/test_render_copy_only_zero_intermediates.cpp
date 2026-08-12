// test_render_copy_only_zero_intermediates.cpp
//
// Render-level proof of the Zero-Spawn Copy Pipeline contract:
//
//   copy_only render (VELOX_ENABLE_LIBAV=ON)
//     external_ffmpeg_processes    = 0
//     external_ffprobe_processes   = 0
//     temporary_segment_files      = 0   (no segment_N.mp4)
//     temporary_video_files        = 0   (no video_only.mp4, no segments.txt)
//     temp_bytes_written           = 0
//     asset_cache_copies           = 0   (no cache -> tmp copies)
//     mux_passes                   = 1   (packets written straight into the
//                                         final muxer)
//
// The engine is exercised through the public RenderEngine::render() entry
// point with a sentinel PATH whose ffmpeg/ffprobe fail immediately. Any
// child-process media call would trip the sentinel and fail the test. All
// assertions run after the full render lifecycle, so the sidecar telemetry
// (concat_mode, phase timings, io counters) reflects the real job.

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
    return "velox_zero_intermediates_" +
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

    // ── Fixtures (generated with the system ffmpeg BEFORE the sentinel
    //    PATH is installed; the render itself must never spawn media
    //    processes). ─────────────────────────────────────────────────────
    const fs::path clipA = root / "clip-a.mp4";
    const fs::path clipB = root / "clip-b.mp4";
    const fs::path audio = root / "audio.m4a";
    const fs::path output = root / "final-output.mp4";
    expect(makeVideo(clipA, "64x64"), "clip A fixture can be created");
    expect(makeVideo(clipB, "64x64"), "clip B fixture can be created");
    expect(makeAudio(audio), "audio fixture can be created");

    // ── Sentinel PATH: any ffmpeg/ffprobe spawn fails hard. ─────────────
    const fs::path sentinelBin = root / "sentinel-bin";
    fs::create_directory(sentinelBin, ec);
    expect(!ec, "sentinel PATH directory can be created");
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

    // ── Copy-only RenderPlan: two 0.8 s clips + one prepared audio track.
    const double segmentDuration = 0.8;
    velox::plan::RenderPlan renderPlan;
    renderPlan.version = 1;
    renderPlan.job_id = "zero-intermediates-test";
    renderPlan.canvas = {64, 64, 5};
    renderPlan.copy_only = true;
    renderPlan.output_path = output.string();
    renderPlan.timeline = {
        {velox::plan::VideoSource{clipA.string(), ""}, segmentDuration, false},
        {velox::plan::VideoSource{clipB.string(), ""}, segmentDuration, false},
    };
    renderPlan.audio_tracks = {
        {audio.string(), 1.0, 0.0, 2.0 * segmentDuration, "music", false},
    };

    const char* previousPath = std::getenv("PATH");
    const bool hadPath = previousPath != nullptr;
    const std::string previousPathValue = hadPath ? previousPath : "";
    setenv("PATH", sentinelBin.c_str(), 1);

    velox::core::RenderEngine engine;
    const velox::core::RenderResult result = engine.render(renderPlan);

    if (hadPath) {
        setenv("PATH", previousPathValue.c_str(), 1);
    } else {
        unsetenv("PATH");
    }

    // ── Render lifecycle assertions. ─────────────────────────────────────
    expect(result.success, "copy-only render succeeds");
    if (!result.success) {
        std::cerr << "render error: " << result.error << "\n";
    }
    expect(engine.concatMode() == "packet_copy",
           "concat_mode is packet_copy (in-process packet mux), actual=\"" +
               engine.concatMode() + "\"");

    // The legacy segment/concat path accumulates temp bytes for every
    // segment_*.mp4 and the final video_only.mp4. Zero here means none of
    // those intermediates ever hit disk.
    expect(engine.tempBytesWritten() == 0,
           "zero temp bytes written (no segment_N.mp4, no video_only.mp4), actual=" +
               std::to_string(engine.tempBytesWritten()));
    expect(engine.encodePasses() == 0, "zero encode passes");
    expect(engine.framesEncoded() == 0, "zero frames encoded");
    expect(engine.framesDecoded() == 0, "zero frames decoded");
    expect(engine.durationSeconds() > 1.5 && engine.durationSeconds() < 1.7,
           "declared duration matches the two 0.8 s segments");

    // Process-scoped I/O counters (reset at the start of render()): local
    // assets must be bound in place — the canonical worker cache is opened
    // directly by libavformat, never copied into the C++ temp dir.
    const auto& io = velox::services::ioCounters();
    expect(io.file_copy_count.load() == 0,
           "zero file copies (no cache -> tmp asset copies), actual=" +
               std::to_string(io.file_copy_count.load()));
    expect(io.asset_bytes_copied.load() == 0,
           "zero asset bytes copied, actual=" +
               std::to_string(io.asset_bytes_copied.load()));
    expect(io.input_open_count.load() >= 2,
           "video inputs opened in-process by libavformat, actual=" +
               std::to_string(io.input_open_count.load()));

    // ── Zero-spawn proof. ────────────────────────────────────────────────
    expect(!fs::exists(ffmpegTouched), "render never executed ffmpeg");
    expect(!fs::exists(ffprobeTouched), "render never executed ffprobe");

    // ── Output: published atomically, valid MP4 with video + audio. ─────
    expect(fs::exists(output), "final output is published");
    // makePartialPath() names atomic partials `<stem>.partial.<pid>.<nonce>.mp4`;
    // a directory scan (not a fixed "output.partial" guess) proves no
    // in-progress mux partial survived publication.
    {
        bool leftoverPartial = false;
        for (const auto& entry : fs::directory_iterator(root)) {
            const std::string name = entry.path().filename().string();
            if (name.rfind("final-output.partial.", 0) == 0) {
                leftoverPartial = true;
                std::cerr << "leftover partial: " << name << "\n";
            }
        }
        expect(!leftoverPartial, "no atomic mux partial survives publication");
    }
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
    }

    // ── Sidecar telemetry: the job is reported as a single packet mux,
    //    with no concat / segment / mux-audio phases. ─────────────────────
    const std::string sidecarPath = output.string() + ".progress.json";
    const std::string sidecar = velox::file::readFile(sidecarPath);
    expect(!sidecar.empty(), "progress sidecar is written");
    if (!sidecar.empty()) {
        expect(contains(sidecar, "\"concat_mode\":\"packet_copy\""),
               "sidecar reports concat_mode packet_copy");
        expect(contains(sidecar, "\"temp_bytes\":0"),
               "sidecar reports zero temp bytes");
        expect(contains(sidecar, "packet_mux_ms"),
               "sidecar reports the packet_mux phase timing");
        expect(!contains(sidecar, "concat_ms"),
               "sidecar has no concat phase (no concat FFmpeg)");
        expect(!contains(sidecar, "segment_build_ms"),
               "sidecar has no segment build phase (no per-segment ffmpeg)");
        expect(!contains(sidecar, "mux_audio_ms"),
               "sidecar has no final mux phase (one in-process mux)");
        expect(contains(sidecar, "\"file_copy_count\":0"),
               "sidecar io counters report zero file copies");
    }

    std::cerr << "summary: fail=" << failures << "\n";
    return failures == 0 ? 0 : 1;
}
