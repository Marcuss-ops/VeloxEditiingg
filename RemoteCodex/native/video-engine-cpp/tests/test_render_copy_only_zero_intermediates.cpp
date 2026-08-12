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
            << " -g 2 -keyint_min 2 -sc_threshold 0 "
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

bool makeNonAacAudio(const fs::path& output) {
    // PCM-in-WAV is NOT FINAL_AUDIO_COPY-compatible: the packet path cannot
    // re-encode, so the render must fail closed instead of silently copying
    // an audio stream that is not the upstream-prepared MP4-AAC final mix.
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i "
            << velox::file::shellQuote("sine=frequency=440:sample_rate=48000")
            << " -t 2.0 -c:a pcm_s16le "
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
    const fs::path nonAacAudio = root / "audio-pcm.wav";
    const fs::path output = root / "final-output.mp4";
    const fs::path rejectedOutput = root / "rejected-output.mp4";
    expect(makeVideo(clipA, "64x64"), "clip A fixture can be created");
    expect(makeVideo(clipB, "64x64"), "clip B fixture can be created");
    expect(makeAudio(audio), "audio fixture can be created");
    expect(makeNonAacAudio(nonAacAudio), "non-AAC audio fixture can be created");

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

    // ── Negative plan: the same timeline but a non-AAC final audio track.
    //    The packet path cannot re-encode, so FINAL_AUDIO_COPY must fail
    //    closed before any packet work (and without spawning media tools).
    velox::plan::RenderPlan rejectedPlan = renderPlan;
    rejectedPlan.job_id = "zero-intermediates-rejected";
    rejectedPlan.output_path = rejectedOutput.string();
    rejectedPlan.audio_tracks = {
        {nonAacAudio.string(), 1.0, 0.0, 2.0 * segmentDuration, "music", false},
    };

    // ── Go-cache wire contract: the worker resolver rewrites velox-asset://
    //    references into verified immutable cache paths before dispatch. The
    //    canonical plan can therefore arrive as url = velox-asset://<id> with
    //    cache_key = the verified local path. The packet path must bind that
    //    path in place and open it directly via libavformat — never copy the
    //    cache file into the C++ workdir. Set up a realistic worker-cache
    //    layout (clips inside a cache directory) and reference them exactly
    //    the way the Go resolver would.
    const fs::path goCacheDir = root / "worker-cache";
    fs::create_directory(goCacheDir, ec);
    expect(!ec, "worker cache directory can be created");
    const fs::path cachedClipA = goCacheDir / "clip-a.mp4";
    const fs::path cachedClipB = goCacheDir / "clip-b.mp4";
    const fs::path cachedAudio = goCacheDir / "audio.m4a";
    fs::copy_file(clipA, cachedClipA, fs::copy_options::overwrite_existing, ec);
    expect(!ec, "cache clip A can be materialized");
    fs::copy_file(clipB, cachedClipB, fs::copy_options::overwrite_existing, ec);
    expect(!ec, "cache clip B can be materialized");
    fs::copy_file(audio, cachedAudio, fs::copy_options::overwrite_existing, ec);
    expect(!ec, "cache audio can be materialized");

    const fs::path cacheOutput = root / "cache-output.mp4";
    velox::plan::RenderPlan cachePlan;
    cachePlan.version = 1;
    cachePlan.job_id = "zero-intermediates-cache-contract";
    cachePlan.canvas = {64, 64, 5};
    cachePlan.copy_only = true;
    cachePlan.output_path = cacheOutput.string();
    // The two cache-wire forms for timeline sources: url as the verified
    // path AND url as the velox-asset:// reference with cache_key carrying
    // the verified path. Audio tracks have no cache_key field in the V1
    // contract; the worker resolver rewrites source_url to the verified
    // local path before dispatch, so the audio uses the local-path form.
    cachePlan.timeline = {
        {velox::plan::VideoSource{cachedClipA.string(), ""}, segmentDuration, false},
        {velox::plan::VideoSource{"velox-asset://cache-clip-b", cachedClipB.string()},
         segmentDuration, false},
    };
    cachePlan.audio_tracks = {
        {cachedAudio.string(), 1.0, 0.0, 2.0 * segmentDuration, "music", false},
    };

    const char* previousPath = std::getenv("PATH");
    const bool hadPath = previousPath != nullptr;
    const std::string previousPathValue = hadPath ? previousPath : "";
    setenv("PATH", sentinelBin.c_str(), 1);

    velox::core::RenderEngine engine;
    const velox::core::RenderResult result = engine.render(renderPlan);
    velox::core::RenderEngine rejectedEngine;
    const velox::core::RenderResult rejected = rejectedEngine.render(rejectedPlan);
    velox::core::RenderEngine cacheEngine;
    const velox::core::RenderResult cacheResult = cacheEngine.render(cachePlan);
    // Capture cache-render counters before the following V2 render resets the
    // process-scoped telemetry. The assertion below is specifically about
    // the cache contract, not about whichever render happened last.
    const int64_t cacheInputOpenCount =
        velox::services::ioCounters().input_open_count.load();

    // ── V2 non-zero source window proof. ───────────────────────────────
    // The parser stores source_in_us/source_duration_us on TimelineItem and
    // RenderEngine must carry them into CopyOnlyVideoSegment unchanged.
    const fs::path v2TrimmedOutput = root / "v2-trimmed-output.mp4";
    velox::plan::RenderPlan v2TrimmedPlan;
    v2TrimmedPlan.version = velox::plan::kRenderPlanVersionV2;
    v2TrimmedPlan.job_id = "v2-non-zero-source-window";
    v2TrimmedPlan.canvas = {64, 64, 5};
    v2TrimmedPlan.copy_only = true;
    v2TrimmedPlan.output_path = v2TrimmedOutput.string();
    v2TrimmedPlan.timeline = {{
        velox::plan::VideoSource{clipA.string(), ""},
        0.0, false, {}, "v2-trimmed-segment", 400'000, 400'000, 400'000, 0, 2,
    }};
    velox::core::RenderEngine v2TrimmedEngine;
    const velox::core::RenderResult v2TrimmedResult = v2TrimmedEngine.render(v2TrimmedPlan);

    if (hadPath) {
        setenv("PATH", previousPathValue.c_str(), 1);
    } else {
        unsetenv("PATH");
    }

    // ── Go-cache binding proof: cache assets opened in place, zero copies. ─
    expect(cacheResult.success, "copy-only render with Go-cache references succeeds");
    if (!cacheResult.success) {
        std::cerr << "cache render error: " << cacheResult.error << "\n";
    }
    expect(cacheEngine.concatMode() == "packet_copy",
           "cache-contract render uses the packet mux, actual=\"" +
               cacheEngine.concatMode() + "\"");
    {
        const auto& cacheIO = velox::services::ioCounters();
        expect(cacheIO.file_copy_count.load() == 0,
               "cache assets are bound in place (zero file copies), actual=" +
                   std::to_string(cacheIO.file_copy_count.load()));
        expect(cacheIO.asset_bytes_copied.load() == 0,
               "zero cache -> tmp asset bytes copied, actual=" +
                   std::to_string(cacheIO.asset_bytes_copied.load()));
        expect(cacheInputOpenCount >= 3,
               "cache assets opened directly by libavformat (video x2 + audio), actual=" +
                   std::to_string(cacheInputOpenCount));
    }
    expect(fs::exists(cacheOutput), "cache-contract output is published");
    expect(!fs::exists(ffmpegTouched), "cache-contract render never executed ffmpeg");
    expect(!fs::exists(ffprobeTouched), "cache-contract render never executed ffprobe");

    // ── FINAL_AUDIO_COPY rejection proof. ────────────────────────────────
    expect(!rejected.success, "non-AAC final audio fails closed");
    if (!rejected.success) {
        expect(rejected.error.find("copy_only final audio is not FINAL_AUDIO_COPY") != std::string::npos,
               "rejection error names the FINAL_AUDIO_COPY gate, actual=\"" +
                   rejected.error + "\"");
        // The decision reason explains which guard rejected the track (e.g.
        // audio_metadata_unverified for raw PCM). The important contract is
        // the fail-closed mode: the packet path never re-encodes.
        expect(rejected.error.find("audio_metadata_unverified") != std::string::npos ||
                   rejected.error.find("audio_codec_not_aac") != std::string::npos ||
                   rejected.error.find("audio_transport_unverified") != std::string::npos,
               "rejection carries a FINAL_AUDIO_COPY decision reason, actual=\"" +
                   rejected.error + "\"");
    }
    expect(!fs::exists(rejectedOutput),
           "rejected plan publishes no output");
    expect(!fs::exists(ffmpegTouched), "rejected plan never executed ffmpeg");
    expect(!fs::exists(ffprobeTouched), "rejected plan never executed ffprobe");

    expect(v2TrimmedResult.success,
           "V2 render with a non-zero source window succeeds on an exact keyframe");
    if (!v2TrimmedResult.success) {
        std::cerr << "V2 trim error: " << v2TrimmedResult.error << "\n";
    }
    const auto v2TrimmedProbe = velox::media::probeMediaInProcess(v2TrimmedOutput);
    expect(v2TrimmedProbe.has_value() && v2TrimmedProbe->duration_verified &&
               v2TrimmedProbe->duration_seconds > 0.25 && v2TrimmedProbe->duration_seconds < 0.55,
           "V2 non-zero source window produces the requested duration");

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
    const std::string cacheSidecarPath = cacheOutput.string() + ".progress.json";
    const std::string cacheSidecar = velox::file::readFile(cacheSidecarPath);
    expect(!cacheSidecar.empty(), "cache-contract sidecar is written");
    if (!cacheSidecar.empty()) {
        expect(contains(cacheSidecar, "\"concat_mode\":\"packet_copy\""),
               "cache-contract sidecar reports packet_copy");
        expect(contains(cacheSidecar, "\"temp_bytes\":0"),
               "cache-contract sidecar reports zero temp bytes");
        expect(contains(cacheSidecar, "packet_mux_ms"),
               "cache-contract sidecar reports the packet_mux phase timing");
        expect(contains(cacheSidecar, "\"file_copy_count\":0"),
               "cache-contract sidecar reports zero file copies");
        expect(contains(cacheSidecar, "\"asset_bytes_copied\":0"),
               "cache-contract sidecar reports zero asset bytes copied");
        expect(contains(cacheSidecar, "\"final_mux_audio_mode\":\"COPY\""),
               "cache-contract sidecar reports FINAL_AUDIO_COPY");
    }

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
        expect(contains(sidecar, "\"external_spawn_count\":0"),
               "engine-declared process counters report zero external spawns (zero-spawn invariant)");
        expect(contains(sidecar, "\"ffmpeg_spawn_count\":0"),
               "engine-declared ffmpeg spawn count is zero on the packet-copy path");
        expect(contains(sidecar, "\"ffprobe_spawn_count\":0"),
               "engine-declared ffprobe spawn count is zero on the packet-copy path");
        expect(contains(sidecar, "\"final_mux_audio_mode\":\"COPY\""),
               "sidecar reports FINAL_AUDIO_COPY for the prepared final audio");
        expect(contains(sidecar, "\"final_mux_audio_encode_passes\":0"),
               "sidecar reports zero audio encode passes (single mux, no AAC re-encode)");
        expect(contains(sidecar, "\"audio_codec\":\"aac\""),
               "sidecar reports the copied AAC audio codec");
        expect(contains(sidecar, "\"decision_reason\":\"verified_final_mix\""),
               "sidecar reports the verified FINAL_AUDIO_COPY reason");
    }

    std::cerr << "summary: fail=" << failures << "\n";
    return failures == 0 ? 0 : 1;
}
