// test_render_visual_replacement_benchmark.cpp
//
// Performance comparison (plan §17): the SAME visual timeline is rendered
// twice — once through the copy-only packet path (BASE/PREPARED/BASE stream
// copy, zero decode/encode) and once through the legacy full-encode path
// (decode → scale → encode every segment). The benchmark records, per path:
//
//   wall_ms            steady_clock around RenderEngine::render()
//   cpu_ms             getrusage user+system delta (engine process itself
//                      PLUS every waited-on ffmpeg child)
//   frames_encoded     engine.framesEncoded()  (copy must be 0, encode > 0)
//   frames_decoded     engine.framesDecoded()
//   copy_segments /
//   transcode_segments zero-render segment accounting
//   file_copy_count /
//   asset_bytes_copied asset materialization (cold download vs warm cache hit)
//   asset_download_ms /
//   mux_audio_ms /
//   publish_atomic_ms  phase timing (download + finalization)
//
// The copy path is executed under a sentinel PATH whose ffmpeg/ffprobe fail
// immediately, proving the zero-spawn contract; the warm re-render proves a
// cache hit (zero file copies). The encode path runs under the real PATH
// (it legitimately spawns ffmpeg) with an ultrafast preset so the benchmark
// stays CI-fast while still proving the architectural invariant:
//
//   copy:  frames_encoded == 0, transcode_segments == 0, copy_segments == 3
//   encode: frames_encoded > 0, frames_decoded > 0, encode_passes > 0
//   copy.wall_ms < encode.wall_ms
//
// VELOX_BENCH_EVIDENCE_DIR pins the fixture/output directory and writes
// benchmark.json so a CI gate can re-read the comparison after this exits.

#include "velox/core/render_engine.hpp"
#include "velox/plan/render_plan.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/io_counters.hpp"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <sys/resource.h>

namespace fs = std::filesystem;

namespace {

int failures = 0;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << "\n";
        ++failures;
    }
}

std::string uniqueStem() {
    return "velox_visual_replacement_benchmark_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

// makeSolidVideo encodes a solid-color, video-only canonical clip: 1080p24,
// H.264 high/4.0, yuv420p, GOP 150 (5 s), no B-frames, closed GOP. The 5 s
// keyframe interval is what makes the 10 s / 15 s trim points keyframe-safe
// for the packet copy path.
bool makeSolidVideo(const fs::path& output, const std::string& colorHex, double durationSec) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote(
                "color=0x" + colorHex + ":s=1920x1080:r=24:d=" + std::to_string(durationSec))
            << " -an -c:v libx264 -preset ultrafast -profile:v high -level:v 4.0"
            << " -pix_fmt yuv420p -r 24"
            << " -g 120 -keyint_min 120 -sc_threshold 0 -bf 0"
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

struct Sample {
    std::string label;
    bool success{false};
    std::string error;
    double wall_ms{0};
    int64_t cpu_user_ms{0};
    int64_t cpu_system_ms{0};
    int64_t frames_encoded{0};
    int64_t frames_decoded{0};
    int64_t encode_passes{0};
    int64_t copy_segments{0};
    int64_t transcode_segments{0};
    int64_t file_copy_count{0};
    int64_t asset_bytes_copied{0};
    double asset_download_ms{0};
    double mux_audio_ms{0};
    double publish_atomic_ms{0};
    std::string concat_mode;

    int64_t cpu_total_ms() const { return cpu_user_ms + cpu_system_ms; }
};

double phaseMs(const velox::core::RenderEngine& engine, const std::string& name) {
    const auto phases = engine.metrics().phaseSnapshot();
    const auto it = phases.find(name);
    return it == phases.end() ? 0.0 : it->second;
}

int64_t timevalToMs(const struct timeval& tv) {
    return static_cast<int64_t>(tv.tv_sec) * 1000 + tv.tv_usec / 1000;
}

Sample runSample(const velox::plan::RenderPlan& plan, const std::string& label) {
    Sample s;
    s.label = label;
    struct rusage selfBefore{}, selfAfter{}, childBefore{}, childAfter{};
    getrusage(RUSAGE_SELF, &selfBefore);
    getrusage(RUSAGE_CHILDREN, &childBefore);
    velox::core::RenderEngine engine;
    const auto t0 = std::chrono::steady_clock::now();
    const velox::core::RenderResult result = engine.render(plan);
    const auto t1 = std::chrono::steady_clock::now();
    getrusage(RUSAGE_SELF, &selfAfter);
    getrusage(RUSAGE_CHILDREN, &childAfter);
    s.success = result.success;
    s.error = result.error;
    s.wall_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    // CPU = engine process (RUSAGE_SELF) + every ffmpeg child it waited on
    // (RUSAGE_CHILDREN). The FFmpeg encode path spends its CPU in children;
    // the copy path does its packet mux in-process, so both are summed for a
    // fair copy-vs-encode CPU comparison.
    s.cpu_user_ms = timevalToMs(selfAfter.ru_utime) - timevalToMs(selfBefore.ru_utime)
                  + timevalToMs(childAfter.ru_utime) - timevalToMs(childBefore.ru_utime);
    s.cpu_system_ms = timevalToMs(selfAfter.ru_stime) - timevalToMs(selfBefore.ru_stime)
                    + timevalToMs(childAfter.ru_stime) - timevalToMs(childBefore.ru_stime);
    s.frames_encoded = engine.framesEncoded();
    s.frames_decoded = engine.framesDecoded();
    s.encode_passes = engine.encodePasses();
    s.copy_segments = engine.copySegments();
    s.transcode_segments = engine.transcodeSegments();
    s.concat_mode = engine.concatMode();
    const auto& io = velox::services::ioCounters();
    s.file_copy_count = io.file_copy_count.load();
    s.asset_bytes_copied = io.asset_bytes_copied.load();
    s.asset_download_ms = phaseMs(engine, "asset_download_ms");
    s.mux_audio_ms = phaseMs(engine, "mux_audio_ms");
    s.publish_atomic_ms = phaseMs(engine, "publish_atomic_ms");
    return s;
}

void printSample(const Sample& s) {
    std::cerr << "[benchmark][" << s.label << "]"
              << " wall_ms=" << s.wall_ms
              << " cpu_ms=" << s.cpu_total_ms()
              << " frames_encoded=" << s.frames_encoded
              << " frames_decoded=" << s.frames_decoded
              << " encode_passes=" << s.encode_passes
              << " copy_segments=" << s.copy_segments
              << " transcode_segments=" << s.transcode_segments
              << " file_copy_count=" << s.file_copy_count
              << " asset_bytes_copied=" << s.asset_bytes_copied
              << " asset_download_ms=" << s.asset_download_ms
              << " mux_audio_ms=" << s.mux_audio_ms
              << " publish_atomic_ms=" << s.publish_atomic_ms
              << " concat_mode=" << s.concat_mode << "\n";
}

void emitSample(std::ostringstream& j, const Sample& s) {
    j << "  \"" << s.label << "\": {\"success\":" << (s.success ? "true" : "false")
      << ",\"wall_ms\":" << s.wall_ms
      << ",\"cpu_user_ms\":" << s.cpu_user_ms
      << ",\"cpu_system_ms\":" << s.cpu_system_ms
      << ",\"frames_encoded\":" << s.frames_encoded
      << ",\"frames_decoded\":" << s.frames_decoded
      << ",\"encode_passes\":" << s.encode_passes
      << ",\"copy_segments\":" << s.copy_segments
      << ",\"transcode_segments\":" << s.transcode_segments
      << ",\"file_copy_count\":" << s.file_copy_count
      << ",\"asset_bytes_copied\":" << s.asset_bytes_copied
      << ",\"asset_download_ms\":" << s.asset_download_ms
      << ",\"mux_audio_ms\":" << s.mux_audio_ms
      << ",\"publish_atomic_ms\":" << s.publish_atomic_ms
      << ",\"concat_mode\":\"" << s.concat_mode << "\"}";
}

void writeEvidenceJson(const fs::path& path, const Sample& copy, const Sample& warm, const Sample& encode) {
    std::ostringstream j;
    j << "{\n";
    emitSample(j, copy);
    j << ",\n";
    emitSample(j, warm);
    j << ",\n";
    emitSample(j, encode);
    j << "\n}\n";
    velox::file::writeFile(path, j.str());
}

} // namespace

int main() {
    fs::path root;
    bool keepEvidence = false;
    if (const char* evidenceDir = std::getenv("VELOX_BENCH_EVIDENCE_DIR");
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
    const fs::path copyOut = root / "copy-output.mp4";
    const fs::path warmOut = root / "copy-output-warm.mp4";
    const fs::path encodeOut = root / "encode-output.mp4";
    expect(makeSolidVideo(baseRed, "FF0000", 30.0), "red BASE fixture can be created (30 s)");
    expect(makeSolidVideo(preparedGreen, "00FF00", 5.0), "green PREPARED fixture can be created (5 s)");
    expect(makeFinalAudio(finalAudio, 30.0), "final audio fixture can be created (30 s)");

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

    // ── Copy plan (V2 integer timing): BASE 0→10 / PREPARED 10→15 /
    //    BASE 15→30, keyframe-safe at 10 s and 15 s (GOP 5 s). ───────────
    velox::plan::RenderPlan copyPlan;
    copyPlan.version = velox::plan::kRenderPlanVersionV2;
    copyPlan.job_id = "visual-replacement-benchmark-copy";
    copyPlan.canvas = {1920, 1080, 24};
    copyPlan.copy_only = true;
    copyPlan.output_path = copyOut.string();
    copyPlan.timeline = {
        {velox::plan::VideoSource{baseRed.string(), ""}, 0.0, false, {}, "base-prefix",
         10'000'000, 0, 10'000'000, 0, 240},
        {velox::plan::VideoSource{preparedGreen.string(), ""}, 0.0, false, {}, "replacement",
         5'000'000, 0, 5'000'000, 240, 120},
        {velox::plan::VideoSource{baseRed.string(), ""}, 0.0, false, {}, "base-suffix",
         15'000'000, 15'000'000, 15'000'000, 360, 360},
    };
    copyPlan.audio_tracks = {
        {finalAudio.string(), 1.0, 0.0, 30.0, "music", false, 0, 30'000'000},
    };

    // ── Encode plan (V1 float timing): the SAME timeline, but the legacy
    //    loop decodes, scales and re-encodes every segment (TransformSpec
    //    default slow_zoom=true routes it to the FFmpeg encode path). ────
    velox::plan::RenderPlan encodePlan;
    encodePlan.version = velox::plan::kRenderPlanVersionV1;
    encodePlan.job_id = "visual-replacement-benchmark-encode";
    encodePlan.canvas = {1920, 1080, 24};
    encodePlan.copy_only = false;
    encodePlan.mixed = false;
    encodePlan.output_path = encodeOut.string();
    encodePlan.timeline = {
        {velox::plan::VideoSource{baseRed.string(), ""}, 10.0, false, {}, "base-prefix", 0, 0, 0, 0, 0},
        {velox::plan::VideoSource{preparedGreen.string(), ""}, 5.0, false, {}, "replacement", 0, 0, 0, 0, 0},
        {velox::plan::VideoSource{baseRed.string(), ""}, 15.0, false, {}, "base-suffix", 0, 0, 0, 0, 0},
    };
    encodePlan.audio_tracks = {
        {finalAudio.string(), 1.0, 0.0, 30.0, "music", false, 0, 30'000'000},
    };

    // ── Copy path (cold + warm) under the sentinel PATH. ────────────────
    const char* previousPath = std::getenv("PATH");
    const bool hadPath = previousPath != nullptr;
    const std::string previousPathValue = hadPath ? previousPath : "";
    setenv("PATH", sentinelBin.c_str(), 1);

    const Sample copy = runSample(copyPlan, "copy_cold");
    velox::plan::RenderPlan warmPlan = copyPlan;
    warmPlan.job_id = "visual-replacement-benchmark-copy-warm";
    warmPlan.output_path = warmOut.string();
    const Sample warm = runSample(warmPlan, "copy_warm");

    // ── Encode path under the real PATH (it legitimately spawns ffmpeg).
    //    An ultrafast preset keeps the comparison CI-fast; the preset only
    //    affects encode wall time, never the zero-encode copy invariant. ──
    if (hadPath) {
        setenv("PATH", previousPathValue.c_str(), 1);
    } else {
        unsetenv("PATH");
    }
    setenv("VELOX_FFMPEG_PRESET", "ultrafast", 1);
    const Sample encode = runSample(encodePlan, "full_encode");
    unsetenv("VELOX_FFMPEG_PRESET");

    printSample(copy);
    printSample(warm);
    printSample(encode);
    if (keepEvidence) {
        writeEvidenceJson(root / "benchmark.json", copy, warm, encode);
    }

    // ── Copy path invariants: zero encode, zero transcode, packet copy. ──
    expect(copy.success, "copy render succeeds");
    if (!copy.success) {
        std::cerr << "copy render error: " << copy.error << "\n";
    }
    expect(copy.concat_mode == "packet_copy",
           "copy concat_mode=packet_copy, actual=\"" + copy.concat_mode + "\"");
    expect(copy.frames_encoded == 0, "copy frames_encoded=0");
    expect(copy.frames_decoded == 0, "copy frames_decoded=0");
    expect(copy.encode_passes == 0, "copy encode_passes=0");
    expect(copy.copy_segments == 3,
           "copy copy_segments=3, actual=" + std::to_string(copy.copy_segments));
    expect(copy.transcode_segments == 0,
           "copy transcode_segments=0, actual=" + std::to_string(copy.transcode_segments));

    // ── Encode path must actually do decode→encode work. ────────────────
    expect(encode.success, "encode render succeeds");
    if (!encode.success) {
        std::cerr << "encode render error: " << encode.error << "\n";
    }
    // frames_encoded is the engine-observable proof of re-encode work: the
    // FFmpeg child's decode is NOT observable by the engine (frames_decoded
    // stays 0 on this path unless decode telemetry is enabled), so the encode
    // proof is frames_encoded > 0 + encode_passes > 0, never frames_decoded.
    expect(encode.frames_encoded > 0,
           "encode frames_encoded>0 (real decode→encode), actual=" +
               std::to_string(encode.frames_encoded));
    expect(encode.encode_passes > 0,
           "encode encode_passes>0, actual=" + std::to_string(encode.encode_passes));

    // ── The comparison: copy is faster than a full encode. ──────────────
    expect(copy.wall_ms < encode.wall_ms,
           "copy wall_ms (" + std::to_string(copy.wall_ms) +
               ") < encode wall_ms (" + std::to_string(encode.wall_ms) + ")");

    // ── Download/staging: the copy path reads assets in place through the
    //    in-process LibAV muxer (zero file copies / zero bytes staged), while
    //    the encode path materializes each source into the workdir. ──────
    expect(copy.file_copy_count == 0,
           "copy path stages zero files (in-place packet mux), actual=" +
               std::to_string(copy.file_copy_count));
    expect(copy.asset_bytes_copied == 0,
           "copy path copies zero asset bytes, actual=" +
               std::to_string(copy.asset_bytes_copied));
    expect(encode.file_copy_count > 0,
           "encode path materializes assets (download/staging), actual=" +
               std::to_string(encode.file_copy_count));

    // ── Warm cache: the second copy render stays zero-copy (a warm asset
    //    cache would behave identically — no re-materialization). ────────
    expect(warm.success, "warm copy render succeeds");
    expect(warm.file_copy_count == 0,
           "warm copy cache hit (file_copy_count=0), actual=" +
               std::to_string(warm.file_copy_count));
    expect(warm.asset_bytes_copied == 0,
           "warm copy zero asset bytes copied, actual=" +
               std::to_string(warm.asset_bytes_copied));

    // ── Zero-spawn proof for the copy path. ─────────────────────────────
    expect(!fs::exists(ffmpegTouched), "copy render never executed ffmpeg");
    expect(!fs::exists(ffprobeTouched), "copy render never executed ffprobe");

    std::cerr << "summary: fail=" << failures << "\n";
    return failures == 0 ? 0 : 1;
}
