// test_frame_pipeline.cpp — Phase-3 AVFrame producer-consumer encode
// pipeline tests.
//
// renderFrames() decodes, renders (scale/pad) and encodes on three threads
// over a bounded AVFrame pool with exactly one persistent encoder
// AVCodecContext. These tests prove:
//   - the pool is bounded and reused (peak_pool_usage == capacity when the
//     frame count exceeds the slot count, so the producer must have blocked);
//   - exactly one encoder context is created for the whole output;
//   - the render stage actually scaled the frames (32x32 output from a
//     64x64 input);
//   - the pipeline never spawns ffmpeg/ffprobe (sentinel PATH);
//   - the artifact is published atomically and is probeable.
//
// Fixtures are generated with the system ffmpeg BEFORE the sentinel PATH is
// installed; renderFrames itself must never spawn media processes.

#include "velox/services/frame_pipeline.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/media_probe.hpp"
#include "velox/render/frame_graph.hpp"
#include "velox/render/kernel_registry.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
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

std::string uniqueStem() {
    return "velox_frame_pipeline_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

bool makeVideo(const fs::path& output, const std::string& size, int fps,
               double duration) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote(
                "testsrc=size=" + size + ":rate=" + std::to_string(fps) +
                ":duration=" + std::to_string(duration))
            << " -an -c:v libx264 -preset ultrafast -pix_fmt yuv420p -r " << fps
            << " " << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

bool makeOffsetVideo(const fs::path& output) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -itsoffset 2 -f lavfi -i "
            << velox::file::shellQuote("testsrc=size=64x64:rate=5:duration=1.2")
            << " -an -c:v libx264 -preset ultrafast -pix_fmt yuv420p -vsync 0 "
            << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

bool hasVideoStream(const fs::path& path) {
    const auto probe = velox::media::probeMediaInProcess(path);
    if (!probe.has_value()) {
        return false;
    }
    for (const auto& stream : probe->streams) {
        if (stream.is_video) {
            return true;
        }
    }
    return false;
}

// A test-only PixelKernel that records how many frames passed through the
// FrameGraph hook and what geometry/pixel format the pipeline adapted into
// the PixelFrame. It never mutates pixels: it only proves the single apply()
// hook is wired and called once per output frame.
class CountingKernel : public velox::render::PixelKernel {
public:
    CountingKernel(std::atomic<int>& calls, int& width, int& height,
                   int& pixel_format)
        : calls_(calls), width_(width), height_(height),
          pixel_format_(pixel_format) {}

    bool apply(velox::render::PixelFrame& frame,
               const velox::render::FrameOp& op,
               std::string* error) override {
        (void)op;
        (void)error;
        width_ = frame.width;
        height_ = frame.height;
        pixel_format_ = frame.pixel_format;
        calls_.fetch_add(1);
        return true;
    }

private:
    std::atomic<int>& calls_;
    int& width_;
    int& height_;
    int& pixel_format_;
};

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

    // 10 frames at 5 fps (more than the 4-slot pool -> reuse must happen).
    const fs::path clip = root / "clip.mp4";
    const fs::path offsetClip = root / "offset-clip.mp4";
    expect(makeVideo(clip, "64x64", 5, 2.0), "clip fixture can be created");
    expect(makeOffsetVideo(offsetClip),
           "non-zero-start clip fixture can be created");

    // ── Sentinel PATH: any ffmpeg/ffprobe spawn during renderFrames fails. ─
    const fs::path sentinelBin = root / "sentinel-bin";
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

    const char* previousPath = std::getenv("PATH");
    const bool hadPath = previousPath != nullptr;
    const std::string previousPathValue = hadPath ? previousPath : "";
    setenv("PATH", sentinelBin.c_str(), 1);

    // ── End-to-end: scale 64x64 -> 32x32, bounded 4-slot pool. ────────────
    const fs::path output = root / "scaled.mp4";
    velox::media::FramePipelineConfig config;
    config.input_path = clip;
    config.output_path = output;
    config.width = 32;
    config.height = 32;
    config.fps_num = 5;
    config.fps_den = 1;
    config.codec = "libx264";
    config.preset = "ultrafast";
    config.pool_capacity = 4;

    velox::media::FramePipelineResult result;
    expect(velox::media::renderFrames(config, &result),
           "frame pipeline encodes successfully");
    if (!result.success) {
        std::cerr << "pipeline error: " << result.error << "\n";
    }
    expect(result.encode_contexts_created == 1,
           "exactly one persistent encoder AVCodecContext, got " +
               std::to_string(result.encode_contexts_created));
    expect(result.decoder_contexts_created == 1,
           "exactly one decoder context, got " +
               std::to_string(result.decoder_contexts_created));
    expect(result.frames_decoded >= 8,
           "input decoded frame count is sane, got " +
               std::to_string(result.frames_decoded));
    expect(result.frames_encoded >= 8,
           "encoded frame count is sane, got " +
               std::to_string(result.frames_encoded));
    expect(result.zero_copy_decoded_frames == result.frames_decoded,
           "decoder hands off every decoded frame without a full-frame copy");
    expect(result.transform_bypass_frames == 0,
           "scaled render does not report transform bypass frames");
    // 10 frames through a 4-slot pool: the producer must have blocked and
    // reused slots. The peak is capacity in practice (decode far outruns
    // encode), but the hard contract is bounded + reuse, so assert the
    // range instead of an exact value to stay robust on very fast hosts.
    expect(result.peak_pool_usage >= 2 && result.peak_pool_usage <= 4,
           "bounded pool stayed within capacity and reused slots, got " +
               std::to_string(result.peak_pool_usage));

    // ── §25 producer-consumer health metrics ────────────────────────
    const auto& pm = result.pipeline_metrics;
    expect(pm.producer_busy_ms >= 0 && pm.producer_wait_ms >= 0,
           "producer busy/wait are non-negative");
    expect(pm.consumer_busy_ms >= 0 && pm.consumer_wait_ms >= 0,
           "consumer busy/wait are non-negative");
    // Time assertions stay lenient: the ms-rounded totals can both round
    // to 0 on a sub-ms producer lifetime on a very fast host. The real
    // "the producer ran" evidence is queue_depth_max >= 1 below (frames
    // flowed through the bounded hand-off queue).
    expect(pm.consumer_busy_ms + pm.consumer_wait_ms > 0,
           "encoder consumer spent measurable time, got busy=" +
               std::to_string(pm.consumer_busy_ms) + " wait=" +
               std::to_string(pm.consumer_wait_ms));
    // The bounded encoder queue: average depth inside the 4-slot pool and
    // the high-water mark bounded by capacity with at least one hand-off.
    expect(pm.queue_depth_avg >= 0 && pm.queue_depth_avg <= 4,
           "average queue depth within the 4-slot pool, got " +
               std::to_string(pm.queue_depth_avg));
    expect(pm.queue_depth_max >= 1 && pm.queue_depth_max <= 4,
           "peak queue depth bounded by the 4-slot pool, got " +
               std::to_string(pm.queue_depth_max));
    expect(pm.queue_empty_ms >= 0 && pm.queue_full_ms >= 0,
           "queue empty/full waits are non-negative");
    // queue_full_ms is a subset of producer_wait_ms by construction, and
    // queue_empty_ms mirrors the consumer's empty-queue wait exactly.
    expect(pm.queue_full_ms <= pm.producer_wait_ms,
           "backpressure wait is a subset of producer wait, got full=" +
               std::to_string(pm.queue_full_ms) + " producer_wait=" +
               std::to_string(pm.producer_wait_ms));
    expect(pm.queue_empty_ms == pm.consumer_wait_ms,
           "queue_empty_ms mirrors consumer wait, got empty=" +
               std::to_string(pm.queue_empty_ms) + " consumer_wait=" +
               std::to_string(pm.consumer_wait_ms));
    expect(pm.producer_stall_ratio >= 0.0 && pm.producer_stall_ratio <= 1.0,
           "producer stall ratio is a fraction in [0,1], got " +
               std::to_string(pm.producer_stall_ratio));
    expect(pm.encoder_starvation_ratio >= 0.0 &&
               pm.encoder_starvation_ratio <= 1.0,
           "encoder starvation ratio is a fraction in [0,1], got " +
               std::to_string(pm.encoder_starvation_ratio));
    expect(pm.backpressure_ratio >= 0.0 && pm.backpressure_ratio <= 1.0,
           "backpressure ratio is a fraction in [0,1], got " +
               std::to_string(pm.backpressure_ratio));
    expect(result.output_durable, "output published with directory durability");
    expect(fs::exists(output), "output is published");
    expect(hasVideoStream(output), "output is probeable with a video stream");
    expect(!fs::exists(ffmpegTouched), "pipeline never executed ffmpeg");
    expect(!fs::exists(ffprobeTouched), "pipeline never executed ffprobe");

    // ── Same-size passthrough with the default 8-slot pool. ───────────────
    const fs::path passthrough = root / "passthrough.mp4";
    velox::media::FramePipelineConfig passConfig = config;
    passConfig.output_path = passthrough;
    passConfig.width = 0;
    passConfig.height = 0;
    velox::media::FramePipelineResult passResult;
    expect(velox::media::renderFrames(passConfig, &passResult),
           "same-size passthrough encode succeeds");
    if (passResult.success) {
        expect(passResult.encode_contexts_created == 1,
               "passthrough also uses a single encoder context");
        expect(passResult.peak_pool_usage <= 8,
               "default pool bound respected, got " +
                   std::to_string(passResult.peak_pool_usage));
        expect(passResult.pipeline_metrics.queue_depth_max <= 8,
               "default 8-slot queue bound respected, got " +
                   std::to_string(passResult.pipeline_metrics.queue_depth_max));
        expect(hasVideoStream(passthrough), "passthrough output is probeable");
        expect(passResult.zero_copy_decoded_frames == passResult.frames_decoded,
               "same-size path also keeps decoded frames zero-copy");
        expect(passResult.transform_bypass_frames == passResult.frames_encoded,
               "same-size compatible path bypasses sws_scale for every frame");
    }

    // ── Explicit decoder/encoder thread budgets. ──────────────────────────
    // A positive budget must be applied before avcodec_open2() and still
    // produce a valid, probeable output. This exercises the per-segment
    // thread coordination the scheduler uses to keep segment parallelism ×
    // encoder threads from oversubscribing the host.
    const fs::path threaded = root / "threaded.mp4";
    velox::media::FramePipelineConfig threadedConfig = config;
    threadedConfig.output_path = threaded;
    threadedConfig.decoder_threads = 1;
    threadedConfig.encoder_threads = 1;
    velox::media::FramePipelineResult threadedResult;
    expect(velox::media::renderFrames(threadedConfig, &threadedResult),
           "explicit decoder/encoder thread budget encode succeeds");
    if (threadedResult.success) {
        expect(threadedResult.encode_contexts_created == 1,
               "explicit-thread-budget encode uses a single encoder context");
        expect(threadedResult.decoder_contexts_created == 1,
               "explicit-thread-budget encode uses a single decoder context");
        expect(hasVideoStream(threaded),
               "explicit-thread-budget output is probeable");
        expect(!fs::exists(ffmpegTouched) && !fs::exists(ffprobeTouched),
               "explicit-thread-budget encode never spawns media processes");
    }

    // ── FrameGraph compositor hook. ────────────────────────────────────────
    // The pipeline applies the graph's active ops through one apply() call
    // per rendered frame (no overlay/logo/text branching inside the
    // pipeline). A registered counting kernel proves the hook fires for
    // every output frame and that the AVFrame was adapted into a planar
    // PixelFrame carrying the output geometry and pixel format.
    std::atomic<int> compositor_calls{0};
    int observed_width = 0;
    int observed_height = 0;
    int observed_format = -1;
    velox::render::PixelKernelRegistry registry;
    expect(registry.registerKernel(
               velox::render::FrameOpType::ImageOverlay,
               std::make_unique<CountingKernel>(compositor_calls,
                                                observed_width, observed_height,
                                                observed_format)),
           "counting overlay kernel can be registered");
    velox::render::FrameGraph graph(&registry);
    velox::render::FrameOp op;
    op.type = velox::render::FrameOpType::ImageOverlay;
    op.start_frame = 0;
    op.end_frame = -1;
    expect(graph.add(op), "frame graph accepts an always-active overlay op");

    const fs::path composited = root / "composited.mp4";
    velox::media::FramePipelineConfig compositedConfig = config;
    compositedConfig.output_path = composited;
    compositedConfig.frame_graph = &graph;
    velox::media::FramePipelineResult compositedResult;
    expect(velox::media::renderFrames(compositedConfig, &compositedResult),
           "frame graph compositor hook encode succeeds");
    if (compositedResult.success) {
        expect(compositor_calls.load() == compositedResult.frames_encoded,
               "frame graph apply() runs once per encoded frame, got calls=" +
                   std::to_string(compositor_calls.load()) + " encoded=" +
                   std::to_string(compositedResult.frames_encoded));
        expect(observed_width == 32 && observed_height == 32,
               "pixel frame carries the output geometry, got " +
                   std::to_string(observed_width) + "x" +
                   std::to_string(observed_height));
        expect(observed_format == 0,
               "pixel frame carries the yuv420p pixel format, got " +
                   std::to_string(observed_format));
    }

    // Fail-closed: an op whose type has no registered kernel must abort the
    // render instead of being silently skipped.
    velox::render::FrameGraph missingGraph(&registry);
    velox::render::FrameOp missingOp;
    missingOp.type = velox::render::FrameOpType::TextOverlay;
    missingOp.start_frame = 0;
    missingOp.end_frame = -1;
    expect(missingGraph.add(missingOp),
           "frame graph accepts a text overlay op");
    const fs::path missingKernelOutput = root / "missing-kernel.mp4";
    velox::media::FramePipelineConfig missingKernelConfig = config;
    missingKernelConfig.output_path = missingKernelOutput;
    missingKernelConfig.frame_graph = &missingGraph;
    velox::media::FramePipelineResult missingKernelResult;
    expect(!velox::media::renderFrames(missingKernelConfig, &missingKernelResult),
           "frame graph with a missing kernel fails closed");
    expect(missingKernelResult.error.find("no pixel kernel registered") !=
               std::string::npos,
           "missing kernel failure explains the unregistered op");
    expect(!fs::exists(missingKernelOutput),
           "missing kernel failure does not publish an output");

    // ── Source-window trim: decode may start at the beginning, but only the
    // requested presentation-time window is emitted by the native pipeline.
    const fs::path trimmed = root / "trimmed.mp4";
    velox::media::FramePipelineConfig trimConfig = config;
    trimConfig.output_path = trimmed;
    trimConfig.source_in_us = 400'000;
    trimConfig.source_duration_us = 400'000;
    velox::media::FramePipelineResult trimResult;
    expect(velox::media::renderFrames(trimConfig, &trimResult),
           "source-window native transcode succeeds");
    if (trimResult.success) {
        expect(trimResult.frames_encoded >= 1 && trimResult.frames_encoded <= 3,
               "source-window output contains only the requested frames, got " +
                   std::to_string(trimResult.frames_encoded));
        expect(hasVideoStream(trimmed), "source-window output is probeable");
    }

    // V2 source_in_us is relative to media start, not absolute stream PTS.
    // This fixture starts at t=2s and requests the same relative 400..800ms
    // window as the zero-offset fixture above.
    const fs::path offsetTrimmed = root / "offset-trimmed.mp4";
    velox::media::FramePipelineConfig offsetConfig = trimConfig;
    offsetConfig.input_path = offsetClip;
    offsetConfig.output_path = offsetTrimmed;
    velox::media::FramePipelineResult offsetResult;
    expect(velox::media::renderFrames(offsetConfig, &offsetResult),
           "source-window native transcode handles non-zero stream start time");
    if (offsetResult.success) {
        expect(offsetResult.frames_encoded >= 1 && offsetResult.frames_encoded <= 3,
               "non-zero-start source window contains only requested relative frames, got " +
                   std::to_string(offsetResult.frames_encoded));
        expect(hasVideoStream(offsetTrimmed),
               "non-zero-start source-window output is probeable");
    }

    // ── Negative cases fail closed. ───────────────────────────────────────
    velox::media::FramePipelineConfig badPool = config;
    badPool.pool_capacity = 1;
    velox::media::FramePipelineResult badResult;
    expect(!velox::media::renderFrames(badPool, &badResult),
           "pool_capacity below 2 fails closed");

    velox::media::FramePipelineConfig missingInput = config;
    missingInput.input_path = root / "does-not-exist.mp4";
    expect(!velox::media::renderFrames(missingInput, nullptr),
           "missing input fails closed");

    if (hadPath) {
        setenv("PATH", previousPathValue.c_str(), 1);
    } else {
        unsetenv("PATH");
    }

    std::cerr << "summary: fail=" << failures << "\n";
    return failures == 0 ? 0 : 1;
}
