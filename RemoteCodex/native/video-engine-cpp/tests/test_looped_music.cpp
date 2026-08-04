#include "velox/core/render_engine.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/media_utils.hpp"
#include "json_utils.hpp"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>

namespace fs = std::filesystem;

namespace {

int g_fail = 0;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << "\n";
        ++g_fail;
    }
}

std::string uniqueStem() {
    return "velox_looped_music_" +
           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

bool makeThirtySecondAudio(const fs::path& output) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote("sine=frequency=440:sample_rate=48000")
            << " -t 30 -c:a aac " << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

bool hasResidualFfmpegForOutput(const fs::path& output) {
    // Restrict the assertion to this test's unique output path. A global
    // `pgrep ffmpeg` would be flaky when another local test is rendering.
    const std::string processes = velox::file::captureCommandOutput("command -v pgrep >/dev/null 2>&1 && pgrep -af ffmpeg");
    return processes.find(output.string()) != std::string::npos;
}

void expectNoResidualFfmpeg(const fs::path& output) {
    // Allow the child process table to settle, but fail if pgrep is absent:
    // a missing process inspector must never turn this safety assertion into
    // a false pass.
    const std::string pgrepCheck = velox::file::captureCommandOutput("command -v pgrep 2>/dev/null");
    expect(!pgrepCheck.empty(), "pgrep is available for residual-process verification");
    for (int attempt = 0; attempt < 10; ++attempt) {
        if (!hasResidualFfmpegForOutput(output)) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    expect(false, "no ffmpeg process for this render remains after completion");
}

void testImplicitDurationLoopTerminates() {
    const fs::path root = fs::temp_directory_path() / uniqueStem();
    std::error_code ec;
    fs::create_directories(root, ec);
    expect(!ec, "temporary test directory can be created");
    if (ec) {
        return;
    }

    const fs::path audio = root / "short-music.m4a";
    const fs::path output = root / "rendered.mp4";
    struct Cleanup {
        fs::path root;
        ~Cleanup() {
            std::error_code ec;
            fs::remove_all(root, ec);
        }
    } cleanup{root};

    expect(makeThirtySecondAudio(audio), "30-second source music can be generated");
    if (!fs::exists(audio)) {
        return;
    }

    velox::plan::RenderPlan plan;
    plan.version = 1;
    plan.job_id = "looped-music-implicit-duration-95s";
    plan.canvas = {64, 64, 5};
    plan.timeline.push_back({velox::plan::ColorSource{"#112233"}, 95.0, false, {"stretch", false}});
    // The duration is intentionally omitted (zero). The engine must bind
    // loop=true to the final 95-second video duration.
    plan.audio_tracks.push_back({audio.string(), 1.0, 0.0, 0.0, "background_music", true});
    plan.output_path = output.string();

    velox::core::RenderEngine engine;
    const auto result = engine.render(plan);
    expect(result.success, "RenderEngine succeeds with looped music without duration_seconds");
    expect(fs::exists(output), "rendered output exists");
    expect(engine.durationSeconds() == 95.0, "engine reports the 95-second final video duration");

    const double outputDuration = velox::media::probeMediaDurationSeconds(output);
    expect(outputDuration >= 94.5 && outputDuration <= 95.5,
           "output duration remains bounded near 95 seconds (got " +
               std::to_string(outputDuration) + ")");

    // The render call is synchronous. This assertion catches the regression
    // where an unbounded -stream_loop/-amix graph leaves ffmpeg running after
    // the worker believes the task is complete.
    expectNoResidualFfmpeg(output);
}

} // namespace

int main() {
    std::cerr << "running looped_music tests\n";
    testImplicitDurationLoopTerminates();
    std::cerr << "summary: fail=" << g_fail << "\n";
    return g_fail == 0 ? 0 : 1;
}
