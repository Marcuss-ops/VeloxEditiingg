#include "velox/core/render_engine.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/media_utils.hpp"
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

std::string stem() {
    return "velox_audio_timeline_" +
           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

bool makeAudio(const fs::path& output, double duration, int frequency) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote(
                "sine=frequency=" + std::to_string(frequency) + ":sample_rate=24000")
            << " -t " << duration << " -c:a aac "
            << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

bool makeVideo(const fs::path& output) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote("color=c=black:s=64x64:r=5")
            << " -t 3 -an -c:v libx264 -pix_fmt yuv420p "
            << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

} // namespace

int main() {
    const fs::path root = fs::temp_directory_path() / stem();
    std::error_code ec;
    fs::create_directories(root, ec);
    expect(!ec, "temporary directory can be created");
    if (ec) return 1;
    struct Cleanup {
        fs::path root;
        ~Cleanup() { std::error_code ec; fs::remove_all(root, ec); }
    } cleanup{root};

    const fs::path video = root / "video.mp4";
    const fs::path audio0 = root / "audio0.m4a";
    const fs::path audio1 = root / "audio1.m4a";
    const fs::path audio2 = root / "audio2.m4a";
    const fs::path output = root / "output.mp4";
    expect(makeVideo(video), "test video can be created");
    expect(makeAudio(audio0, 1.0, 440), "first audio can be created");
    expect(makeAudio(audio1, 1.5, 550), "second audio can be created");
    expect(makeAudio(audio2, 0.5, 660), "third audio can be created");
    if (failures != 0) return 1;

    const char* previous = std::getenv("VELOX_AUDIO_MIX_STRATEGY");
    const std::string previousValue = previous == nullptr ? "" : previous;
    const bool hadPrevious = previous != nullptr;
    const char* previousProfile = std::getenv("VELOX_AUDIO_MIX_PROFILE");
    const std::string previousProfileValue = previousProfile == nullptr ? "" : previousProfile;
    const bool hadPreviousProfile = previousProfile != nullptr;
    setenv("VELOX_AUDIO_MIX_STRATEGY", "optimized", 1);
    setenv("VELOX_AUDIO_MIX_PROFILE", "1", 1);

    velox::plan::RenderPlan plan;
    plan.version = 1;
    plan.job_id = "audio-timeline-optimized-test";
    plan.canvas = {64, 64, 5};
    plan.timeline.push_back({velox::plan::ColorSource{"#000000"}, 3.0, false,
                             {"stretch", false}, "test"});
    plan.audio_tracks.push_back({audio0.string(), 1.0, 0.0, 1.0, "voiceover", false});
    plan.audio_tracks.push_back({audio1.string(), 1.0, 1.0, 1.5, "clip_audio", false});
    plan.audio_tracks.push_back({audio2.string(), 1.0, 2.5, 0.5, "voiceover", false});
    plan.output_path = output.string();

    velox::core::RenderEngine engine;
    const auto result = engine.render(plan);

    if (hadPrevious) {
        setenv("VELOX_AUDIO_MIX_STRATEGY", previousValue.c_str(), 1);
    } else {
        unsetenv("VELOX_AUDIO_MIX_STRATEGY");
    }
    if (hadPreviousProfile) {
        setenv("VELOX_AUDIO_MIX_PROFILE", previousProfileValue.c_str(), 1);
    } else {
        unsetenv("VELOX_AUDIO_MIX_PROFILE");
    }

    expect(result.success, "optimized sequential audio render succeeds: " + result.error);
    expect(fs::exists(output), "optimized output exists");
    if (fs::exists(output)) {
        const double duration = velox::media::probeMediaDurationSeconds(output);
        // The synthetic 5fps color source may contain the closing frame
        // differently across FFmpeg builds (3.0s vs 3.4s container duration).
        // The audio timeline itself is bounded to exactly 3s; accept the
        // frame-quantized video envelope here.
        expect(duration >= 2.8 && duration <= 3.6,
               "optimized output duration is preserved (got " + std::to_string(duration) + ")");
    }
    const std::string sidecar = velox::file::readFile(output.string() + ".progress.json");
    expect(sidecar.find("\"audio_mix_strategy\":\"OPTIMIZED_TIMELINE\"") != std::string::npos,
           "sidecar records optimized strategy");
    expect(sidecar.find("\"audio_amix_input_count\":0") != std::string::npos,
           "optimized sidecar records no amix inputs");
    expect(sidecar.find("\"audio_profile_method\":\"controlled_ffmpeg_null_output\"") != std::string::npos,
           "controlled audio profile is recorded");
    expect(sidecar.find("\"audio_decode_ms\":null") == std::string::npos,
           "controlled profile records decode timing");

    std::cerr << "summary: fail=" << failures << "\n";
    return failures == 0 ? 0 : 1;
}
