#pragma once
#include <string>
#include <vector>
#include <filesystem>

namespace velox::media {

struct SceneSegmentParams {
    int width{1920};
    int height{1080};
    int fps{30};
    bool slow_zoom{true};
    std::string scale_mode{"cover"}; // cover, contain, stretch
    std::string color_hex{""};
};

double probeMediaDurationSeconds(const std::filesystem::path& mediaPath);

// ─── F5: args-only builders ─────────────────────────────────────────────
//
// These return the FFmpeg ARGUMENTS string only — NO `ffmpeg` prefix,
// NO leading global flags like `-y` / `-hide_banner` / `-loglevel`.
//
// Callers prepend their own global flags. This lets
// `render_engine.cpp` inject `-progress pipe:1 -nostats` cleanly.
std::string buildSceneSegmentArgs(
    const std::filesystem::path& imagePath,
    const std::filesystem::path& segmentPath,
    double duration,
    const SceneSegmentParams& params);

std::string buildVideoSegmentArgs(
    const std::filesystem::path& clipPath,
    const std::filesystem::path& segmentPath,
    double duration,
    const SceneSegmentParams& params);

std::string buildColorSegmentArgs(
    const std::filesystem::path& segmentPath,
    double duration,
    const SceneSegmentParams& params,
    const std::string& color_hex);

// ─── Existing execution wrappers (shell-prepend "ffmpeg" + runCommand) ──
//
// These preserve the legacy surface used by `cmd_full_video.cpp`. They
// internally delegate to the *Args builders and add the canonical
// global flags.
bool buildSceneSegment(const std::filesystem::path& imagePath,
                       const std::filesystem::path& segmentPath,
                       double duration,
                       const SceneSegmentParams& params = {});

bool buildVideoSegment(const std::filesystem::path& clipPath,
                       const std::filesystem::path& segmentPath,
                       double duration,
                       const SceneSegmentParams& params = {});

bool concatSegments(const std::vector<std::filesystem::path>& segments,
                    const std::filesystem::path& outputPath,
                    const std::filesystem::path& workDir);

bool muxAudio(const std::filesystem::path& videoPath,
              const std::filesystem::path& audioPath,
              const std::filesystem::path& outputPath,
              double volume = 1.0,
              double startOffset = 0.0);

} // namespace velox::media
