#pragma once
#include <string>
#include <vector>
#include <filesystem>

namespace velox::media {

struct SceneSegmentParams {
    int width{1920};
    int height{1080};
    int fps{30};
    bool ken_burns{true};
    std::string scale_mode{"cover"}; // cover, contain, stretch
    std::string color_hex{""};
};

double probeMediaDurationSeconds(const std::filesystem::path& mediaPath);
bool buildSceneSegment(const std::filesystem::path& imagePath, const std::filesystem::path& segmentPath, double duration, const SceneSegmentParams& params = {});
bool buildVideoSegment(const std::filesystem::path& clipPath, const std::filesystem::path& segmentPath, double duration, const SceneSegmentParams& params = {});
bool concatSegments(const std::vector<std::filesystem::path>& segments, const std::filesystem::path& outputPath, const std::filesystem::path& workDir);
bool muxAudio(const std::filesystem::path& videoPath, const std::filesystem::path& audioPath, const std::filesystem::path& outputPath, double volume = 1.0);

} // namespace velox::media
