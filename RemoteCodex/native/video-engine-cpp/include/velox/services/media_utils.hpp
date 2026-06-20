#pragma once
#include <string>
#include <vector>
#include <filesystem>

namespace velox::media {

double probeMediaDurationSeconds(const std::filesystem::path& mediaPath);
bool buildSceneSegment(const std::filesystem::path& imagePath, const std::filesystem::path& segmentPath, double duration);
bool buildVideoSegment(const std::filesystem::path& clipPath, const std::filesystem::path& segmentPath, double duration);
bool concatSegments(const std::vector<std::filesystem::path>& segments, const std::filesystem::path& outputPath, const std::filesystem::path& workDir);
bool muxAudio(const std::filesystem::path& videoPath, const std::filesystem::path& audioPath, const std::filesystem::path& outputPath);

} // namespace velox::media
