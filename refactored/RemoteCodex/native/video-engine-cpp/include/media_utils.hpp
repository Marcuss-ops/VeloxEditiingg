#ifndef VELOX_MEDIA_UTILS_HPP
#define VELOX_MEDIA_UTILS_HPP

#include <cmath>
#include <filesystem>
#include <sstream>
#include <string>
#include <vector>

#include "file_utils.hpp"
#include "json_utils.hpp"

namespace fs = std::filesystem;

namespace velox {
namespace media {

inline double probeMediaDurationSeconds(const fs::path& mediaPath) {
    if (mediaPath.empty() || !fs::exists(mediaPath)) {
        return 0.0;
    }
    std::ostringstream cmd;
    cmd << "ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "
        << file::shellQuote(mediaPath.string());
    const std::string output = json::trim(file::captureCommandOutput(cmd.str()));
    if (output.empty() || output == "N/A") {
        return 0.0;
    }
    try {
        const double duration = std::stod(output);
        return duration > 0.0 ? duration : 0.0;
    } catch (...) {
        return 0.0;
    }
}

inline bool buildSceneSegment(const fs::path& imagePath, const fs::path& segmentPath, double duration) {
    std::ostringstream cmd;
    cmd << "ffmpeg -y ";
    if (!imagePath.empty() && fs::exists(imagePath)) {
        const int fps = 30;
        const int frames = std::max(1, static_cast<int>(std::round(duration * fps)));
        const std::string filter =
            "scale=1920:1080:force_original_aspect_ratio=increase,crop=1920:1080,"
            "zoompan=z='min(zoom+0.0008,1.10)':d=" + std::to_string(frames) + ":s=1920x1080:fps=30,"
            "format=yuv420p";
        cmd << "-loop 1 -i " << file::shellQuote(imagePath.string())
            << " -vf " << file::shellQuote(filter)
            << " -frames:v " << frames
            << " -c:v libx264 -pix_fmt yuv420p -r 30 " << file::shellQuote(segmentPath.string());
    } else {
        cmd << "-f lavfi -t " << duration
            << " -i " << file::shellQuote("color=c=black:s=1920x1080")
            << " -c:v libx264 -pix_fmt yuv420p -r 30 " << file::shellQuote(segmentPath.string());
    }
    return file::runCommand(cmd.str());
}

inline bool buildVideoSegment(const fs::path& clipPath, const fs::path& segmentPath, double duration) {
    std::ostringstream cmd;
    cmd << "ffmpeg -y ";
    if (!clipPath.empty() && fs::exists(clipPath)) {
        cmd << "-i " << file::shellQuote(clipPath.string())
            << " -t " << duration
            << " -vf " << file::shellQuote("scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,format=yuv420p")
            << " -c:v libx264 -pix_fmt yuv420p -r 30 -an " << file::shellQuote(segmentPath.string());
    } else {
        return false;
    }
    return file::runCommand(cmd.str());
}

inline bool concatSegments(const std::vector<fs::path>& segments, const fs::path& outputPath, const fs::path& workDir) {
    auto listPath = workDir / "segments.txt";
    std::ostringstream list;
    for (const auto& segment : segments) {
        list << "file " << file::shellQuote(segment.string()) << "\n";
    }
    if (!file::writeFile(listPath, list.str())) {
        return false;
    }
    std::ostringstream cmd;
    cmd << "ffmpeg -y -f concat -safe 0 -i " << file::shellQuote(listPath.string())
        << " -c copy " << file::shellQuote(outputPath.string());
    return file::runCommand(cmd.str());
}

inline bool muxAudio(const fs::path& videoPath, const fs::path& audioPath, const fs::path& outputPath) {
    std::ostringstream cmd;
    cmd << "ffmpeg -y -i " << file::shellQuote(videoPath.string())
        << " -i " << file::shellQuote(audioPath.string())
        << " -c:v copy -c:a aac -shortest " << file::shellQuote(outputPath.string());
    return file::runCommand(cmd.str());
}

} // namespace media
} // namespace velox

#endif // VELOX_MEDIA_UTILS_HPP
