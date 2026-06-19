#ifndef VELOX_MEDIA_UTILS_HPP
#define VELOX_MEDIA_UTILS_HPP

// Utility per operazioni multimediali tramite ffprobe/ffmpeg:
//   - Rilevamento durata (audio/video)
//   - Generazione segmenti video da immagini (con zoompan effect)
//   - Concatenazione segmenti
//   - Muxing audio su video

#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <sstream>
#include <string>
#include <vector>

#include "file_utils.hpp"
#include "json_utils.hpp"

namespace fs = std::filesystem;

namespace velox {
namespace media {

inline std::string envString(const char* name, const std::string& fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr) {
        return fallback;
    }
    std::string trimmed = json::trim(value);
    return trimmed.empty() ? fallback : trimmed;
}

inline int envInt(const char* name, int fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr) {
        return fallback;
    }
    try {
        const int parsed = std::stoi(json::trim(value));
        return parsed > 0 ? parsed : fallback;
    } catch (...) {
        return fallback;
    }
}

inline std::string ffmpegVideoCodec() {
    return envString("VELOX_FFMPEG_VCODEC", "libx264");
}

inline bool codecContains(const std::string& codec, const std::string& needle) {
    return codec.find(needle) != std::string::npos;
}

inline std::string ffmpegVideoPresetForCodec(const std::string& codec) {
    const std::string overrideValue = envString("VELOX_FFMPEG_PRESET", "");
    if (!overrideValue.empty()) {
        return overrideValue;
    }
    if (codecContains(codec, "x264") || codecContains(codec, "x265")) {
        return "veryfast";
    }
    return "";
}

inline std::string ffmpegVideoTuneForCodec(const std::string& codec, bool stillImage) {
    const std::string overrideValue = envString("VELOX_FFMPEG_TUNE", "");
    if (!overrideValue.empty()) {
        return overrideValue;
    }
    if (stillImage && (codecContains(codec, "x264") || codecContains(codec, "x265"))) {
        return "stillimage";
    }
    return "";
}

inline std::string ffmpegVideoExtraArgs() {
    return envString("VELOX_FFMPEG_VENC_ARGS", "");
}

inline int ffmpegThreadCount() {
    return envInt("VELOX_FFMPEG_THREADS", 0);
}

inline std::string ffmpegRateControlArgsForCodec(const std::string& codec) {
    if (codecContains(codec, "nvenc")) {
        return " -rc constqp -qp 23";
    }
    if (codecContains(codec, "vaapi")) {
        return " -qp 23";
    }
    if (codecContains(codec, "qsv")) {
        return " -global_quality 23";
    }
    return " -crf 20";
}

inline void appendFfmpegVideoEncodingArgs(
    std::ostringstream& cmd,
    const std::string& codec,
    const std::string& preset,
    const std::string& tune,
    int threads,
    const std::string& extraArgs
) {
    const std::string selectedCodec = codec.empty() ? ffmpegVideoCodec() : codec;
    cmd << " -c:v " << selectedCodec;
    if (!preset.empty()) {
        cmd << " -preset " << preset;
    }
    if (!tune.empty()) {
        cmd << " -tune " << tune;
    }
    if (threads > 0) {
        cmd << " -threads " << threads;
    }
    const std::string selectedExtra = extraArgs.empty() ? ffmpegVideoExtraArgs() : extraArgs;
    if (!selectedExtra.empty()) {
        cmd << " " << selectedExtra;
    }
}

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
    cmd << "ffmpeg -y -hide_banner -loglevel error ";
    if (!imagePath.empty() && fs::exists(imagePath)) {
        const int fps = 30;
        const int frames = std::max(1, static_cast<int>(std::round(duration * fps)));
        const std::string filter =
            "scale=1920:1080:force_original_aspect_ratio=increase,crop=1920:1080,"
            "zoompan=z='min(zoom+0.0008,1.10)':d=" + std::to_string(frames) + ":s=1920x1080:fps=30,"
            "format=yuv420p";
        cmd << "-stream_loop -1 -i " << file::shellQuote(imagePath.string())
            << " -vf " << file::shellQuote(filter)
            << " -frames:v " << frames
            << ffmpegRateControlArgsForCodec(ffmpegVideoCodec())
            << " -pix_fmt yuv420p -r 30";
        appendFfmpegVideoEncodingArgs(
            cmd,
            ffmpegVideoCodec(),
            ffmpegVideoPresetForCodec(ffmpegVideoCodec()),
            ffmpegVideoTuneForCodec(ffmpegVideoCodec(), true),
            ffmpegThreadCount(),
            "");
        cmd << " " << file::shellQuote(segmentPath.string());
    } else {
        cmd << "-f lavfi -t " << duration
            << " -i " << file::shellQuote("color=c=black:s=1920x1080")
            << ffmpegRateControlArgsForCodec(ffmpegVideoCodec())
            << " -pix_fmt yuv420p -r 30";
        appendFfmpegVideoEncodingArgs(
            cmd,
            ffmpegVideoCodec(),
            ffmpegVideoPresetForCodec(ffmpegVideoCodec()),
            ffmpegVideoTuneForCodec(ffmpegVideoCodec(), true),
            ffmpegThreadCount(),
            "");
        cmd << " " << file::shellQuote(segmentPath.string());
    }
    return file::runCommand(cmd.str());
}

inline bool buildVideoSegment(const fs::path& clipPath, const fs::path& segmentPath, double duration) {
    std::ostringstream cmd;
    cmd << "ffmpeg -y -hide_banner -loglevel error ";
    if (!clipPath.empty() && fs::exists(clipPath)) {
        cmd << "-i " << file::shellQuote(clipPath.string())
            << " -t " << duration
            << " -vf " << file::shellQuote("scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,format=yuv420p")
            << ffmpegRateControlArgsForCodec(ffmpegVideoCodec())
            << " -pix_fmt yuv420p -r 30 -an";
        appendFfmpegVideoEncodingArgs(
            cmd,
            ffmpegVideoCodec(),
            ffmpegVideoPresetForCodec(ffmpegVideoCodec()),
            ffmpegVideoTuneForCodec(ffmpegVideoCodec(), false),
            ffmpegThreadCount(),
            "");
        cmd << " " << file::shellQuote(segmentPath.string());
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
    cmd << "ffmpeg -y -hide_banner -loglevel error -f concat -safe 0 -i " << file::shellQuote(listPath.string())
        << " -c copy " << file::shellQuote(outputPath.string());
    return file::runCommand(cmd.str());
}

inline bool muxAudio(const fs::path& videoPath, const fs::path& audioPath, const fs::path& outputPath) {
    std::ostringstream cmd;
    cmd << "ffmpeg -y -hide_banner -loglevel error -i " << file::shellQuote(videoPath.string())
        << " -i " << file::shellQuote(audioPath.string())
        << " -map 0:v:0 -map 1:a:0 -c:v copy -c:a aac -shortest -movflags +faststart "
        << file::shellQuote(outputPath.string());
    return file::runCommand(cmd.str());
}

} // namespace media
} // namespace velox

#endif // VELOX_MEDIA_UTILS_HPP
