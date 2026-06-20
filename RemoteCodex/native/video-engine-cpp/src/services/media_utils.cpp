#include "velox/services/media_utils.hpp"
#include "velox/services/file_utils.hpp"
#include "json_utils.hpp"
#include <cmath>
#include <cstdlib>
#include <sstream>
#include <iostream>

namespace fs = std::filesystem;

namespace velox::media {

static std::string envString(const char* name, const std::string& fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr) {
        return fallback;
    }
    std::string trimmed = json::trim(value);
    return trimmed.empty() ? fallback : trimmed;
}

static int envInt(const char* name, int fallback) {
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

static std::string ffmpegVideoCodec() {
    return envString("VELOX_FFMPEG_VCODEC", "libx264");
}

static bool codecContains(const std::string& codec, const std::string& needle) {
    return codec.find(needle) != std::string::npos;
}

static std::string ffmpegVideoPresetForCodec(const std::string& codec) {
    const std::string overrideValue = envString("VELOX_FFMPEG_PRESET", "");
    if (!overrideValue.empty()) {
        return overrideValue;
    }
    if (codecContains(codec, "x264") || codecContains(codec, "x265")) {
        return "veryfast";
    }
    return "";
}

static std::string ffmpegVideoTuneForCodec(const std::string& codec, bool stillImage) {
    const std::string overrideValue = envString("VELOX_FFMPEG_TUNE", "");
    if (!overrideValue.empty()) {
        return overrideValue;
    }
    if (stillImage && (codecContains(codec, "x264") || codecContains(codec, "x265"))) {
        return "stillimage";
    }
    return "";
}

static std::string ffmpegVideoExtraArgs() {
    return envString("VELOX_FFMPEG_VENC_ARGS", "");
}

static int ffmpegThreadCount() {
    return envInt("VELOX_FFMPEG_THREADS", 0);
}

static std::string ffmpegRateControlArgsForCodec(const std::string& codec) {
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

static void appendFfmpegVideoEncodingArgs(
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

double probeMediaDurationSeconds(const fs::path& mediaPath) {
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

bool buildSceneSegment(const fs::path& imagePath, const fs::path& segmentPath, double duration, const SceneSegmentParams& params) {
    std::ostringstream cmd;
    cmd << "ffmpeg -y -hide_banner -loglevel error ";
    const int w = params.width > 0 ? params.width : 1920;
    const int h = params.height > 0 ? params.height : 1080;
    const int fps = params.fps > 0 ? params.fps : 30;
    const std::string res = std::to_string(w) + "x" + std::to_string(h);
    const std::string size = std::to_string(w) + ":" + std::to_string(h);

    if (!imagePath.empty() && fs::exists(imagePath)) {
        const int frames = std::max(1, static_cast<int>(std::round(duration * fps)));

        std::string scaleFilter;
        if (params.scale_mode == "contain") {
            scaleFilter = "scale=" + size + ":force_original_aspect_ratio=decrease,pad=" + size + ":(ow-iw)/2:(oh-ih)/2,format=yuv420p";
        } else if (params.scale_mode == "stretch") {
            scaleFilter = "scale=" + size + ",format=yuv420p";
        } else {
            // cover (default)
            scaleFilter = "scale=" + size + ":force_original_aspect_ratio=increase,crop=" + size + ",format=yuv420p";
        }

        std::string filter;
        if (params.slow_zoom) {
            // Slow gradual zoom-in: starts at 1.0x, ends at ~1.08x over the duration.
            // Uses zoompan with a gentle linear ramp — no panning, pure center zoom.
            filter = scaleFilter
                + ",zoompan=z='1+0.08*on/(" + std::to_string(frames) + ")'"
                  ":x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'"
                  ":d=" + std::to_string(frames)
                  + ":s=" + res + ":fps=" + std::to_string(fps)
                + ",format=yuv420p";
        } else {
            filter = scaleFilter;
        }

        cmd << "-stream_loop -1 -i " << file::shellQuote(imagePath.string())
            << " -vf " << file::shellQuote(filter)
            << " -frames:v " << frames
            << ffmpegRateControlArgsForCodec(ffmpegVideoCodec())
            << " -pix_fmt yuv420p -r " << fps;
        appendFfmpegVideoEncodingArgs(
            cmd,
            ffmpegVideoCodec(),
            ffmpegVideoPresetForCodec(ffmpegVideoCodec()),
            ffmpegVideoTuneForCodec(ffmpegVideoCodec(), true),
            ffmpegThreadCount(),
            "");
        cmd << " " << file::shellQuote(segmentPath.string());
    } else {
        std::string bgColor = "black";
        if (!params.color_hex.empty()) {
            std::string hex = params.color_hex;
            if (!hex.empty() && hex[0] == '#') hex = hex.substr(1);
            bgColor = "0x" + hex;
        }
        cmd << "-f lavfi -t " << duration
            << " -i " << file::shellQuote("color=c=" + bgColor + ":s=" + res)
            << ffmpegRateControlArgsForCodec(ffmpegVideoCodec())
            << " -pix_fmt yuv420p -r " << fps;
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

bool buildVideoSegment(const fs::path& clipPath, const fs::path& segmentPath, double duration, const SceneSegmentParams& params) {
    std::ostringstream cmd;
    cmd << "ffmpeg -y -hide_banner -loglevel error ";
    const int w = params.width > 0 ? params.width : 1920;
    const int h = params.height > 0 ? params.height : 1080;
    const int fps = params.fps > 0 ? params.fps : 30;
    const std::string size = std::to_string(w) + ":" + std::to_string(h);

    if (!clipPath.empty() && fs::exists(clipPath)) {
        std::string scaleFilter;
        if (params.scale_mode == "cover") {
            scaleFilter = "scale=" + size + ":force_original_aspect_ratio=increase,crop=" + size + ",format=yuv420p";
        } else if (params.scale_mode == "stretch") {
            scaleFilter = "scale=" + size + ",format=yuv420p";
        } else {
            // contain (default for video clips) — fit within canvas, pad edges
            scaleFilter = "scale=" + size + ":force_original_aspect_ratio=decrease,pad=" + size + ":(ow-iw)/2:(oh-ih)/2,format=yuv420p";
        }

        cmd << "-i " << file::shellQuote(clipPath.string())
            << " -t " << duration
            << " -vf " << file::shellQuote(scaleFilter)
            << ffmpegRateControlArgsForCodec(ffmpegVideoCodec())
            << " -pix_fmt yuv420p -r " << fps << " -an";
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

bool concatSegments(const std::vector<fs::path>& segments, const fs::path& outputPath, const fs::path& workDir) {
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

bool muxAudio(const fs::path& videoPath, const fs::path& audioPath, const fs::path& outputPath, double volume, double startOffset) {
    std::ostringstream cmd;
    cmd << "ffmpeg -y -hide_banner -loglevel error -i " << file::shellQuote(videoPath.string())
        << " -i " << file::shellQuote(audioPath.string())
        << " -map 0:v:0 -map 1:a:0 -c:v copy -c:a aac";

    // Build audio filter chain: volume + optional delay
    std::ostringstream af;
    bool hasFilter = false;
    if (volume > 0.0 && volume != 1.0) {
        af << "volume=" << volume;
        hasFilter = true;
    }
    if (startOffset > 0.0) {
        int delayMs = static_cast<int>(startOffset * 1000);
        if (hasFilter) af << ",";
        af << "adelay=" << delayMs << "|" << delayMs;
        hasFilter = true;
    }
    if (hasFilter) {
        cmd << " -af " << file::shellQuote(af.str());
    }

    cmd << " -shortest -movflags +faststart "
        << file::shellQuote(outputPath.string());
    return file::runCommand(cmd.str());
}

} // namespace velox::media
