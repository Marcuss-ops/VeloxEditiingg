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

// Resolution/canvas helpers shared by all three builders.
static void canvasDims(const SceneSegmentParams& p, int& w, int& h, int& fps) {
    w = p.width > 0 ? p.width : 1920;
    h = p.height > 0 ? p.height : 1080;
    fps = p.fps > 0 ? p.fps : 30;
}

static std::string scaleFilterString(const std::string& scale_mode,
                                      const std::string& size,
                                      const std::string& res) {
    std::string filter;
    if (scale_mode == "contain") {
        filter = "scale=" + size + ":force_original_aspect_ratio=decrease,pad=" + size + ":(ow-iw)/2:(oh-ih)/2,format=yuv420p";
    } else if (scale_mode == "stretch") {
        filter = "scale=" + size + ",format=yuv420p";
    } else {
        // cover (default) — for image sources.
        filter = "scale=" + size + ":force_original_aspect_ratio=increase,crop=" + size + ",format=yuv420p";
    }
    (void)res;
    return filter;
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

// ─── F5: args-only builders (canonical, the others are wrappers) ──────

std::string buildColorSegmentArgs(
    const fs::path& segmentPath,
    double duration,
    const SceneSegmentParams& params,
    const std::string& color_hex
) {
    int w, h, fps;
    canvasDims(params, w, h, fps);
    const std::string res = std::to_string(w) + "x" + std::to_string(h);

    std::string bgColor = "black";
    if (!color_hex.empty()) {
        std::string hex = color_hex;
        if (!hex.empty() && hex[0] == '#') hex = hex.substr(1);
        bgColor = "0x" + hex;
    }

    std::ostringstream cmd;
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
    return cmd.str();
}

std::string buildSceneSegmentArgs(
    const fs::path& imagePath,
    const fs::path& segmentPath,
    double duration,
    const SceneSegmentParams& params
) {
    int w, h, fps;
    canvasDims(params, w, h, fps);
    const std::string res = std::to_string(w) + "x" + std::to_string(h);
    const std::string size = std::to_string(w) + ":" + std::to_string(h);
    const int frames = std::max(1, static_cast<int>(std::round(duration * fps)));

    std::string scaleFilter = scaleFilterString(params.scale_mode, size, res);

    std::string filter;
    if (params.slow_zoom) {
        // Slow gradual zoom-in: starts at 1.0x, ends at ~1.08x.
        filter = scaleFilter
            + ",zoompan=z='1+0.08*on/(" + std::to_string(frames) + ")'"
              ":x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'"
              ":d=" + std::to_string(frames)
              + ":s=" + res + ":fps=" + std::to_string(fps)
            + ",format=yuv420p";
    } else {
        filter = scaleFilter;
    }

    std::ostringstream cmd;
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
    return cmd.str();
}

std::string buildVideoSegmentArgs(
    const fs::path& clipPath,
    const fs::path& segmentPath,
    double duration,
    const SceneSegmentParams& params
) {
    int w, h, fps;
    canvasDims(params, w, h, fps);
    const std::string size = std::to_string(w) + ":" + std::to_string(h);

    // contain (default for clips) — fit + pad
    std::string scale_filter = "scale=" + size + ":force_original_aspect_ratio=decrease,pad=" + size + ":(ow-iw)/2:(oh-ih)/2,format=yuv420p";
    if (params.scale_mode == "cover") {
        scale_filter = "scale=" + size + ":force_original_aspect_ratio=increase,crop=" + size + ",format=yuv420p";
    } else if (params.scale_mode == "stretch") {
        scale_filter = "scale=" + size + ",format=yuv420p";
    }

    std::ostringstream cmd;
    // Narrated scenes can be much longer than the source stock clip. Loop
    // the source so the visual bed covers the whole requested scene; -t
    // still bounds the generated segment exactly to that scene duration.
    cmd << "-stream_loop -1 -i " << file::shellQuote(clipPath.string())
        << " -t " << duration
        << " -vf " << file::shellQuote(scale_filter)
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
    return cmd.str();
}

// ─── Execution wrappers (preserve legacy surface used by cmd_full_video) ──

bool buildSceneSegment(const fs::path& imagePath, const fs::path& segmentPath, double duration, const SceneSegmentParams& params) {
    if (!imagePath.empty() && fs::exists(imagePath)) {
        const std::string args = buildSceneSegmentArgs(imagePath, segmentPath, duration, params);
        const std::string cmd = "ffmpeg -y -hide_banner -loglevel error " + args;
        file::CommandResult r = file::runCommandTimed(cmd);
        std::cerr << "{\"metric\":\"ffmpeg.scene_segment_ms\",\"value\":" << r.wall_ms
                  << ",\"ok\":" << (r.ok ? "true" : "false")
                  << ",\"exit_code\":" << r.exit_code << "}" << std::endl;
        return r.ok;
    }
    const std::string args = buildColorSegmentArgs(segmentPath, duration, params, params.color_hex);
    const std::string cmd = "ffmpeg -y -hide_banner -loglevel error " + args;
    file::CommandResult r = file::runCommandTimed(cmd);
    std::cerr << "{\"metric\":\"ffmpeg.color_segment_ms\",\"value\":" << r.wall_ms
              << ",\"ok\":" << (r.ok ? "true" : "false")
              << ",\"exit_code\":" << r.exit_code << "}" << std::endl;
    return r.ok;
}

bool buildVideoSegment(const fs::path& clipPath, const fs::path& segmentPath, double duration, const SceneSegmentParams& params) {
    if (clipPath.empty() || !fs::exists(clipPath)) {
        return false;
    }
    const std::string args = buildVideoSegmentArgs(clipPath, segmentPath, duration, params);
    const std::string cmd = "ffmpeg -y -hide_banner -loglevel error " + args;
    file::CommandResult r = file::runCommandTimed(cmd);
    std::cerr << "{\"metric\":\"ffmpeg.clip_segment_ms\",\"value\":" << r.wall_ms
              << ",\"ok\":" << (r.ok ? "true" : "false")
              << ",\"exit_code\":" << r.exit_code << "}" << std::endl;
    return r.ok;
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
    file::CommandResult r = file::runCommandTimed(cmd.str());
    std::cerr << "{\"metric\":\"ffmpeg.concat_ms\",\"value\":" << r.wall_ms
              << ",\"ok\":" << (r.ok ? "true" : "false")
              << ",\"exit_code\":" << r.exit_code << "}" << std::endl;
    return r.ok;
}

bool muxAudio(const fs::path& videoPath, const fs::path& audioPath, const fs::path& outputPath, double volume, double startOffset) {
    std::ostringstream cmd;
    cmd << "ffmpeg -y -hide_banner -loglevel error -i " << file::shellQuote(videoPath.string())
        << " -i " << file::shellQuote(audioPath.string())
        << " -map 0:v:0 -map 1:a:0 -c:v copy -c:a aac";

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
    file::CommandResult r = file::runCommandTimed(cmd.str());
    std::cerr << "{\"metric\":\"ffmpeg.mux_audio_ms\",\"value\":" << r.wall_ms
              << ",\"ok\":" << (r.ok ? "true" : "false")
              << ",\"exit_code\":" << r.exit_code << "}" << std::endl;
    return r.ok;
}

} // namespace velox::media
