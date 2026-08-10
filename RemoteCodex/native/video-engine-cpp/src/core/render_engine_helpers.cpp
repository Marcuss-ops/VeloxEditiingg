#include "render_engine_helpers.hpp"

#include "velox/services/file_utils.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>

namespace velox::core::render_detail {

void reportProgress(int percent, const std::string& stage) {
    std::cerr << "{\"progress\":" << percent
              << ",\"percent\":" << percent
              << ",\"stage\":\"" << stage << "\"}" << std::endl;
}

void reportDetailedProgress(const services::EngineProgress& progress,
                            int scene, int total_scenes,
                            int segment, int total_segments,
                            const std::string& phase,
                            int64_t frames_encoded,
                            int64_t frames_decoded,
                            int64_t frames_composited,
                            int64_t elapsed_ms,
                            bool segment_completed) {
    std::cerr << "{\"progress\":" << static_cast<int>(progress.progress_pct)
              << ",\"percent\":" << static_cast<int>(progress.progress_pct)
              << ",\"scene\":" << scene
              << ",\"total_scenes\":" << total_scenes
              << ",\"segment\":" << segment
              << ",\"total_segments\":" << total_segments
              << ",\"segment_completed\":" << (segment_completed ? "true" : "false")
              << ",\"stage\":\"" << phase << "\""
              << ",\"phase\":\"" << phase << "\""
              << ",\"frames_encoded\":" << frames_encoded
              << ",\"frames_decoded\":" << frames_decoded
              << ",\"frames_composited\":" << frames_composited
              << ",\"speed_x\":" << progress.speed_x
              << ",\"elapsed_ms\":" << elapsed_ms
              << "}" << std::endl;
}

media::SceneSegmentParams makeParams(
    const plan::CanvasSpec& canvas,
    const plan::TransformSpec& transform,
    const std::string& color_hex) {
    media::SceneSegmentParams p;
    p.width = canvas.width;
    p.height = canvas.height;
    p.fps = canvas.fps;
    p.slow_zoom = transform.slow_zoom;
    p.scale_mode = transform.scale_mode;
    p.color_hex = color_hex;
    return p;
}

std::string extractColorHex(const plan::MediaSource& source) {
    if (std::holds_alternative<plan::ColorSource>(source)) {
        return std::get<plan::ColorSource>(source).color_hex;
    }
    return "";
}

int64_t fileSize(const fs::path& path) {
    std::error_code ec;
    auto size = fs::file_size(path, ec);
    return ec ? 0 : static_cast<int64_t>(size);
}

int64_t decodedFramesFromShowInfo(const std::string& stderr_out) {
    int64_t max_frame = -1;
    size_t cursor = 0;
    while ((cursor = stderr_out.find(" n:", cursor)) != std::string::npos) {
        cursor += 3;
        while (cursor < stderr_out.size() &&
               std::isspace(static_cast<unsigned char>(stderr_out[cursor]))) {
            ++cursor;
        }
        size_t end = cursor;
        while (end < stderr_out.size() &&
               std::isdigit(static_cast<unsigned char>(stderr_out[end]))) {
            ++end;
        }
        if (end > cursor) {
            try {
                max_frame = std::max(
                    max_frame,
                    static_cast<int64_t>(std::stoll(stderr_out.substr(cursor, end - cursor))));
            } catch (...) {
            }
        }
        cursor = end;
    }
    return max_frame >= 0 ? max_frame + 1 : 0;
}

bool runFfmpegSegmentWithProgress(
    const std::string& full_cmd,
    const services::ProgressCallback& callback,
    int64_t expected_duration_us,
    int64_t& decoded_frames) {
    if (!callback) {
        return file::runCommand(full_cmd);
    }
    std::string stderr_out;
    int exit_code = 0;
    bool ok = services::runFfmpegCapturingProgress(
        full_cmd,
        fs::current_path(),
        callback,
        expected_duration_us,
        stderr_out,
        exit_code);
    decoded_frames = decodedFramesFromShowInfo(stderr_out);
    if (!ok || exit_code != 0) {
        std::cerr << "ffmpeg failed (exit=" << exit_code << "): "
                  << stderr_out << std::endl;
    }
    return ok && exit_code == 0;
}

std::string composeSegmentCmd(const std::string& args_only) {
    const char* telemetry = std::getenv("VELOX_FFMPEG_DECODE_TELEMETRY");
    const bool enabled = telemetry != nullptr &&
        (std::string(telemetry) != "0" && std::string(telemetry) != "false");
    return std::string("ffmpeg -y -hide_banner -loglevel ") +
        (enabled ? "info" : "error") + " -progress pipe:1 -nostats " + args_only;
}

bool burnSubtitleTrack(
    const fs::path& input_video,
    const fs::path& subtitle_file,
    const fs::path& output_video) {
    std::ostringstream filter;
    filter << "subtitles=" << file::shellQuote(subtitle_file.string());

    std::ostringstream cmd;
    cmd << "ffmpeg -y -hide_banner -loglevel error"
        << " -i " << file::shellQuote(input_video.string())
        << " -vf " << file::shellQuote(filter.str())
        << " -c:v libx264 -preset veryfast -crf 20"
        << " -pix_fmt yuv420p -map 0:v:0 -map 0:a? -c:a aac -ar 48000 -ac 2 "
        << file::shellQuote(output_video.string());
    file::CommandResult result = file::runCommandTimed(cmd.str());
    std::cerr << "{\"metric\":\"ffmpeg.subtitle_burn_ms\",\"value\":" << result.wall_ms
              << ",\"ok\":" << (result.ok ? "true" : "false")
              << ",\"exit_code\":" << result.exit_code << "}" << std::endl;
    return result.ok;
}

} // namespace velox::core::render_detail
