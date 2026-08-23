#include "velox/services/media_utils.hpp"
#include "json_utils.hpp"
#include "media_utils_internal.hpp"

#ifdef VELOX_ENABLE_LIBAV
#include "velox/services/media_probe.hpp"
#include "velox/services/segment_execution.hpp"
#include "velox/services/segment_execution_libav.hpp"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavutil/pixfmt.h>
}
#endif

#include <algorithm>
#include <cmath>
#include <sstream>

namespace fs = std::filesystem;

namespace velox::media {

#ifdef VELOX_ENABLE_LIBAV

bool detail::nativeVideoStreamCopyCompatible(const fs::path& clip_path, int width, int height, int fps, double requested_duration) {
    if (clip_path.empty() || !fs::exists(clip_path) || requested_duration <= 0.0) return false;
    const auto probe = probeMediaInProcess(clip_path);
    if (!probe.has_value()) return false;
    const auto video = std::find_if(probe->streams.begin(), probe->streams.end(),
        [](const MediaProbeStream& stream) { return stream.is_video; });
    if (video == probe->streams.end()) return false;

    SegmentProbe segment_probe;
    if (!probeSegmentForExecution(clip_path, 0, MediaKind::Video, &segment_probe, nullptr)) return false;
    MediaSignature target;
    target.kind = MediaKind::Video;
    target.codec_id = static_cast<int>(AV_CODEC_ID_H264);
    target.pixel_format = static_cast<int>(AV_PIX_FMT_YUV420P);
    target.width = width;
    target.height = height;
    target.frame_rate_num = fps;
    target.frame_rate_den = 1;
    std::string compatibility_reason;
    if (!mediaSignaturesCompatible(segment_probe.signature, target, &compatibility_reason)) return false;
    const double source_duration = probe->duration_verified ? probe->duration_seconds : video->duration_seconds;
    return source_duration > 0.0 && requested_duration <= source_duration + 0.05;
}

bool hasAudioStream(const fs::path& media_path) {
    const auto probe = probeMediaInProcess(media_path);
    if (!probe.has_value()) return false;
    return std::any_of(probe->streams.begin(), probe->streams.end(),
        [](const MediaProbeStream& stream) { return stream.is_audio; });
}

#else

namespace {

bool parseFrameRate(const std::string& value, double& output) {
    const auto slash = value.find('/');
    try {
        if (slash == std::string::npos) output = std::stod(value);
        else {
            const double numerator = std::stod(value.substr(0, slash));
            const double denominator = std::stod(value.substr(slash + 1));
            if (denominator == 0.0) return false;
            output = numerator / denominator;
        }
    } catch (...) {
        return false;
    }
    return output > 0.0;
}

} // namespace

bool detail::nativeVideoStreamCopyCompatible(const fs::path& clip_path, int width, int height, int fps, double requested_duration) {
    if (clip_path.empty() || !fs::exists(clip_path) || requested_duration <= 0.0) return false;
    std::ostringstream probe;
    probe << "ffprobe -v error -select_streams v:0 -show_entries stream=codec_name,width,height,avg_frame_rate,pix_fmt -of csv=p=0 "
          << file::shellQuote(clip_path.string());
    const std::string output = json::trim(file::captureCommandOutput(probe.str()));
    if (output.empty()) return false;
    std::istringstream fields(output);
    std::string codec, width_text, height_text, pixel_format, frame_rate_text;
    if (!std::getline(fields, codec, ',') || !std::getline(fields, width_text, ',') ||
        !std::getline(fields, height_text, ',') || !std::getline(fields, pixel_format, ',') ||
        !std::getline(fields, frame_rate_text, ',')) return false;
    double source_fps = 0.0;
    int source_width = 0;
    int source_height = 0;
    try {
        source_width = std::stoi(width_text);
        source_height = std::stoi(height_text);
    } catch (...) {
        return false;
    }
    if (!parseFrameRate(frame_rate_text, source_fps)) return false;
    if (json::trim(codec) != "h264" || json::trim(pixel_format) != "yuv420p" ||
        source_width != width || source_height != height ||
        std::abs(source_fps - static_cast<double>(fps)) > 0.001) return false;
    const double source_duration = probeMediaDurationSeconds(clip_path);
    return source_duration > 0.0 && requested_duration <= source_duration + 0.05;
}

bool hasAudioStream(const fs::path& media_path) {
    if (media_path.empty() || !fs::exists(media_path)) return false;
    std::ostringstream command;
    command << "ffprobe -v error -select_streams a:0 -show_entries stream=index -of csv=p=0 "
            << file::shellQuote(media_path.string());
    return !json::trim(file::captureCommandOutput(command.str())).empty();
}

#endif

} // namespace velox::media
