#include "velox/services/media_utils.hpp"
#include "json_utils.hpp"
#include "media_utils_internal.hpp"

#ifdef VELOX_ENABLE_LIBAV
#include "velox/services/media_probe.hpp"
#include "velox/services/segment_execution.hpp"
#include "velox/services/segment_execution_libav.hpp"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavcodec/codec_id.h>
#include <libavutil/pixfmt.h>
}
#endif

#include <algorithm>
#include <cmath>
#include <cctype>
#include <sstream>

namespace fs = std::filesystem;

namespace velox::media {

#ifdef VELOX_ENABLE_LIBAV

double probeMediaDurationSeconds(const fs::path& media_path) {
    const auto probe = probeMediaInProcess(media_path);
    if (!probe.has_value() || !probe->duration_verified) return 0.0;
    return probe->duration_seconds;
}

bool detail::nativeVideoStreamCopyCompatible(
    const fs::path& clip_path,
    int width,
    int height,
    int fps,
    double requested_duration) {
    if (clip_path.empty() || !fs::exists(clip_path) || requested_duration <= 0.0) {
        return false;
    }
    const auto probe = probeMediaInProcess(clip_path);
    if (!probe.has_value()) return false;
    const auto video = std::find_if(
        probe->streams.begin(), probe->streams.end(),
        [](const MediaProbeStream& stream) { return stream.is_video; });
    if (video == probe->streams.end()) return false;

    SegmentProbe segment_probe;
    if (!probeSegmentForExecution(
            clip_path, 0, MediaKind::Video, &segment_probe, nullptr)) {
        return false;
    }
    MediaSignature target;
    target.kind = MediaKind::Video;
    target.codec_id = static_cast<int>(AV_CODEC_ID_H264);
    target.pixel_format = static_cast<int>(AV_PIX_FMT_YUV420P);
    target.width = width;
    target.height = height;
    target.frame_rate_num = fps;
    target.frame_rate_den = 1;
    std::string compatibility_reason;
    if (!mediaSignaturesCompatible(
            segment_probe.signature, target, &compatibility_reason)) {
        return false;
    }
    const double source_duration = probe->duration_verified
        ? probe->duration_seconds : video->duration_seconds;
    return source_duration > 0.0 && requested_duration <= source_duration + 0.05;
}

bool hasAudioStream(const fs::path& media_path) {
    const auto probe = probeMediaInProcess(media_path);
    if (!probe.has_value()) return false;
    return std::any_of(
        probe->streams.begin(), probe->streams.end(),
        [](const MediaProbeStream& stream) { return stream.is_audio; });
}

namespace {

bool rawAacContainer(const std::string& format_name) {
    std::string normalized;
    normalized.reserve(format_name.size());
    for (const char value : format_name) {
        normalized.push_back(static_cast<char>(
            std::tolower(static_cast<unsigned char>(value))));
    }
    return normalized == "aac" || normalized.find("adts") != std::string::npos ||
           normalized.find("latm") != std::string::npos ||
           normalized.find("loas") != std::string::npos;
}

} // namespace

FinalAudioMetadata probeFinalAudioMetadata(const fs::path& audio_path) {
    FinalAudioMetadata metadata;
    const auto probe = probeMediaInProcess(audio_path);
    if (!probe.has_value()) return metadata;
    const auto audio = std::find_if(
        probe->streams.begin(), probe->streams.end(),
        [](const MediaProbeStream& stream) { return stream.is_audio; });
    if (audio == probe->streams.end()) return metadata;

    metadata.codec = avcodec_get_name(static_cast<AVCodecID>(audio->codec_id));
    metadata.sample_rate = audio->sample_rate;
    metadata.channels = audio->channels;
    metadata.channel_layout = audio->channel_layout;
    metadata.duration_seconds = audio->duration_seconds;
    metadata.duration_verified = audio->duration_verified;
    metadata.start_time_seconds = audio->start_time_seconds;
    metadata.start_time_verified = audio->start_time_verified;
    metadata.format_name = probe->format_name;
    metadata.extradata_verified = audio->extradata_present;
    metadata.container_verified = !rawAacContainer(metadata.format_name);
    metadata.metadata_verified =
        !metadata.codec.empty() && metadata.sample_rate > 0 && metadata.channels > 0 &&
        !metadata.channel_layout.empty() && metadata.duration_verified &&
        std::isfinite(metadata.duration_seconds) && metadata.duration_seconds > 0.0 &&
        metadata.start_time_verified && std::isfinite(metadata.start_time_seconds);
    return metadata;
}

#else

double probeMediaDurationSeconds(const fs::path& media_path) {
    if (media_path.empty() || !fs::exists(media_path)) return 0.0;
    std::ostringstream command;
    command << "ffprobe -v error -show_entries format=duration "
            << "-of default=noprint_wrappers=1:nokey=1 "
            << file::shellQuote(media_path.string());
    const std::string output = json::trim(file::captureCommandOutput(command.str()));
    if (output.empty() || output == "N/A") return 0.0;
    try {
        const double duration = std::stod(output);
        return duration > 0.0 ? duration : 0.0;
    } catch (...) {
        return 0.0;
    }
}

namespace {

bool parseFrameRate(const std::string& value, double& output) {
    const auto slash = value.find('/');
    try {
        if (slash == std::string::npos) {
            output = std::stod(value);
        } else {
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

bool detail::nativeVideoStreamCopyCompatible(
    const fs::path& clip_path,
    int width,
    int height,
    int fps,
    double requested_duration) {
    if (clip_path.empty() || !fs::exists(clip_path) || requested_duration <= 0.0) {
        return false;
    }
    std::ostringstream probe;
    probe << "ffprobe -v error -select_streams v:0 "
          << "-show_entries stream=codec_name,width,height,avg_frame_rate,pix_fmt "
          << "-of csv=p=0 " << file::shellQuote(clip_path.string());
    const std::string output = json::trim(file::captureCommandOutput(probe.str()));
    if (output.empty()) return false;

    std::istringstream fields(output);
    std::string codec;
    std::string width_text;
    std::string height_text;
    std::string pixel_format;
    std::string frame_rate_text;
    if (!std::getline(fields, codec, ',') ||
        !std::getline(fields, width_text, ',') ||
        !std::getline(fields, height_text, ',') ||
        !std::getline(fields, pixel_format, ',') ||
        !std::getline(fields, frame_rate_text, ',')) {
        return false;
    }
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
        std::abs(source_fps - static_cast<double>(fps)) > 0.001) {
        return false;
    }
    const double source_duration = probeMediaDurationSeconds(clip_path);
    return source_duration > 0.0 && requested_duration <= source_duration + 0.05;
}

bool hasAudioStream(const fs::path& media_path) {
    if (media_path.empty() || !fs::exists(media_path)) return false;
    std::ostringstream command;
    command << "ffprobe -v error -select_streams a:0"
            << " -show_entries stream=index -of csv=p=0 "
            << file::shellQuote(media_path.string());
    return !json::trim(file::captureCommandOutput(command.str())).empty();
}

FinalAudioMetadata probeFinalAudioMetadata(const fs::path& audio_path) {
    FinalAudioMetadata metadata;
    if (audio_path.empty() || !fs::exists(audio_path)) return metadata;
    std::ostringstream command;
    command << "ffprobe -v error -select_streams a:0"
            << " -show_entries stream=codec_name,sample_rate,channels,channel_layout,duration,start_time"
            << " -of default=noprint_wrappers=1 "
            << file::shellQuote(audio_path.string());
    const std::string output = file::captureCommandOutput(command.str());
    std::istringstream lines(output);
    std::string line;
    while (std::getline(lines, line)) {
        line = json::trim(line);
        const auto separator = line.find('=');
        if (separator == std::string::npos) continue;
        const std::string key = line.substr(0, separator);
        const std::string value = json::trim(line.substr(separator + 1));
        try {
            if (key == "codec_name") metadata.codec = value;
            else if (key == "sample_rate") metadata.sample_rate = std::stoi(value);
            else if (key == "channels") metadata.channels = std::stoi(value);
            else if (key == "channel_layout") metadata.channel_layout = value;
            else if (key == "duration" && value != "N/A") {
                metadata.duration_seconds = std::stod(value);
                metadata.duration_verified = true;
            } else if (key == "start_time" && value != "N/A") {
                metadata.start_time_seconds = std::stod(value);
                metadata.start_time_verified = true;
            }
        } catch (...) {
            return FinalAudioMetadata{};
        }
    }
    metadata.metadata_verified =
        !metadata.codec.empty() && metadata.sample_rate > 0 && metadata.channels > 0 &&
        !metadata.channel_layout.empty() && metadata.duration_verified &&
        std::isfinite(metadata.duration_seconds) && metadata.duration_seconds > 0.0 &&
        metadata.start_time_verified && std::isfinite(metadata.start_time_seconds);
    return metadata;
}

#endif

} // namespace velox::media
