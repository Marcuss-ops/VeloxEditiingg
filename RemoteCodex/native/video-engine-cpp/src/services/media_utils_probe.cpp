#include "velox/services/media_utils.hpp"
#include "json_utils.hpp"

#ifdef VELOX_ENABLE_LIBAV
#include "velox/services/media_probe.hpp"

extern "C" {
#include <libavcodec/avcodec.h>
}
#endif

#include <cctype>
#include <cmath>
#include <sstream>

namespace fs = std::filesystem;

namespace velox::media {

#ifdef VELOX_ENABLE_LIBAV

double probeMediaDurationSeconds(const fs::path& media_path) {
    const auto probe = probeMediaInProcess(media_path);
    if (!probe.has_value() || !probe->duration_verified) return 0.0;
    return probe->duration_seconds;
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
