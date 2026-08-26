#include "velox/services/media_probe.hpp"
#include "velox/services/io_counters.hpp"

// The in-process MediaProbe is built only when VELOX_ENABLE_LIBAV is ON.
// Without the flag this translation unit is empty: media_utils_probe.cpp uses the
// ffprobe CLI fallback for every probe, so probeMediaInProcess() has no
// callers in a non-LibAV build (and the libav-only tests are excluded).
#ifdef VELOX_ENABLE_LIBAV

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/channel_layout.h>
#include <libavutil/error.h>
#include <libavutil/pixfmt.h>
#include <libavutil/version.h>
}

#include <cmath>
#include <limits>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace fs = std::filesystem;

namespace velox::media {
namespace {

struct ProbeCacheEntry {
    MediaProbeResult result;
    uintmax_t file_size{0};
    std::filesystem::file_time_type mtime{};
};

std::unordered_map<std::string, ProbeCacheEntry> g_probeCache;
std::mutex g_probeCacheMu;

std::optional<ProbeCacheEntry> probeCacheLookup(const fs::path& mediaPath) {
    std::lock_guard<std::mutex> lock(g_probeCacheMu);
    auto it = g_probeCache.find(mediaPath.string());
    if (it == g_probeCache.end()) return std::nullopt;
    std::error_code ec;
    auto sz = fs::file_size(mediaPath, ec);
    if (ec) return std::nullopt;
    auto mt = fs::last_write_time(mediaPath, ec);
    if (ec) return std::nullopt;
    if (it->second.file_size != sz || it->second.mtime != mt) {
        g_probeCache.erase(it);
        return std::nullopt;
    }
    return it->second;
}

void probeCacheStore(const fs::path& mediaPath, const MediaProbeResult& result) {
    std::error_code ec;
    auto sz = fs::file_size(mediaPath, ec);
    if (ec) return;
    auto mt = fs::last_write_time(mediaPath, ec);
    if (ec) return;
    std::lock_guard<std::mutex> lock(g_probeCacheMu);
    g_probeCache[mediaPath.string()] = ProbeCacheEntry{result, sz, mt};
}

struct FormatContextDeleter {
    void operator()(AVFormatContext* context) const {
        if (context != nullptr) {
            avformat_close_input(&context);
        }
    }
};

using UniqueFormatContext = std::unique_ptr<AVFormatContext, FormatContextDeleter>;

bool validTimestamp(int64_t value) {
    return value != AV_NOPTS_VALUE;
}

double streamDurationSeconds(const AVStream* stream) {
    if (stream == nullptr || !validTimestamp(stream->duration)) {
        return 0.0;
    }
    const double seconds = static_cast<double>(stream->duration) * av_q2d(stream->time_base);
    return std::isfinite(seconds) && seconds > 0.0 ? seconds : 0.0;
}

double streamStartTimeSeconds(const AVStream* stream) {
    if (stream == nullptr || !validTimestamp(stream->start_time)) {
        return 0.0;
    }
    const double seconds = static_cast<double>(stream->start_time) * av_q2d(stream->time_base);
    return std::isfinite(seconds) ? seconds : 0.0;
}

double frameRate(const AVStream* stream) {
    if (stream == nullptr) {
        return 0.0;
    }
    AVRational rate = stream->avg_frame_rate;
    if (rate.num <= 0 || rate.den <= 0) {
        rate = stream->r_frame_rate;
    }
    const double value = av_q2d(rate);
    return std::isfinite(value) && value > 0.0 ? value : 0.0;
}

std::string channelLayoutName(const AVChannelLayout& layout) {
    char buffer[256]{};
    if (av_channel_layout_describe(&layout, buffer, sizeof(buffer)) >= 0) {
        return buffer;
    }
    return {};
}

std::optional<MediaProbeResult> probeOpenContext(const fs::path& mediaPath) {
    AVFormatContext* rawContext = nullptr;
    const int openResult = avformat_open_input(
        &rawContext, mediaPath.c_str(), nullptr, nullptr);
    if (openResult < 0 || rawContext == nullptr) {
        if (rawContext != nullptr) {
            avformat_close_input(&rawContext);
        }
        return std::nullopt;
    }
    // Every in-process probe (duration, audio stream, final-audio
    // metadata, output verification) opens a real input through here.
    services::recordInputOpen(mediaPath.string());
    UniqueFormatContext context(rawContext);

    if (avformat_find_stream_info(context.get(), nullptr) < 0) {
        return std::nullopt;
    }

    MediaProbeResult result;
    if (context->iformat != nullptr && context->iformat->name != nullptr) {
        result.format_name = context->iformat->name;
    }
    if (validTimestamp(context->duration)) {
        const double seconds = static_cast<double>(context->duration) / AV_TIME_BASE;
        if (std::isfinite(seconds) && seconds > 0.0) {
            result.duration_seconds = seconds;
            result.duration_verified = true;
        }
    }
    result.streams.reserve(context->nb_streams);
    for (unsigned int position = 0; position < context->nb_streams; ++position) {
        const AVStream* stream = context->streams[position];
        if (stream == nullptr || stream->codecpar == nullptr) {
            continue;
        }
        const AVCodecParameters* parameters = stream->codecpar;
        MediaProbeStream output;
        output.index = static_cast<int>(position);
        output.is_video = parameters->codec_type == AVMEDIA_TYPE_VIDEO;
        output.is_audio = parameters->codec_type == AVMEDIA_TYPE_AUDIO;
        output.codec_id = static_cast<int>(parameters->codec_id);
        output.pixel_format = parameters->format;
        output.width = parameters->width;
        output.height = parameters->height;
        output.average_frame_rate = frameRate(stream);
        output.sample_rate = parameters->sample_rate;
#if LIBAVUTIL_VERSION_MAJOR >= 57
        output.channels = parameters->ch_layout.nb_channels;
        if (parameters->ch_layout.nb_channels > 0 &&
            parameters->ch_layout.order != AV_CHANNEL_ORDER_UNSPEC) {
            output.channel_layout = channelLayoutName(parameters->ch_layout);
        }
#else
        output.channels = parameters->channels;
        if (parameters->channel_layout != 0) {
            char buffer[64]{};
            av_get_channel_layout_string(
                buffer, sizeof(buffer), parameters->channels, parameters->channel_layout);
            output.channel_layout = buffer;
        }
#endif
        output.duration_seconds = streamDurationSeconds(stream);
        output.duration_verified = output.duration_seconds > 0.0;
        output.start_time_seconds = streamStartTimeSeconds(stream);
        output.start_time_verified = validTimestamp(stream->start_time);
        output.extradata_present = parameters->extradata_size > 0 &&
            parameters->extradata != nullptr;
        result.streams.push_back(std::move(output));

        if (!result.duration_verified && output.duration_verified) {
            result.duration_seconds = output.duration_seconds;
            result.duration_verified = true;
        }
    }
    return result;
}

} // namespace

std::optional<MediaProbeResult> probeMediaInProcess(const fs::path& mediaPath) {
    if (mediaPath.empty() || !fs::is_regular_file(mediaPath)) {
        return std::nullopt;
    }
    if (auto cached = probeCacheLookup(mediaPath)) {
        return cached->result;
    }
    auto result = probeOpenContext(mediaPath);
    if (result) probeCacheStore(mediaPath, *result);
    return result;
}

} // namespace velox::media

#endif // VELOX_ENABLE_LIBAV
