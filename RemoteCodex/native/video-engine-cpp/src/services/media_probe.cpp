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
#include <libavutil/mem.h>
#include <libavutil/pixdesc.h>
#include <libavutil/sha.h>
#include <libavutil/pixfmt.h>
#include <libavutil/rational.h>
#include <libavutil/version.h>
}

#include <cmath>
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <list>
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

struct ProbeCacheRecord {
    std::shared_ptr<const ProbeCacheEntry> entry;
    std::list<fs::path>::iterator lru_position;
};

constexpr std::size_t kProbeCacheMaxEntries = 512;
constexpr std::size_t kProbeCacheShards = 16;

struct ProbeCacheShard {
    std::unordered_map<fs::path, ProbeCacheRecord> cache;
    std::list<fs::path> order;
    std::mutex mu;
};

ProbeCacheShard g_probeCacheShards[kProbeCacheShards];

inline ProbeCacheShard& shardFor(const fs::path& mediaPath) {
    const std::size_t h = fs::hash_value(mediaPath);
    return g_probeCacheShards[h % kProbeCacheShards];
}

std::shared_ptr<const ProbeCacheEntry> probeCacheSnapshot(const fs::path& mediaPath) {
    auto& shard = shardFor(mediaPath);
    std::lock_guard<std::mutex> lock(shard.mu);
    auto it = shard.cache.find(mediaPath);
    if (it == shard.cache.end()) return {};
    return it->second.entry;
}

std::shared_ptr<const ProbeCacheEntry> probeCacheLookup(const fs::path& mediaPath) {
    auto cached = probeCacheSnapshot(mediaPath);
    if (!cached) return {};

    std::error_code ec;
    auto sz = fs::file_size(mediaPath, ec);
    if (ec) return {};
    auto mt = fs::last_write_time(mediaPath, ec);
    if (ec) return {};

    if (cached->file_size == sz && cached->mtime == mt) {
        return cached;
    }

    // Only invalidate the snapshot we observed. A concurrent probe may have
    // installed a newer entry while the filesystem calls were outside the
    // mutex; never erase that newer result.
    auto& shard = shardFor(mediaPath);
    std::lock_guard<std::mutex> lock(shard.mu);
    auto it = shard.cache.find(mediaPath);
    if (it != shard.cache.end() &&
        it->second.entry == cached) {
        shard.order.erase(it->second.lru_position);
        shard.cache.erase(it);
    }
    return {};
}

void probeCacheStore(const fs::path& mediaPath, const MediaProbeResult& result) {
    std::error_code ec;
    auto sz = fs::file_size(mediaPath, ec);
    if (ec) return;
    auto mt = fs::last_write_time(mediaPath, ec);
    if (ec) return;
    auto& shard = shardFor(mediaPath);
    std::lock_guard<std::mutex> lock(shard.mu);
    auto it = shard.cache.find(mediaPath);
    if (it != shard.cache.end()) {
        it->second.entry = std::make_shared<const ProbeCacheEntry>(
            ProbeCacheEntry{result, sz, mt});
        shard.order.splice(shard.order.end(), shard.order,
                                 it->second.lru_position);
        return;
    }
    const std::size_t per_shard_limit = kProbeCacheMaxEntries / kProbeCacheShards;
    while (shard.cache.size() >= per_shard_limit && !shard.order.empty()) {
        const fs::path oldest = shard.order.front();
        shard.order.pop_front();
        shard.cache.erase(oldest);
    }
    shard.order.push_back(mediaPath);
    auto position = std::prev(shard.order.end());
    shard.cache.emplace(mediaPath, ProbeCacheRecord{
        std::make_shared<const ProbeCacheEntry>(ProbeCacheEntry{result, sz, mt}),
        position});
}

struct FormatContextDeleter {
    void operator()(AVFormatContext* context) const {
        if (context != nullptr) {
            avformat_close_input(&context);
        }
    }
};

struct CodecContextDeleter {
    void operator()(AVCodecContext* context) const { avcodec_free_context(&context); }
};

struct PacketDeleter {
    void operator()(AVPacket* packet) const { av_packet_free(&packet); }
};

struct FrameDeleter {
    void operator()(AVFrame* frame) const { av_frame_free(&frame); }
};

struct SHADeleter {
    void operator()(struct AVSHA* sha) const { av_free(sha); }
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

bool drainDecodedFrames(AVCodecContext* decoder, AVFrame* frame, int64_t* count) {
    for (;;) {
        const int result = avcodec_receive_frame(decoder, frame);
        if (result == AVERROR(EAGAIN) || result == AVERROR_EOF) return true;
        if (result < 0) return false;
        if (*count == std::numeric_limits<int64_t>::max()) return false;
        ++*count;
        av_frame_unref(frame);
    }
}

std::string sha256Hex(const uint8_t* data, std::size_t size) {
    if (data == nullptr || size == 0) return {};
    std::unique_ptr<AVSHA, SHADeleter> sha(av_sha_alloc());
    if (!sha || av_sha_init(sha.get(), 256) < 0) return {};
    av_sha_update(sha.get(), data, size);
    uint8_t digest[32]{};
    av_sha_final(sha.get(), digest);
    static constexpr char digits[] = "0123456789abcdef";
    std::string result(64, '0');
    for (std::size_t i = 0; i < sizeof(digest); ++i) {
        result[i * 2] = digits[digest[i] >> 4];
        result[i * 2 + 1] = digits[digest[i] & 0x0f];
    }
    return result;
}

} // namespace

std::optional<MediaProbeResult> probeMediaInProcess(const fs::path& mediaPath) {
    if (mediaPath.empty()) {
        return std::nullopt;
    }
    if (auto cached = probeCacheLookup(mediaPath)) {
        return cached->result;
    }
    auto result = probeOpenContext(mediaPath);
    if (result) probeCacheStore(mediaPath, *result);
    return result;
}

std::optional<MediaSourceCertificationProbe> probeSourceForCertificationInProcess(
    const fs::path& mediaPath) {
    if (mediaPath.empty()) return std::nullopt;

    AVFormatContext* rawContext = nullptr;
    if (avformat_open_input(&rawContext, mediaPath.c_str(), nullptr, nullptr) < 0 ||
        rawContext == nullptr) {
        if (rawContext != nullptr) avformat_close_input(&rawContext);
        return std::nullopt;
    }
    services::recordInputOpen(mediaPath.string());
    UniqueFormatContext format(rawContext);
    if (avformat_find_stream_info(format.get(), nullptr) < 0) return std::nullopt;

    AVStream* stream = nullptr;
    for (unsigned int i = 0; i < format->nb_streams; ++i) {
        if (format->streams[i] != nullptr && format->streams[i]->codecpar != nullptr &&
            format->streams[i]->codecpar->codec_type == AVMEDIA_TYPE_VIDEO) {
            stream = format->streams[i];
            break;
        }
    }
    if (stream == nullptr || stream->codecpar == nullptr ||
        stream->time_base.num <= 0 || stream->time_base.den <= 0 ||
        !validTimestamp(stream->start_time)) {
        return std::nullopt;
    }

    const AVCodecParameters* parameters = stream->codecpar;
    if (parameters->extradata == nullptr || parameters->extradata_size <= 0) {
        return std::nullopt;
    }
    const AVCodec* codec = avcodec_find_decoder(parameters->codec_id);
    if (codec == nullptr) return std::nullopt;
    std::unique_ptr<AVCodecContext, CodecContextDeleter> decoder(avcodec_alloc_context3(codec));
    if (!decoder || avcodec_parameters_to_context(decoder.get(), parameters) < 0 ||
        avcodec_open2(decoder.get(), codec, nullptr) < 0) {
        return std::nullopt;
    }
    std::unique_ptr<AVPacket, PacketDeleter> packet(av_packet_alloc());
    std::unique_ptr<AVFrame, FrameDeleter> frame(av_frame_alloc());
    if (!packet || !frame) return std::nullopt;

    MediaSourceCertificationProbe result;
    result.codec_name = avcodec_get_name(parameters->codec_id);
    if (const char* profile = av_get_profile_name(codec, parameters->profile); profile != nullptr) {
        result.profile = profile;
        std::transform(result.profile.begin(), result.profile.end(), result.profile.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    }
    result.level = parameters->level;
    result.width = parameters->width;
    result.height = parameters->height;
    if (const char* pixelFormat = av_get_pix_fmt_name(
            static_cast<AVPixelFormat>(parameters->format)); pixelFormat != nullptr) {
        result.pixel_format = pixelFormat;
    }
    const AVRational frameRate = stream->r_frame_rate;
    const AVRational averageRate = stream->avg_frame_rate;
    result.frame_rate_num = frameRate.num;
    result.frame_rate_den = frameRate.den;
    result.average_frame_rate_num = averageRate.num;
    result.average_frame_rate_den = averageRate.den;
    result.time_base_num = stream->time_base.num;
    result.time_base_den = stream->time_base.den;
    result.start_time = stream->start_time;
    result.start_time_verified = true;
    result.start_time_seconds = static_cast<double>(stream->start_time) * av_q2d(stream->time_base);
    result.b_frames = decoder->has_b_frames;
    result.bit_rate = parameters->bit_rate;
    result.extradata_sha256 = sha256Hex(parameters->extradata,
                                        static_cast<std::size_t>(parameters->extradata_size));

    bool complete = true;
    int readResult = 0;
    while ((readResult = av_read_frame(format.get(), packet.get())) >= 0) {
        if (packet->stream_index == stream->index) {
            if ((packet->flags & AV_PKT_FLAG_KEY) != 0) {
                const int64_t timestamp = packet->pts != AV_NOPTS_VALUE ? packet->pts : packet->dts;
                if (timestamp != AV_NOPTS_VALUE) {
                    result.packet_keyframe_times.push_back(
                        static_cast<double>(timestamp) * av_q2d(stream->time_base));
                }
            }
            const int sent = avcodec_send_packet(decoder.get(), packet.get());
            if (sent < 0 || !drainDecodedFrames(decoder.get(), frame.get(), &result.exact_frame_count)) {
                complete = false;
                av_packet_unref(packet.get());
                break;
            }
        }
        av_packet_unref(packet.get());
    }
    if (readResult != AVERROR_EOF) complete = false;
    if (complete) {
        const int flush = avcodec_send_packet(decoder.get(), nullptr);
        if (flush < 0 && flush != AVERROR_EOF) complete = false;
        if (complete && !drainDecodedFrames(decoder.get(), frame.get(), &result.exact_frame_count)) {
            complete = false;
        }
    }
    if (!complete || result.exact_frame_count <= 0 || result.packet_keyframe_times.empty() ||
        result.codec_name.empty() || result.profile.empty() || result.pixel_format.empty() ||
        result.frame_rate_num <= 0 || result.frame_rate_den <= 0 ||
        result.average_frame_rate_num <= 0 || result.average_frame_rate_den <= 0 ||
        result.extradata_sha256.empty() || !std::isfinite(result.start_time_seconds)) {
        return std::nullopt;
    }
    return result;
}

} // namespace velox::media

#else

namespace velox::media {

std::optional<MediaSourceCertificationProbe> probeSourceForCertificationInProcess(
    const std::filesystem::path&) {
    return std::nullopt;
}

} // namespace velox::media

#endif // VELOX_ENABLE_LIBAV
