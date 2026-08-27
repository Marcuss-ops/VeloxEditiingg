#ifdef VELOX_ENABLE_LIBAV

#include "media_packet_pipeline_internal.hpp"
#include "velox/services/io_counters.hpp"

extern "C" {
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
#include <libavutil/mathematics.h>
}

#include <filesystem>
#include <limits>
#include <string>

namespace fs = std::filesystem;

namespace velox::media::packet {

bool validTimestamp(int64_t value) {
    return value != AV_NOPTS_VALUE;
}

std::string ffmpegError(int error) {
    char buffer[AV_ERROR_MAX_STRING_SIZE]{};
    av_strerror(error, buffer, sizeof(buffer));
    return buffer;
}

int64_t rescale(int64_t value, AVRational source, AVRational destination) {
    if (!validTimestamp(value)) {
        return AV_NOPTS_VALUE;
    }
    return av_rescale_q_rnd(
        value, source, destination,
        static_cast<AVRounding>(AV_ROUND_NEAR_INF | AV_ROUND_PASS_MINMAX));
}

int64_t relativeTimestamp(int64_t timestamp,
                          int64_t source_start,
                          AVRational input_time_base) {
    if (!validTimestamp(timestamp)) {
        return AV_NOPTS_VALUE;
    }
    // Remove stream start in the input time base before converting to the
    // common microsecond timeline, avoiding a rounded-origin drift.
    return rescale(timestamp - source_start, input_time_base, kMicrosecondTimeBase);
}

Demuxer::~Demuxer() {
    close();
}

bool Demuxer::open(const fs::path& path, std::string& error) {
    close();
    if (path.empty() || !fs::is_regular_file(path)) {
        error = "input is not a regular local file: " + path.string();
        return false;
    }

    AVFormatContext* raw = nullptr;
    const int open_result = avformat_open_input(&raw, path.c_str(), nullptr, nullptr);
    if (open_result < 0 || raw == nullptr) {
        error = "avformat_open_input(" + path.string() + "): " + ffmpegError(open_result);
        if (raw != nullptr) {
            avformat_close_input(&raw);
        }
        return false;
    }
    services::recordInputOpen(path.string());
    context_ = raw;
    const int info_result = avformat_find_stream_info(context_, nullptr);
    if (info_result < 0) {
        error = "avformat_find_stream_info(" + path.string() + "): " + ffmpegError(info_result);
        close();
        return false;
    }
    return true;
}

int Demuxer::firstStream(AVMediaType type) const {
    if (context_ == nullptr) {
        return -1;
    }
    for (unsigned int i = 0; i < context_->nb_streams; ++i) {
        const AVStream* stream = context_->streams[i];
        if (stream != nullptr && stream->codecpar != nullptr &&
            stream->codecpar->codec_type == type) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

const AVStream* Demuxer::stream(int index) const {
    if (context_ == nullptr || index < 0 ||
        static_cast<unsigned int>(index) >= context_->nb_streams) {
        return nullptr;
    }
    return context_->streams[index];
}

const AVFormatContext* Demuxer::raw() const {
    return context_;
}

bool Demuxer::seekToTimestampUs(int stream_index, int64_t timestamp_us, std::string& error) {
    if (context_ == nullptr) {
        error = "demuxer is not open";
        return false;
    }
    if (timestamp_us < 0) {
        error = "demuxer seek timestamp must be non-negative";
        return false;
    }
    const AVStream* input_stream = stream(stream_index);
    if (input_stream == nullptr) {
        error = "demuxer seek stream index is invalid";
        return false;
    }
    const int64_t source_start = validTimestamp(input_stream->start_time)
        ? input_stream->start_time : 0;
    const int64_t target = av_rescale_q_rnd(
        timestamp_us, kMicrosecondTimeBase, input_stream->time_base,
        static_cast<AVRounding>(AV_ROUND_DOWN | AV_ROUND_PASS_MINMAX)) + source_start;
    services::recordInputSeek();
    const int seek_result = avformat_seek_file(
        context_, stream_index, std::numeric_limits<int64_t>::min(), target,
        std::numeric_limits<int64_t>::max(), AVSEEK_FLAG_BACKWARD);
    if (seek_result < 0) {
        error = "avformat_seek_file: " + ffmpegError(seek_result);
        return false;
    }
    avformat_flush(context_);
    return true;
}

bool Demuxer::readFrame(AVPacket& packet, bool& eof, std::string& error) {
    eof = false;
    if (context_ == nullptr) {
        error = "demuxer is not open";
        return false;
    }
    const int read_result = av_read_frame(context_, &packet);
    if (read_result >= 0) {
        services::recordFirstPacketRead();
    }
    if (read_result == AVERROR_EOF) {
        eof = true;
        return true;
    }
    if (read_result < 0) {
        error = ffmpegError(read_result);
        return false;
    }
    return true;
}

void Demuxer::close() {
    if (context_ != nullptr) {
        avformat_close_input(&context_);
        context_ = nullptr;
    }
}

} // namespace velox::media::packet

#endif // VELOX_ENABLE_LIBAV
