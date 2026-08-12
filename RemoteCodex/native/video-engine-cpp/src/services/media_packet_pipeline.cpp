#include "velox/services/media_packet_pipeline.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/io_counters.hpp"
#include "velox/services/media_probe.hpp"

// The in-process packet copy pipeline is built only when VELOX_ENABLE_LIBAV
// is ON. Without the flag a fail-closed stub is compiled instead: the
// engine's copy-only packet branch is also compiled out (RenderEngine falls
// back to the legacy segment/concat path), so this stub exists to keep the
// symbol defined for any caller built without the flag.
#ifdef VELOX_ENABLE_LIBAV

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
#include <libavutil/channel_layout.h>
#include <libavutil/mathematics.h>
#include <libavutil/version.h>
}

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <sstream>
#include <system_error>
#include <unistd.h>

namespace fs = std::filesystem;

namespace velox::media {
namespace {

constexpr AVRational kMicrosecondTimeBase{1, 1'000'000};

struct InputContextDeleter {
    void operator()(AVFormatContext* context) const {
        if (context != nullptr) {
            avformat_close_input(&context);
        }
    }
};
using UniqueInputContext = std::unique_ptr<AVFormatContext, InputContextDeleter>;

struct OutputContextDeleter {
    void operator()(AVFormatContext* context) const {
        if (context != nullptr) {
            avformat_free_context(context);
        }
    }
};
using UniqueOutputContext = std::unique_ptr<AVFormatContext, OutputContextDeleter>;

struct PacketHolder {
    AVPacket packet{};
    int output_stream_index{0};
    int64_t sort_dts{AV_NOPTS_VALUE};

    ~PacketHolder() {
        av_packet_unref(&packet);
    }

    PacketHolder() = default;
    PacketHolder(const PacketHolder&) = delete;
    PacketHolder& operator=(const PacketHolder&) = delete;
};

struct TimestampState {
    int64_t last_dts{AV_NOPTS_VALUE};
    int64_t last_pts{AV_NOPTS_VALUE};
};

struct OutputStreams {
    AVStream* video{nullptr};
    AVStream* audio{nullptr};
};

bool validTimestamp(int64_t value) {
    return value != AV_NOPTS_VALUE;
}

std::string ffmpegError(int error) {
    char buffer[AV_ERROR_MAX_STRING_SIZE]{};
    av_strerror(error, buffer, sizeof(buffer));
    return buffer;
}

bool openInput(const fs::path& path, UniqueInputContext& result, std::string& error) {
    if (path.empty() || !fs::is_regular_file(path)) {
        error = "input is not a regular local file: " + path.string();
        return false;
    }

    AVFormatContext* raw = nullptr;
    const int openResult = avformat_open_input(&raw, path.c_str(), nullptr, nullptr);
    if (openResult < 0 || raw == nullptr) {
        error = "avformat_open_input(" + path.string() + "): " + ffmpegError(openResult);
        if (raw != nullptr) {
            avformat_close_input(&raw);
        }
        return false;
    }
    // This open is real media I/O: every successful avformat open in the
    // packet muxer goes through this chokepoint (stream discovery in the
    // main loop AND the readPackets reopen).
    services::recordInputOpen(path.string());
    result.reset(raw);
    const int infoResult = avformat_find_stream_info(result.get(), nullptr);
    if (infoResult < 0) {
        error = "avformat_find_stream_info(" + path.string() + "): " + ffmpegError(infoResult);
        result.reset();
        return false;
    }
    return true;
}

int firstStream(const AVFormatContext* context, AVMediaType type) {
    if (context == nullptr) {
        return -1;
    }
    for (unsigned int i = 0; i < context->nb_streams; ++i) {
        const AVStream* stream = context->streams[i];
        if (stream != nullptr && stream->codecpar != nullptr &&
            stream->codecpar->codec_type == type) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

bool compatibleCodecParameters(const AVCodecParameters* left,
                               const AVCodecParameters* right,
                               AVMediaType type) {
    if (left == nullptr || right == nullptr || left->codec_type != type ||
        right->codec_type != type || left->codec_id != right->codec_id ||
        left->profile != right->profile || left->level != right->level ||
        left->extradata_size != right->extradata_size) {
        return false;
    }
    if (type == AVMEDIA_TYPE_VIDEO &&
        (left->width != right->width || left->height != right->height ||
         left->format != right->format)) {
        return false;
    }
    if (type == AVMEDIA_TYPE_AUDIO &&
        (left->sample_rate != right->sample_rate ||
         left->format != right->format)) {
        return false;
    }
#if LIBAVUTIL_VERSION_MAJOR >= 57
    if (type == AVMEDIA_TYPE_AUDIO &&
        av_channel_layout_compare(&left->ch_layout, &right->ch_layout) != 0) {
        return false;
    }
#else
    if (type == AVMEDIA_TYPE_AUDIO &&
        (left->channels != right->channels ||
         left->channel_layout != right->channel_layout)) {
        return false;
    }
#endif
    if (left->extradata_size > 0 &&
        std::memcmp(left->extradata, right->extradata,
                    static_cast<size_t>(left->extradata_size)) != 0) {
        return false;
    }
    return true;
}

bool initializeOutputStream(AVFormatContext* output,
                            const AVStream* input,
                            AVStream*& destination,
                            std::string& error) {
    destination = avformat_new_stream(output, nullptr);
    if (destination == nullptr) {
        error = "avformat_new_stream failed";
        return false;
    }
    const int copyResult = avcodec_parameters_copy(destination->codecpar, input->codecpar);
    if (copyResult < 0) {
        error = "avcodec_parameters_copy: " + ffmpegError(copyResult);
        return false;
    }
    // Let the MP4 muxer select the correct sample entry/tag.
    destination->codecpar->codec_tag = 0;
    destination->time_base = kMicrosecondTimeBase;
    if (input->avg_frame_rate.num > 0 && input->avg_frame_rate.den > 0) {
        destination->avg_frame_rate = input->avg_frame_rate;
    }
    return true;
}

int64_t rescale(int64_t value, AVRational source, AVRational destination);

int64_t streamDurationUs(const AVFormatContext* context, const AVStream* stream) {
    if (stream != nullptr && validTimestamp(stream->duration) && stream->duration > 0) {
        return rescale(stream->duration, stream->time_base, kMicrosecondTimeBase);
    }
    if (context != nullptr && validTimestamp(context->duration) && context->duration > 0) {
        return rescale(context->duration, AVRational{1, AV_TIME_BASE}, kMicrosecondTimeBase);
    }
    return 0;
}

int64_t rescale(int64_t value, AVRational source, AVRational destination) {
    if (!validTimestamp(value)) {
        return AV_NOPTS_VALUE;
    }
    return av_rescale_q_rnd(
        value, source, destination,
        static_cast<AVRounding>(AV_ROUND_NEAR_INF | AV_ROUND_PASS_MINMAX));
}

void normalizeFinalPacket(AVPacket& packet, TimestampState& state) {
    if (validTimestamp(packet.dts)) {
        if (validTimestamp(state.last_dts) && packet.dts <= state.last_dts) {
            packet.dts = state.last_dts + 1;
        }
        state.last_dts = packet.dts;
    }
    if (validTimestamp(packet.pts)) {
        if (validTimestamp(state.last_pts) && packet.pts <= state.last_pts) {
            packet.pts = state.last_pts + 1;
        }
        state.last_pts = packet.pts;
    }
    if (validTimestamp(packet.pts) && validTimestamp(packet.dts) && packet.pts < packet.dts) {
        packet.pts = packet.dts;
        state.last_pts = packet.pts;
    }
}

int64_t relativeTimestamp(int64_t timestamp,
                          int64_t source_start,
                          AVRational input_time_base) {
    if (!validTimestamp(timestamp)) {
        return AV_NOPTS_VALUE;
    }
    // Subtract in the input time base before rescaling so a non-zero stream
    // start is removed exactly rather than after a rounded conversion.
    return rescale(timestamp - source_start, input_time_base, kMicrosecondTimeBase);
}

bool rewritePacket(AVPacket& packet,
                   const AVStream* input_stream,
                   AVStream* output_stream,
                   int64_t source_start,
                   int64_t timeline_offset,
                   int64_t segment_duration,
                   TimestampState& state,
                   int64_t& sort_dts) {
    const int64_t reference = validTimestamp(packet.pts) ? packet.pts : packet.dts;
    const int64_t relative_reference = relativeTimestamp(
        reference, source_start, input_stream->time_base);
    if (!validTimestamp(relative_reference) || relative_reference >= segment_duration) {
        return false;
    }

    packet.pts = relativeTimestamp(packet.pts, source_start, input_stream->time_base);
    packet.dts = relativeTimestamp(packet.dts, source_start, input_stream->time_base);
    // Decoder priming and reordered video can expose a negative first DTS.
    // The copy-only contract starts each source at its requested timeline
    // origin, so clamp only the negative prefix; later packet order remains
    // governed by the monotonic state below.
    if (validTimestamp(packet.pts) && packet.pts < 0) packet.pts = 0;
    if (validTimestamp(packet.dts) && packet.dts < 0) packet.dts = 0;
    packet.duration = packet.duration > 0
        ? rescale(packet.duration, input_stream->time_base, kMicrosecondTimeBase)
        : 0;

    if (validTimestamp(packet.pts)) {
        packet.pts += timeline_offset;
    }
    if (validTimestamp(packet.dts)) {
        packet.dts += timeline_offset;
    }
    if (validTimestamp(packet.pts) && validTimestamp(packet.dts) && packet.pts < packet.dts) {
        packet.pts = packet.dts;
    }

    const int64_t end = timeline_offset + segment_duration;
    const int64_t packet_timestamp = validTimestamp(packet.pts) ? packet.pts : packet.dts;
    if (!validTimestamp(packet_timestamp) || packet_timestamp >= end) {
        // Do not mutate the cross-segment monotonic state for a packet that
        // was excluded by the trim boundary.
        return false;
    }
    if (packet.duration > 0 && packet_timestamp + packet.duration > end) {
        packet.duration = end - packet_timestamp;
    }

    // Commit monotonic state only after the packet is accepted. A packet just
    // beyond a segment boundary must not move the next segment's baseline.
    if (validTimestamp(packet.pts)) {
        if (validTimestamp(state.last_pts) && packet.pts <= state.last_pts) {
            packet.pts = state.last_pts + 1;
        }
        state.last_pts = packet.pts;
    }
    if (validTimestamp(packet.dts)) {
        if (validTimestamp(state.last_dts) && packet.dts <= state.last_dts) {
            packet.dts = state.last_dts + 1;
        }
        state.last_dts = packet.dts;
    }
    if (validTimestamp(packet.pts) && validTimestamp(packet.dts) && packet.pts < packet.dts) {
        packet.pts = packet.dts;
        state.last_pts = packet.pts;
    }
    packet.stream_index = output_stream->index;
    sort_dts = validTimestamp(packet.dts) ? packet.dts : packet.pts;
    return true;
}

bool readPackets(const fs::path& path,
                 AVMediaType type,
                 int input_stream_index,
                 AVStream* output_stream,
                 int64_t timeline_offset,
                 int64_t duration_us,
                 TimestampState& state,
                 std::vector<std::unique_ptr<PacketHolder>>& packets,
                 int64_t& packet_count,
                 std::string& error) {
    UniqueInputContext input;
    if (!openInput(path, input, error)) {
        return false;
    }
    if (input_stream_index < 0 ||
        static_cast<unsigned int>(input_stream_index) >= input->nb_streams) {
        error = "stream index is invalid for " + path.string();
        return false;
    }
    const AVStream* input_stream = input->streams[input_stream_index];
    if (input_stream == nullptr || input_stream->codecpar == nullptr ||
        input_stream->codecpar->codec_type != type) {
        error = "requested stream is missing from " + path.string();
        return false;
    }

    int64_t source_start = validTimestamp(input_stream->start_time)
        ? input_stream->start_time : 0;
    AVPacket* packet = av_packet_alloc();
    if (packet == nullptr) {
        error = "av_packet_alloc failed";
        return false;
    }

    while (true) {
        const int readResult = av_read_frame(input.get(), packet);
        if (readResult == AVERROR_EOF) {
            break;
        }
        if (readResult < 0) {
            error = "av_read_frame(" + path.string() + "): " + ffmpegError(readResult);
            av_packet_free(&packet);
            return false;
        }
        if (packet->stream_index == input_stream_index) {
            int64_t sort_dts = AV_NOPTS_VALUE;
            if (rewritePacket(*packet, input_stream, output_stream, source_start,
                              timeline_offset, duration_us, state, sort_dts)) {
                auto holder = std::make_unique<PacketHolder>();
                av_packet_move_ref(&holder->packet, packet);
                holder->output_stream_index = output_stream->index;
                holder->sort_dts = sort_dts;
                packets.push_back(std::move(holder));
                ++packet_count;
            }
        }
        av_packet_unref(packet);
    }
    av_packet_free(&packet);
    return true;
}

bool fail(CopyOnlyMuxResult* result, const std::string& error) {
    if (result != nullptr) {
        result->success = false;
        result->error = error;
    }
    return false;
}

} // namespace

bool muxCopyOnly(const CopyOnlyMuxRequest& request, CopyOnlyMuxResult* result) {
    CopyOnlyMuxResult local;
    if (result == nullptr) {
        result = &local;
    }
    *result = CopyOnlyMuxResult{};

    if (request.video_segments.empty()) {
        return fail(result, "copy-only packet mux requires at least one video segment");
    }
    if (request.output_path.empty()) {
        return fail(result, "copy-only packet mux requires an output path");
    }
    if (request.audio.has_value() && request.audio->start_offset_us < 0) {
        return fail(result, "copy-only packet mux rejects negative audio offsets");
    }
    if (request.audio.has_value() && std::any_of(
            request.video_segments.begin(), request.video_segments.end(),
            [](const CopyOnlyVideoSegment& segment) { return segment.include_audio; })) {
        return fail(result, "copy-only cannot combine segment audio with final audio");
    }

    fs::path output_path = request.output_path;
    fs::path parent = output_path.parent_path();
    std::error_code ec;
    if (parent.empty()) {
        parent = fs::current_path(ec);
    }
    if (ec || parent.empty()) {
        return fail(result, "copy-only packet mux cannot resolve output directory");
    }
    fs::create_directories(parent, ec);
    if (ec) {
        return fail(result, "copy-only packet mux cannot create output directory: " + ec.message());
    }
    const fs::path partial = file::makePartialPath(output_path);
    auto cleanupPartial = [&]() {
        std::error_code remove_error;
        fs::remove(partial, remove_error);
    };

    AVFormatContext* raw_output = nullptr;
    const int allocResult = avformat_alloc_output_context2(
        &raw_output, nullptr, "mp4", partial.c_str());
    if (allocResult < 0 || raw_output == nullptr) {
        cleanupPartial();
        return fail(result, "avformat_alloc_output_context2: " + ffmpegError(allocResult));
    }
    UniqueOutputContext output(raw_output);

    OutputStreams streams;
    std::vector<std::unique_ptr<PacketHolder>> packets;
    packets.reserve(request.video_segments.size() * 16);
    TimestampState video_state;
    TimestampState segment_audio_state;
    int64_t timeline_offset = 0;
    std::string error;

    for (const auto& segment : request.video_segments) {
        if (segment.duration_us <= 0) {
            cleanupPartial();
            return fail(result, "copy-only packet mux rejects non-positive video duration");
        }
        UniqueInputContext input;
        if (!openInput(segment.path, input, error)) {
            cleanupPartial();
            return fail(result, error);
        }
        const int video_index = firstStream(input.get(), AVMEDIA_TYPE_VIDEO);
        if (video_index < 0) {
            cleanupPartial();
            return fail(result, "video stream missing from " + segment.path.string());
        }
        const AVStream* input_video = input->streams[video_index];
        const int64_t source_video_duration = streamDurationUs(input.get(), input_video);
        if (source_video_duration > 0 &&
            source_video_duration + 50'000 < segment.duration_us) {
            cleanupPartial();
            return fail(result, "copy-only video source is shorter than its requested segment");
        }
        if (streams.video == nullptr) {
            if (!initializeOutputStream(output.get(), input_video, streams.video, error)) {
                cleanupPartial();
                return fail(result, error);
            }
        } else if (!compatibleCodecParameters(streams.video->codecpar,
                                              input_video->codecpar,
                                              AVMEDIA_TYPE_VIDEO)) {
            cleanupPartial();
            return fail(result, "copy-only video codec parameters differ at " + segment.path.string());
        }

        int audio_index = -1;
        if (segment.include_audio) {
            audio_index = firstStream(input.get(), AVMEDIA_TYPE_AUDIO);
            if (audio_index < 0) {
                cleanupPartial();
                return fail(result, "copy-only segment requests audio but the source has no audio stream");
            }
            const AVStream* input_audio = input->streams[audio_index];
            const int64_t source_audio_duration = streamDurationUs(input.get(), input_audio);
            if (source_audio_duration > 0 &&
                source_audio_duration + 50'000 < segment.duration_us) {
                cleanupPartial();
                return fail(result, "copy-only segment audio is shorter than its requested segment");
            }
            if (streams.audio == nullptr) {
                if (!initializeOutputStream(output.get(), input_audio, streams.audio, error)) {
                    cleanupPartial();
                    return fail(result, error);
                }
            } else if (!compatibleCodecParameters(streams.audio->codecpar,
                                                  input_audio->codecpar,
                                                  AVMEDIA_TYPE_AUDIO)) {
                cleanupPartial();
                return fail(result, "copy-only segment audio codec parameters differ");
            }
        }
        input.reset();

        if (!readPackets(segment.path, AVMEDIA_TYPE_VIDEO, video_index, streams.video,
                         timeline_offset, segment.duration_us, video_state, packets,
                         result->video_packets, error)) {
            cleanupPartial();
            return fail(result, error);
        }
        if (segment.include_audio && !readPackets(
                segment.path, AVMEDIA_TYPE_AUDIO, audio_index, streams.audio,
                timeline_offset, segment.duration_us, segment_audio_state, packets,
                result->audio_packets, error)) {
            cleanupPartial();
            return fail(result, error);
        }
        timeline_offset += segment.duration_us;
    }
    result->duration_us = timeline_offset;

    if (request.audio.has_value()) {
        const auto& audio_request = *request.audio;
        UniqueInputContext input;
        if (!openInput(audio_request.path, input, error)) {
            cleanupPartial();
            return fail(result, error);
        }
        const int audio_index = firstStream(input.get(), AVMEDIA_TYPE_AUDIO);
        if (audio_index < 0) {
            cleanupPartial();
            return fail(result, "audio stream missing from " + audio_request.path.string());
        }
        const AVStream* input_audio = input->streams[audio_index];
        if (streams.audio == nullptr) {
            if (!initializeOutputStream(output.get(), input_audio, streams.audio, error)) {
                cleanupPartial();
                return fail(result, error);
            }
        } else if (!compatibleCodecParameters(streams.audio->codecpar,
                                              input_audio->codecpar,
                                              AVMEDIA_TYPE_AUDIO)) {
            cleanupPartial();
            return fail(result, "copy-only audio codec parameters are incompatible");
        }
        const int64_t available_duration = std::max<int64_t>(
            0, result->duration_us - audio_request.start_offset_us);
        const int64_t source_audio_duration = streamDurationUs(input.get(), input_audio);
        if ((source_audio_duration > 0 &&
             source_audio_duration + 50'000 < available_duration) ||
            (audio_request.duration_us > 0 &&
             audio_request.duration_us + 50'000 < available_duration)) {
            cleanupPartial();
            return fail(result, "copy-only audio is shorter than the video timeline");
        }
        const int64_t audio_duration = audio_request.duration_us > 0
            ? std::min(audio_request.duration_us, available_duration)
            : available_duration;
        if (audio_duration <= 0) {
            cleanupPartial();
            return fail(result, "copy-only audio has no duration inside the video timeline");
        }
        input.reset();
        TimestampState audio_state_local;
        if (!readPackets(audio_request.path, AVMEDIA_TYPE_AUDIO, audio_index, streams.audio,
                         audio_request.start_offset_us, audio_duration, audio_state_local,
                         packets, result->audio_packets, error)) {
            cleanupPartial();
            return fail(result, error);
        }
    }

    if (result->video_packets == 0) {
        cleanupPartial();
        return fail(result, "copy-only packet mux found no video packets in the requested ranges");
    }
    const bool needsAudio = request.audio.has_value() || std::any_of(
        request.video_segments.begin(), request.video_segments.end(),
        [](const CopyOnlyVideoSegment& segment) { return segment.include_audio; });
    if (needsAudio && result->audio_packets == 0) {
        cleanupPartial();
        return fail(result, "copy-only packet mux found no audio packets in the requested ranges");
    }

    std::stable_sort(packets.begin(), packets.end(), [](const auto& left, const auto& right) {
        if (left->sort_dts != right->sort_dts) {
            if (!validTimestamp(left->sort_dts)) return false;
            if (!validTimestamp(right->sort_dts)) return true;
            return left->sort_dts < right->sort_dts;
        }
        return left->output_stream_index < right->output_stream_index;
    });

    if (!(output->oformat->flags & AVFMT_NOFILE)) {
        const int ioResult = avio_open(&output->pb, partial.c_str(), AVIO_FLAG_WRITE);
        if (ioResult < 0) {
            cleanupPartial();
            return fail(result, "avio_open: " + ffmpegError(ioResult));
        }
    }
    const int headerResult = avformat_write_header(output.get(), nullptr);
    if (headerResult < 0) {
        if (output->pb != nullptr) avio_closep(&output->pb);
        cleanupPartial();
        return fail(result, "avformat_write_header: " + ffmpegError(headerResult));
    }

    TimestampState final_video_state;
    TimestampState final_audio_state;
    for (auto& holder : packets) {
        holder->packet.stream_index = holder->output_stream_index;
        const AVStream* output_stream = nullptr;
        if (streams.video != nullptr && holder->output_stream_index == streams.video->index) {
            output_stream = streams.video;
        } else if (streams.audio != nullptr && holder->output_stream_index == streams.audio->index) {
            output_stream = streams.audio;
        }
        if (output_stream == nullptr) {
            av_write_trailer(output.get());
            if (output->pb != nullptr) avio_closep(&output->pb);
            cleanupPartial();
            return fail(result, "packet references an unknown output stream");
        }
        // MP4 is allowed to replace the provisional microsecond time base
        // while writing the header. Convert the already rewritten packet from
        // the canonical pipeline base to the muxer's final stream base now.
        av_packet_rescale_ts(&holder->packet, kMicrosecondTimeBase,
                             output_stream->time_base);
        TimestampState& final_state =
            (streams.video != nullptr && output_stream->index == streams.video->index)
                ? final_video_state : final_audio_state;
        normalizeFinalPacket(holder->packet, final_state);
        const int writeResult = av_interleaved_write_frame(output.get(), &holder->packet);
        if (writeResult < 0) {
            av_write_trailer(output.get());
            if (output->pb != nullptr) avio_closep(&output->pb);
            cleanupPartial();
            return fail(result, "av_interleaved_write_frame: " + ffmpegError(writeResult));
        }
    }
    const int trailerResult = av_write_trailer(output.get());
    if (output->pb != nullptr) {
        avio_closep(&output->pb);
    }
    if (trailerResult < 0) {
        cleanupPartial();
        return fail(result, "av_write_trailer: " + ffmpegError(trailerResult));
    }
    output.reset();

    // Validate the completed partial before publishing it. This catches a
    // short/corrupt packet range even when at least one packet was available;
    // no truncated artifact may replace the caller's existing output.
    const auto finalProbe = probeMediaInProcess(partial);
    const double expectedDuration = static_cast<double>(result->duration_us) / 1'000'000.0;
    const auto finalVideo = finalProbe.has_value()
        ? std::find_if(finalProbe->streams.begin(), finalProbe->streams.end(),
                       [](const MediaProbeStream& stream) { return stream.is_video; })
        : std::vector<MediaProbeStream>::const_iterator{};
    const bool videoCoversTimeline = finalProbe.has_value() &&
        finalVideo != finalProbe->streams.end() &&
        finalVideo->duration_verified &&
        finalVideo->duration_seconds + 0.08 >= expectedDuration;
    if (!finalProbe.has_value() || !finalProbe->duration_verified ||
        std::abs(finalProbe->duration_seconds - expectedDuration) > 0.08 ||
        !videoCoversTimeline) {
        cleanupPartial();
        return fail(result, "copy-only packet mux output duration does not cover the requested timeline");
    }

    bool durable = false;
    if (!file::publishAtomic(partial, output_path, &error, &durable)) {
        cleanupPartial();
        return fail(result, error);
    }

    result->output_durable = durable;
    result->success = true;
    result->error.clear();
    return true;
}

} // namespace velox::media

#else  // !VELOX_ENABLE_LIBAV

namespace velox::media {

bool muxCopyOnly(const CopyOnlyMuxRequest& request, CopyOnlyMuxResult* result) {
    (void)request;
    if (result != nullptr) {
        *result = CopyOnlyMuxResult{};
        result->success = false;
        result->output_durable = false;
        result->error =
            "VELOX_ENABLE_LIBAV=OFF: in-process packet mux requires "
            "libavformat/libavcodec/libavutil; rebuild with "
            "-DVELOX_ENABLE_LIBAV=ON";
    }
    return false;
}

} // namespace velox::media

#endif // VELOX_ENABLE_LIBAV
