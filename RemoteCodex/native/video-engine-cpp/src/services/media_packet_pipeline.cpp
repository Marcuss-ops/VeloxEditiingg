#include "velox/services/media_packet_pipeline.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/io_counters.hpp"
#include "velox/services/media_probe.hpp"
#include "velox/services/segment_execution.hpp"

// The in-process packet copy pipeline is built only when VELOX_ENABLE_LIBAV
// is ON. Without the flag a fail-closed stub is compiled instead: the
// engine's copy-only packet branch is also compiled out (RenderEngine falls
// back to the legacy segment/concat path), so this stub exists to keep the
// symbol defined for any caller built without the flag.
#ifdef VELOX_ENABLE_LIBAV

// LibAV-aware component API (Demuxer, PacketTrimmer, TimestampRewriter).
// Included inside the guard: the header errors out without VELOX_ENABLE_LIBAV.
#include "velox/services/media_packet_components.hpp"
#include "velox/services/segment_execution_libav.hpp"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
#include <libavutil/mathematics.h>
}

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <limits>
#include <sstream>
#include <system_error>
#include <unistd.h>

namespace fs = std::filesystem;

// ─── Named packet-pipeline components (Demuxer, PacketTrimmer,
//     TimestampRewriter) ──────────────────────────────────────────────────
// Public API declared in media_packet_components.hpp. Everything here is
// stream-copy: raw AVPackets are demuxed, trimmed and timestamp-rewritten
// in-process; no ffprobe or ffmpeg child is ever spawned per segment.
namespace velox::media::packet {

PacketHolder::PacketHolder() = default;

PacketHolder::~PacketHolder() {
    av_packet_unref(&packet);
}

bool validTimestamp(int64_t value) {
    return value != AV_NOPTS_VALUE;
}

static std::string ffmpegError(int error) {
    char buffer[AV_ERROR_MAX_STRING_SIZE]{};
    av_strerror(error, buffer, sizeof(buffer));
    return buffer;
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
    // main loop AND the demuxAndRewrite reopen).
    services::recordInputOpen(path.string());
    context_ = raw;
    const int infoResult = avformat_find_stream_info(context_, nullptr);
    if (infoResult < 0) {
        error = "avformat_find_stream_info(" + path.string() + "): " + ffmpegError(infoResult);
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
    const int seekResult = avformat_seek_file(
        context_, stream_index, std::numeric_limits<int64_t>::min(), target,
        std::numeric_limits<int64_t>::max(), AVSEEK_FLAG_BACKWARD);
    if (seekResult < 0) {
        error = "avformat_seek_file: " + ffmpegError(seekResult);
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
    const int readResult = av_read_frame(context_, &packet);
    if (readResult == AVERROR_EOF) {
        eof = true;
        return true;
    }
    if (readResult < 0) {
        error = ffmpegError(readResult);
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

static int64_t relativeTimestamp(int64_t timestamp,
                                 int64_t source_start,
                                 AVRational input_time_base);

bool InputSession::open(const fs::path& path, std::string& error) {
    if (demuxer_.isOpen()) {
        if (path_ == path) {
            return true;
        }
        demuxer_.close();
        keyframe_decisions_.clear();
    }
    if (!demuxer_.open(path, error)) {
        return false;
    }
    path_ = path;
    return true;
}

bool InputSession::seekToTimestampUs(int stream_index, int64_t timestamp_us, std::string& error) {
    return demuxer_.seekToTimestampUs(stream_index, timestamp_us, error);
}

bool InputSession::sourceWindowStartsOnKeyframe(int input_stream_index,
                                                int64_t source_in_us,
                                                std::string& error) {
    if (source_in_us < 0) {
        error = "copy-only source_in_us must be non-negative";
        return false;
    }
    const auto cacheKey = std::make_pair(input_stream_index, source_in_us);
    const auto cached = keyframe_decisions_.find(cacheKey);
    if (cached != keyframe_decisions_.end()) {
        if (!cached->second) {
            error = "copy-only source window must start on an exact video keyframe: " +
                path_.string() + " source_in_us=" + std::to_string(source_in_us);
        }
        return cached->second;
    }
    if (!demuxer_.isOpen()) {
        error = "input session is not open";
        return false;
    }
    if (input_stream_index < 0 ||
        static_cast<unsigned int>(input_stream_index) >= demuxer_.raw()->nb_streams) {
        error = "stream index is invalid for " + path_.string();
        return false;
    }
    const AVStream* input_stream = demuxer_.stream(input_stream_index);
    if (input_stream == nullptr || input_stream->codecpar == nullptr ||
        input_stream->codecpar->codec_type != AVMEDIA_TYPE_VIDEO) {
        error = "requested keyframe stream is missing from " + path_.string();
        return false;
    }
    if (!demuxer_.seekToTimestampUs(input_stream_index, source_in_us, error)) {
        return false;
    }
    const int64_t source_start = validTimestamp(input_stream->start_time)
        ? input_stream->start_time : 0;
    AVPacket* packet = av_packet_alloc();
    if (packet == nullptr) {
        error = "av_packet_alloc failed while checking keyframe alignment";
        return false;
    }
    bool found = false;
    bool eof = false;
    // Hoisted out of the per-packet loop for the same reason as the
    // demuxAndRewrite read loop: readError is reused across packets and
    // only assigned on failure (which returns immediately).
    std::string readError;
    while (!eof) {
        if (!demuxer_.readFrame(*packet, eof, readError)) {
            error = "av_read_frame(" + path_.string() + ") while checking keyframe alignment: " + readError;
            av_packet_free(&packet);
            return false;
        }
        if (eof) break;
        if (packet->stream_index == input_stream_index &&
            (packet->flags & AV_PKT_FLAG_KEY) != 0) {
            const int64_t packet_us = relativeTimestamp(
                packet->pts != AV_NOPTS_VALUE ? packet->pts : packet->dts,
                source_start, input_stream->time_base);
            if (packet_us == source_in_us) {
                found = true;
                av_packet_unref(packet);
                break;
            }
        }
        av_packet_unref(packet);
    }
    av_packet_free(&packet);
    keyframe_decisions_[cacheKey] = found;
    if (!found) {
        error = "copy-only source window must start on an exact video keyframe: " +
            path_.string() + " source_in_us=" + std::to_string(source_in_us);
    }
    return found;
}

InputSession* InputSessionRegistry::resolve(const fs::path& path, std::string& error) {
    const std::string key = path.lexically_normal().string();
    auto existing = sessions_.find(key);
    if (existing != sessions_.end()) {
        return existing->second.get();
    }
    auto session = std::make_unique<InputSession>();
    if (!session->open(path, error)) {
        return nullptr;
    }
    InputSession* result = session.get();
    sessions_.emplace(key, std::move(session));
    return result;
}

// Same-TU helpers only (no header declarations): keep them internal so the
// packet namespace never exposes accidental external linkage.
static int64_t rescale(int64_t value, AVRational source, AVRational destination) {
    if (!validTimestamp(value)) {
        return AV_NOPTS_VALUE;
    }
    return av_rescale_q_rnd(
        value, source, destination,
        static_cast<AVRounding>(AV_ROUND_NEAR_INF | AV_ROUND_PASS_MINMAX));
}

static int64_t relativeTimestamp(int64_t timestamp,
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
                   const AVStream* output_stream,
                   int64_t source_start,
                   int64_t source_in_us,
                   int64_t timeline_offset,
                   int64_t segment_duration,
                   TimestampState& state,
                   int64_t& sort_dts) {
    const int64_t reference = validTimestamp(packet.pts) ? packet.pts : packet.dts;
    int64_t relative_reference = relativeTimestamp(
        reference, source_start, input_stream->time_base);
    if (validTimestamp(relative_reference)) {
        relative_reference -= source_in_us;
    }
    if (!validTimestamp(relative_reference) ||
        (source_in_us > 0 && relative_reference < 0) ||
        relative_reference >= segment_duration) {
        return false;
    }

    packet.pts = relativeTimestamp(packet.pts, source_start, input_stream->time_base);
    packet.dts = relativeTimestamp(packet.dts, source_start, input_stream->time_base);
    if (validTimestamp(packet.pts)) packet.pts -= source_in_us;
    if (validTimestamp(packet.dts)) packet.dts -= source_in_us;
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

bool demuxAndRewrite(const fs::path& path,
                     AVMediaType type,
                     int input_stream_index,
                     AVStream* output_stream,
                     int64_t timeline_offset,
                     int64_t source_in_us,
                     int64_t duration_us,
                     TimestampState& state,
                     std::vector<std::unique_ptr<PacketHolder>>& packets,
                     int64_t& packet_count,
                     std::string& error,
                     bool extend_video_tail) {
    Demuxer input;
    if (!input.open(path, error)) {
        return false;
    }

    return demuxAndRewrite(input, path, type, input_stream_index, output_stream,
                           timeline_offset, source_in_us, duration_us, state,
                           packets, packet_count, error, extend_video_tail);
}

bool demuxAndRewrite(Demuxer& input,
                     const fs::path& path,
                     AVMediaType type,
                     int input_stream_index,
                     AVStream* output_stream,
                     int64_t timeline_offset,
                     int64_t source_in_us,
                     int64_t duration_us,
                     TimestampState& state,
                     std::vector<std::unique_ptr<PacketHolder>>& packets,
                     int64_t& packet_count,
                     std::string& error,
                     bool extend_video_tail) {
    if (!input.seekToTimestampUs(input_stream_index, source_in_us, error)) {
        return false;
    }
    if (input_stream_index < 0 ||
        static_cast<unsigned int>(input_stream_index) >= input.raw()->nb_streams) {
        error = "stream index is invalid for " + path.string();
        return false;
    }
    const AVStream* input_stream = input.stream(input_stream_index);
    if (input_stream == nullptr || input_stream->codecpar == nullptr ||
        input_stream->codecpar->codec_type != type) {
        error = "requested stream is missing from " + path.string();
        return false;
    }

    const int64_t source_start = validTimestamp(input_stream->start_time)
        ? input_stream->start_time : 0;
    AVPacket* packet = av_packet_alloc();
    if (packet == nullptr) {
        error = "av_packet_alloc failed";
        return false;
    }

    AVPacket last_video_packet{};
    bool have_last_video_packet = false;

    bool eof = false;
    // Hoisted out of the per-packet loop: readError is reused across
    // packets instead of being default-constructed once per packet. The
    // demuxer only assigns it on failure (which returns immediately), so
    // reuse across the success path is safe.
    std::string readError;
    while (!eof) {
        if (!input.readFrame(*packet, eof, readError)) {
            error = "av_read_frame(" + path.string() + "): " + readError;
            av_packet_free(&packet);
            return false;
        }
        if (eof) {
            break;
        }
        if (packet->stream_index == input_stream_index) {
            int64_t sort_dts = AV_NOPTS_VALUE;
            if (rewritePacket(*packet, input_stream, output_stream, source_start,
                              source_in_us,
                              timeline_offset, duration_us, state, sort_dts)) {
                if (extend_video_tail && type == AVMEDIA_TYPE_VIDEO) {
                    av_packet_unref(&last_video_packet);
                    if (av_packet_ref(&last_video_packet, packet) < 0) {
                        error = "av_packet_ref failed while retaining the last video packet";
                        av_packet_free(&packet);
                        return false;
                    }
                    have_last_video_packet = true;
                }
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

    if (extend_video_tail && type == AVMEDIA_TYPE_VIDEO && have_last_video_packet) {
        const int64_t timeline_end = timeline_offset + duration_us;
        AVRational frame_rate = input_stream->avg_frame_rate;
        if (frame_rate.num <= 0 || frame_rate.den <= 0) {
            frame_rate = input_stream->r_frame_rate;
        }
        const int64_t frame_duration_us = frame_rate.num > 0 && frame_rate.den > 0
            ? std::max<int64_t>(1, av_rescale_q(
                  1, AVRational{frame_rate.den, frame_rate.num}, kMicrosecondTimeBase))
            : 1;
        const int64_t packet_step_us = last_video_packet.duration > 0
            ? last_video_packet.duration
            : frame_duration_us;
        int64_t next_pts = validTimestamp(last_video_packet.pts)
            ? last_video_packet.pts + packet_step_us : AV_NOPTS_VALUE;
        int64_t next_dts = validTimestamp(last_video_packet.dts)
            ? last_video_packet.dts + packet_step_us : AV_NOPTS_VALUE;

        while (validTimestamp(next_pts) || validTimestamp(next_dts)) {
            const int64_t next_timestamp = validTimestamp(next_pts)
                ? next_pts : next_dts;
            if (next_timestamp >= timeline_end) {
                break;
            }

            AVPacket duplicate{};
            if (av_packet_ref(&duplicate, &last_video_packet) < 0) {
                av_packet_unref(&last_video_packet);
                error = "av_packet_ref failed while extending the video tail";
                return false;
            }
            duplicate.pts = next_pts;
            duplicate.dts = next_dts;
            duplicate.duration = std::min<int64_t>(
                packet_step_us, timeline_end - next_timestamp);
            duplicate.stream_index = output_stream->index;

            auto holder = std::make_unique<PacketHolder>();
            av_packet_move_ref(&holder->packet, &duplicate);
            holder->output_stream_index = output_stream->index;
            holder->sort_dts = validTimestamp(next_dts) ? next_dts : next_pts;
            packets.push_back(std::move(holder));
            ++packet_count;

            if (validTimestamp(next_pts)) next_pts += packet_step_us;
            if (validTimestamp(next_dts)) next_dts += packet_step_us;
        }
    }
    av_packet_unref(&last_video_packet);
    return true;
}

bool sourceWindowStartsOnKeyframe(const fs::path& path,
                                  int input_stream_index,
                                  int64_t source_in_us,
                                  std::string& error) {
    InputSession session;
    if (!session.open(path, error)) {
        return false;
    }
    return session.sourceWindowStartsOnKeyframe(input_stream_index, source_in_us, error);
}

} // namespace velox::media::packet

namespace velox::media {
namespace {

struct OutputContextDeleter {
    void operator()(AVFormatContext* context) const {
        if (context != nullptr) {
            avformat_free_context(context);
        }
    }
};
using UniqueOutputContext = std::unique_ptr<AVFormatContext, OutputContextDeleter>;

struct OutputStreams {
    AVStream* video{nullptr};
    AVStream* audio{nullptr};
};

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
        error = "avcodec_parameters_copy: " + packet::ffmpegError(copyResult);
        return false;
    }
    // Let the MP4 muxer select the correct sample entry/tag.
    destination->codecpar->codec_tag = 0;
    destination->time_base = packet::kMicrosecondTimeBase;
    if (input->avg_frame_rate.num > 0 && input->avg_frame_rate.den > 0) {
        destination->avg_frame_rate = input->avg_frame_rate;
    }
    return true;
}

int64_t streamDurationUs(const AVFormatContext* context, const AVStream* stream) {
    if (stream != nullptr && packet::validTimestamp(stream->duration) && stream->duration > 0) {
        return packet::rescale(stream->duration, stream->time_base, packet::kMicrosecondTimeBase);
    }
    if (context != nullptr && packet::validTimestamp(context->duration) && context->duration > 0) {
        return packet::rescale(context->duration, AVRational{1, AV_TIME_BASE}, packet::kMicrosecondTimeBase);
    }
    return 0;
}

void normalizeFinalPacket(AVPacket& packet, packet::TimestampState& state) {
    if (packet::validTimestamp(packet.dts)) {
        if (packet::validTimestamp(state.last_dts) && packet.dts <= state.last_dts) {
            packet.dts = state.last_dts + 1;
        }
        state.last_dts = packet.dts;
    }
    if (packet::validTimestamp(packet.pts)) {
        if (packet::validTimestamp(state.last_pts) && packet.pts <= state.last_pts) {
            packet.pts = state.last_pts + 1;
        }
        state.last_pts = packet.pts;
    }
    if (packet::validTimestamp(packet.pts) && packet::validTimestamp(packet.dts) && packet.pts < packet.dts) {
        packet.pts = packet.dts;
        state.last_pts = packet.pts;
    }
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
        return fail(result, "avformat_alloc_output_context2: " + packet::ffmpegError(allocResult));
    }
    UniqueOutputContext output(raw_output);

    OutputStreams streams;
    std::vector<std::unique_ptr<packet::PacketHolder>> packets;
    packets.reserve(request.video_segments.size() * 16);
    packet::TimestampState video_state;
    packet::TimestampState segment_audio_state;
    packet::InputSessionRegistry input_sessions;
    int64_t timeline_offset = 0;
    std::string error;

    for (const auto& segment : request.video_segments) {
        if (segment.source_duration_us <= 0 || segment.source_in_us < 0) {
            cleanupPartial();
            return fail(result, "copy-only packet mux rejects invalid source video window");
        }
        if (segment.source_in_us > std::numeric_limits<int64_t>::max() -
                segment.source_duration_us) {
            cleanupPartial();
            return fail(result, "copy-only source video window overflows int64");
        }
        // A normalized transcode output already lives in the canonical
        // profile and starts at its own frame zero; a non-zero source_in
        // would silently trim the encoded segment and must be rejected.
        if (segment.normalized && segment.source_in_us != 0) {
            cleanupPartial();
            return fail(result, "copy-only normalized segment must start at source_in_us 0");
        }
        packet::InputSession* session = input_sessions.resolve(segment.path, error);
        if (session == nullptr) {
            cleanupPartial();
            return fail(result, error);
        }
        packet::Demuxer& input = session->demuxer();
        const int video_index = input.firstStream(AVMEDIA_TYPE_VIDEO);
        if (video_index < 0) {
            cleanupPartial();
            return fail(result, "video stream missing from " + segment.path.string());
        }
        const AVStream* input_video = input.stream(video_index);
        // A normalized segment is its own complete segment (source_in_us == 0),
        // so the raw-source "shorter than requested" heuristic does not apply;
        // the final output-duration validation below still bounds the whole
        // assembled timeline. Raw copy ranges may opt into a decode-free tail
        // freeze when the requested window exceeds the source duration.
        const int64_t source_video_duration = streamDurationUs(input.raw(), input_video);
        const bool extendVideoTail = !segment.normalized &&
            source_video_duration > 0 &&
            source_video_duration + 50'000 < segment.source_in_us + segment.source_duration_us;
        // A normalized segment starts on a keyframe by construction (a fresh
        // encode's first frame); only raw source ranges need the keyframe
        // probe, which never guesses for a non-keyframe cut.
        bool keyframeSafe = true;
        if (!segment.normalized) {
            keyframeSafe = session->sourceWindowStartsOnKeyframe(
                video_index, segment.source_in_us, error);
            if (!keyframeSafe) {
                cleanupPartial();
                return fail(result, error);
            }
        }
        const MediaSignature sourceVideoSignature = mediaSignatureFromStream(input_video);
        if (streams.video == nullptr) {
            if (!initializeOutputStream(output.get(), input_video, streams.video, error)) {
                cleanupPartial();
                return fail(result, error);
            }
        }
        SegmentExecutionRequest videoExecution;
        videoExecution.source = sourceVideoSignature;
        videoExecution.target = streams.video == nullptr
            ? sourceVideoSignature
            : mediaSignatureFromStream(streams.video);
        videoExecution.source_window_keyframe_safe = keyframeSafe;
        const SegmentExecutionDecision videoDecision =
            resolveSegmentExecution(videoExecution);
        if (videoDecision.mode != SegmentExecutionMode::PacketCopy) {
            cleanupPartial();
            return fail(result, "copy-only video segment execution rejected at " +
                segment.path.string() + ": " + videoDecision.reason);
        }

        int audio_index = -1;
        int64_t audio_duration_us = segment.source_duration_us;
        if (segment.include_audio) {
            audio_index = input.firstStream(AVMEDIA_TYPE_AUDIO);
            if (audio_index < 0) {
                cleanupPartial();
                return fail(result, "copy-only segment requests audio but the source has no audio stream");
            }
            const AVStream* input_audio = input.stream(audio_index);
            const int64_t source_audio_duration = streamDurationUs(input.raw(), input_audio);
            audio_duration_us = source_audio_duration > 0
                ? std::min<int64_t>(segment.source_duration_us, source_audio_duration)
                : segment.source_duration_us;
            const MediaSignature sourceAudioSignature = mediaSignatureFromStream(input_audio);
            if (streams.audio == nullptr) {
                if (!initializeOutputStream(output.get(), input_audio, streams.audio, error)) {
                    cleanupPartial();
                    return fail(result, error);
                }
            }
            SegmentExecutionRequest audioExecution;
            audioExecution.source = sourceAudioSignature;
            audioExecution.target = streams.audio == nullptr
                ? sourceAudioSignature
                : mediaSignatureFromStream(streams.audio);
            // Audio has no video keyframe boundary; the source window has
            // already passed the packet-duration checks above.
            audioExecution.source_window_keyframe_safe = true;
            const SegmentExecutionDecision audioDecision =
                resolveSegmentExecution(audioExecution);
            if (audioDecision.mode != SegmentExecutionMode::PacketCopy) {
                cleanupPartial();
                return fail(result, "copy-only audio segment execution rejected at " +
                    segment.path.string() + ": " + audioDecision.reason);
            }
        }
        if (!packet::demuxAndRewrite(input, segment.path, AVMEDIA_TYPE_VIDEO, video_index, streams.video,
                                     timeline_offset, segment.source_in_us, segment.source_duration_us,
                                     video_state, packets,
                                     result->video_packets, error, extendVideoTail)) {
            cleanupPartial();
            return fail(result, error);
        }
        if (segment.include_audio && !packet::demuxAndRewrite(
                input, segment.path, AVMEDIA_TYPE_AUDIO, audio_index, streams.audio,
                timeline_offset, segment.source_in_us, audio_duration_us,
                segment_audio_state, packets,
                result->audio_packets, error)) {
            cleanupPartial();
            return fail(result, error);
        }
        timeline_offset += segment.source_duration_us;
    }
    result->duration_us = timeline_offset;

    if (request.audio.has_value()) {
        const auto& audio_request = *request.audio;
        packet::InputSession* session = input_sessions.resolve(audio_request.path, error);
        if (session == nullptr) {
            cleanupPartial();
            return fail(result, error);
        }
        packet::Demuxer& input = session->demuxer();
        const int audio_index = input.firstStream(AVMEDIA_TYPE_AUDIO);
        if (audio_index < 0) {
            cleanupPartial();
            return fail(result, "audio stream missing from " + audio_request.path.string());
        }
        const AVStream* input_audio = input.stream(audio_index);
        if (streams.audio == nullptr) {
            if (!initializeOutputStream(output.get(), input_audio, streams.audio, error)) {
                cleanupPartial();
                return fail(result, error);
            }
        } else {
            std::string compatibility_reason;
            if (!mediaSignaturesCompatible(
                    mediaSignatureFromStream(input_audio), mediaSignatureFromStream(streams.audio),
                    &compatibility_reason)) {
                cleanupPartial();
                return fail(result, "copy-only audio codec parameters are incompatible: " +
                    compatibility_reason);
            }
        }
        const int64_t available_duration = std::max<int64_t>(
            0, result->duration_us - audio_request.start_offset_us);
        const int64_t source_audio_duration = streamDurationUs(input.raw(), input_audio);
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
        packet::TimestampState audio_state_local;
        if (!packet::demuxAndRewrite(input, audio_request.path, AVMEDIA_TYPE_AUDIO, audio_index, streams.audio,
                                     0, audio_request.start_offset_us, audio_duration, audio_state_local,
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
            if (!packet::validTimestamp(left->sort_dts)) return false;
            if (!packet::validTimestamp(right->sort_dts)) return true;
            return left->sort_dts < right->sort_dts;
        }
        return left->output_stream_index < right->output_stream_index;
    });

    if (!(output->oformat->flags & AVFMT_NOFILE)) {
        const int ioResult = avio_open(&output->pb, partial.c_str(), AVIO_FLAG_WRITE);
        if (ioResult < 0) {
            cleanupPartial();
            return fail(result, "avio_open: " + packet::ffmpegError(ioResult));
        }
    }
    const int headerResult = avformat_write_header(output.get(), nullptr);
    if (headerResult < 0) {
        if (output->pb != nullptr) avio_closep(&output->pb);
        cleanupPartial();
        return fail(result, "avformat_write_header: " + packet::ffmpegError(headerResult));
    }

    packet::TimestampState final_video_state;
    packet::TimestampState final_audio_state;
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
        av_packet_rescale_ts(&holder->packet, packet::kMicrosecondTimeBase,
                             output_stream->time_base);
        packet::TimestampState& final_state =
            (streams.video != nullptr && output_stream->index == streams.video->index)
                ? final_video_state : final_audio_state;
        normalizeFinalPacket(holder->packet, final_state);
        const int writeResult = av_interleaved_write_frame(output.get(), &holder->packet);
        if (writeResult < 0) {
            av_write_trailer(output.get());
            if (output->pb != nullptr) avio_closep(&output->pb);
            cleanupPartial();
            return fail(result, "av_interleaved_write_frame: " + packet::ffmpegError(writeResult));
        }
    }
    const int trailerResult = av_write_trailer(output.get());
    if (output->pb != nullptr) {
        avio_closep(&output->pb);
    }
    if (trailerResult < 0) {
        cleanupPartial();
        return fail(result, "av_write_trailer: " + packet::ffmpegError(trailerResult));
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
