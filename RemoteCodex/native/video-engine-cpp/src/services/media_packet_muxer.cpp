#ifdef VELOX_ENABLE_LIBAV

#include "media_packet_pipeline_internal.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/segment_execution.hpp"
#include "velox/services/segment_execution_libav.hpp"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
}

#include <algorithm>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

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

std::string validateCopyOnlyRequest(const CopyOnlyMuxRequest& request) {
    if (request.video_segments.empty()) {
        return "copy-only packet mux requires at least one video segment";
    }
    if (request.output_path.empty()) {
        return "copy-only packet mux requires an output path";
    }
    if (request.audio.has_value() && request.audio->start_offset_us < 0) {
        return "copy-only packet mux rejects negative audio offsets";
    }
    if (request.audio.has_value() && std::any_of(
            request.video_segments.begin(), request.video_segments.end(),
            [](const CopyOnlyVideoSegment& segment) { return segment.include_audio; })) {
        return "copy-only cannot combine segment audio with final audio";
    }
    return {};
}

void sortCopyOnlyPackets(
    std::vector<std::unique_ptr<packet::PacketHolder>>& packets) {
    std::stable_sort(packets.begin(), packets.end(), [](const auto& left, const auto& right) {
        if (left->sort_dts != right->sort_dts) {
            if (!packet::validTimestamp(left->sort_dts)) return false;
            if (!packet::validTimestamp(right->sort_dts)) return true;
            return left->sort_dts < right->sort_dts;
        }
        return left->output_stream_index < right->output_stream_index;
    });
}

bool writeCopyOnlyOutput(
    UniqueOutputContext& output,
    const OutputStreams& streams,
    const fs::path& partial,
    const fs::path& output_path,
    std::vector<std::unique_ptr<packet::PacketHolder>>& packets,
    CopyOnlyMuxResult* result,
    int64_t expected_duration_us,
    std::string& error) {
    const auto cleanupPartial = [&]() {
        std::error_code remove_error;
        fs::remove(partial, remove_error);
    };

    sortCopyOnlyPackets(packets);
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

    int64_t writtenVideoEndUs = AV_NOPTS_VALUE;
    packet::TimestampState finalVideoState;
    packet::TimestampState finalAudioState;
    for (auto& holder : packets) {
        holder->packet.stream_index = holder->output_stream_index;
        const AVStream* outputStream = nullptr;
        if (streams.video != nullptr && holder->output_stream_index == streams.video->index) {
            outputStream = streams.video;
        } else if (streams.audio != nullptr && holder->output_stream_index == streams.audio->index) {
            outputStream = streams.audio;
        }
        if (outputStream == nullptr) {
            av_write_trailer(output.get());
            if (output->pb != nullptr) avio_closep(&output->pb);
            cleanupPartial();
            return fail(result, "packet references an unknown output stream");
        }
        if (outputStream == streams.video) {
            const int64_t base = packet::validTimestamp(holder->packet.pts)
                ? holder->packet.pts : holder->packet.dts;
            if (packet::validTimestamp(base)) {
                const int64_t endUs = holder->packet.duration > 0
                    ? base + holder->packet.duration : base;
                if (!packet::validTimestamp(writtenVideoEndUs) ||
                    endUs > writtenVideoEndUs) {
                    writtenVideoEndUs = endUs;
                }
            }
        }
        av_packet_rescale_ts(&holder->packet, packet::kMicrosecondTimeBase,
                             outputStream->time_base);
        packet::TimestampState& finalState =
            (streams.video != nullptr && outputStream->index == streams.video->index)
                ? finalVideoState : finalAudioState;
        normalizeFinalPacket(holder->packet, finalState);
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

    constexpr int64_t kDurationToleranceUs = 80'000;
    if (!packet::validTimestamp(writtenVideoEndUs) ||
        writtenVideoEndUs + kDurationToleranceUs < expected_duration_us) {
        cleanupPartial();
        return fail(result, "copy-only packet mux video stream ends before the requested timeline");
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

} // namespace

bool runCopyOnlyMux(const CopyOnlyMuxRequest& request, CopyOnlyMuxResult* result) {
    CopyOnlyMuxResult local;
    if (result == nullptr) {
        result = &local;
    }
    *result = CopyOnlyMuxResult{};

    if (const std::string validationError = validateCopyOnlyRequest(request);
        !validationError.empty()) {
        return fail(result, validationError);
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
    std::optional<MediaSignature> canonical_video_signature;
    std::optional<MediaSignature> canonical_audio_signature;
    std::vector<std::unique_ptr<packet::PacketHolder>> packets;
    packets.reserve(request.video_segments.size() * 16);
    packet::TimestampState video_state;
    packet::TimestampState segment_audio_state;
    packet::InputSessionRegistry input_sessions;
    int64_t timeline_offset = 0;
    std::string error;

    {
        std::vector<fs::path> all_inputs;
        all_inputs.reserve(request.video_segments.size() +
                           (request.audio.has_value() ? 1 : 0));
        for (const auto& segment : request.video_segments) {
            all_inputs.push_back(segment.path);
        }
        if (request.audio.has_value()) {
            all_inputs.push_back(request.audio->path);
        }
        if (!input_sessions.preopen(all_inputs, error)) {
            cleanupPartial();
            return fail(result, error);
        }
    }

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
        const int64_t source_video_duration = streamDurationUs(input.raw(), input_video);
        const bool extendVideoTail = !segment.normalized &&
            source_video_duration > 0 &&
            source_video_duration + 50'000 < segment.source_in_us + segment.source_duration_us;
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
        if (!canonical_video_signature.has_value()) {
            canonical_video_signature = sourceVideoSignature;
        }
        if (streams.video == nullptr) {
            if (!initializeOutputStream(output.get(), input_video, streams.video, error)) {
                cleanupPartial();
                return fail(result, error);
            }
        }
        SegmentExecutionRequest videoExecution;
        videoExecution.source = sourceVideoSignature;
        videoExecution.target = *canonical_video_signature;
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
            if (!canonical_audio_signature.has_value()) {
                canonical_audio_signature = sourceAudioSignature;
            }
            if (streams.audio == nullptr) {
                if (!initializeOutputStream(output.get(), input_audio, streams.audio, error)) {
                    cleanupPartial();
                    return fail(result, error);
                }
            }
            SegmentExecutionRequest audioExecution;
            audioExecution.source = sourceAudioSignature;
            audioExecution.target = *canonical_audio_signature;
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
                                     video_state, packets, result->video_packets, error, extendVideoTail)) {
            cleanupPartial();
            return fail(result, error);
        }
        if (segment.include_audio && !packet::demuxAndRewrite(
                input, segment.path, AVMEDIA_TYPE_AUDIO, audio_index, streams.audio,
                timeline_offset, segment.source_in_us, audio_duration_us,
                segment_audio_state, packets, result->audio_packets, error)) {
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
            if (!canonical_audio_signature.has_value()) {
                canonical_audio_signature = mediaSignatureFromStream(input_audio);
            }
            if (!mediaSignaturesCompatible(
                    mediaSignatureFromStream(input_audio), *canonical_audio_signature,
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

    if (!writeCopyOnlyOutput(
            output, streams, partial, output_path, packets, result,
            result->duration_us, error)) {
        cleanupPartial();
        return false;
    }
    return true;
}

} // namespace velox::media

#endif // VELOX_ENABLE_LIBAV
