#ifdef VELOX_ENABLE_LIBAV

#include "media_packet_pipeline_internal.hpp"

#include "velox/core/execution_plan.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/io_counters.hpp"
#include "velox/services/media_packet_cursors.hpp"
#include "velox/services/media_packet_output_sink.hpp"
#include "velox/services/segment_execution_libav.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
}

namespace fs = std::filesystem;

namespace velox::media {
namespace {

struct OutputContextDeleter {
    void operator()(AVFormatContext* context) const {
        if (context != nullptr) avformat_free_context(context);
    }
};
using UniqueOutputContext = std::unique_ptr<AVFormatContext, OutputContextDeleter>;

struct OutputStreams { AVStream* video{nullptr}; AVStream* audio{nullptr}; };

struct PreparedVideoSegment {
    packet::InputSession* session{};
    fs::path path;
    int video_stream_index{-1};
    int audio_stream_index{-1};
    int64_t source_in_us{};
    int64_t source_duration_us{};
    int64_t timeline_offset_us{};
    bool include_audio{};
    bool extend_video_tail{};
};

struct PreparedAudioTrack {
    packet::InputSession* session{};
    fs::path path;
    int stream_index{-1};
    int64_t start_offset_us{};
    int64_t duration_us{};
};

struct PreparedCopyMuxPlan {
    std::vector<PreparedVideoSegment> segments;
    std::optional<PreparedAudioTrack> audio;
    OutputStreams streams;
    int64_t expected_duration_us{};
};

struct VideoCandidate {
    packet::InputSession* session{};
    fs::path path;
    int video_stream_index{-1};
    int audio_stream_index{-1};
    int64_t source_in_us{};
    int64_t source_duration_us{};
    int64_t timeline_offset_us{};
    bool include_audio{};
    bool extend_video_tail{};
};

struct AudioCandidate {
    packet::InputSession* session{};
    fs::path path;
    int stream_index{-1};
    std::size_t execution_index{};
};

bool initializeOutputStream(AVFormatContext* output, const AVStream* input,
                            AVStream*& destination, std::string& error) {
    destination = avformat_new_stream(output, nullptr);
    if (destination == nullptr) {
        error = "avformat_new_stream failed";
        return false;
    }
    const int rc = avcodec_parameters_copy(destination->codecpar, input->codecpar);
    if (rc < 0) {
        error = "avcodec_parameters_copy: " + packet::ffmpegError(rc);
        return false;
    }
    destination->codecpar->codec_tag = 0;
    destination->time_base = packet::kMicrosecondTimeBase;
    destination->avg_frame_rate = input->avg_frame_rate;
    return true;
}

int64_t streamDurationUs(const AVFormatContext* context, const AVStream* stream) {
    if (stream != nullptr && packet::validTimestamp(stream->duration) && stream->duration > 0) {
        return packet::rescale(stream->duration, stream->time_base,
                               packet::kMicrosecondTimeBase);
    }
    if (context != nullptr && packet::validTimestamp(context->duration) && context->duration > 0) {
        return packet::rescale(context->duration, {1, AV_TIME_BASE},
                               packet::kMicrosecondTimeBase);
    }
    return 0;
}

bool fail(CopyOnlyMuxResult* result, const std::string& error) {
    if (result != nullptr) {
        result->success = false;
        result->error = error;
    }
    return false;
}

std::string validateRequest(const CopyOnlyMuxRequest& request) {
    if (request.video_segments.empty()) return "copy-only packet mux requires at least one video segment";
    if (request.output_path.empty()) return "copy-only packet mux requires an output path";
    if (request.audio && request.audio->start_offset_us < 0) {
        return "copy-only packet mux rejects negative audio offsets";
    }
    if (request.audio && std::any_of(request.video_segments.begin(), request.video_segments.end(),
                                     [](const auto& segment) { return segment.include_audio; })) {
        return "copy-only cannot combine segment audio with final audio";
    }
    return {};
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
    if (packet::validTimestamp(packet.pts) && packet::validTimestamp(packet.dts) &&
        packet.pts < packet.dts) {
        packet.pts = packet.dts;
        state.last_pts = packet.pts;
    }
}

struct Writer {
    AVFormatContext* output{};
    OutputStreams streams;
    packet::TimestampState video_state;
    packet::TimestampState audio_state;
    int64_t video_end_us{AV_NOPTS_VALUE};
};

bool consume(packet::PendingPacket& pending, void* opaque, std::string& error) {
    auto& writer = *static_cast<Writer*>(opaque);
    AVStream* stream = nullptr;
    if (writer.streams.video != nullptr && pending.output_stream_index == writer.streams.video->index) {
        stream = writer.streams.video;
    } else if (writer.streams.audio != nullptr && pending.output_stream_index == writer.streams.audio->index) {
        stream = writer.streams.audio;
    }
    if (stream == nullptr) {
        error = "packet references an unknown output stream";
        return false;
    }
    if (stream == writer.streams.video) {
        const auto base = packet::validTimestamp(pending.packet.pts)
            ? pending.packet.pts : pending.packet.dts;
        if (packet::validTimestamp(base)) {
            const auto end = base + std::max<int64_t>(0, pending.packet.duration);
            if (!packet::validTimestamp(writer.video_end_us) || end > writer.video_end_us) {
                writer.video_end_us = end;
            }
        }
    }
    pending.packet.stream_index = stream->index;
    av_packet_rescale_ts(&pending.packet, packet::kMicrosecondTimeBase, stream->time_base);
    normalizeFinalPacket(pending.packet,
                         stream == writer.streams.video ? writer.video_state : writer.audio_state);
    services::recordFirstOutputWrite();
    const int rc = av_interleaved_write_frame(writer.output, &pending.packet);
    if (rc < 0) {
        error = "av_interleaved_write_frame: " + packet::ffmpegError(rc);
        return false;
    }
    return true;
}

bool preparePlan(const CopyOnlyMuxRequest& request, packet::InputSessionRegistry& sessions,
                 AVFormatContext* output, PreparedCopyMuxPlan& plan,
                 CopyOnlyMuxResult* result, std::string& error) {
    std::vector<fs::path> paths;
    paths.reserve(request.video_segments.size() + (request.audio ? 1 : 0));
    for (const auto& segment : request.video_segments) paths.push_back(segment.path);
    if (request.audio) paths.push_back(request.audio->path);
    if (!sessions.preopen(paths, error)) return fail(result, error);

    std::vector<VideoCandidate> videos;
    std::vector<AudioCandidate> audios;
    std::vector<core::SegmentExecutionInput> inputs;
    videos.reserve(request.video_segments.size());
    std::size_t input_reserve = request.video_segments.size() +
        (request.audio ? 1 : 0);
    for (const auto& segment : request.video_segments) {
        if (segment.include_audio && input_reserve <
            std::numeric_limits<std::size_t>::max()) {
            ++input_reserve;
        }
    }
    inputs.reserve(input_reserve);
    std::optional<MediaSignature> videoTarget = request.target_video_signature;
    std::optional<MediaSignature> audioTarget;
    int64_t timeline = 0;

    for (std::size_t index = 0; index < request.video_segments.size(); ++index) {
        const auto& segment = request.video_segments[index];
        if (segment.source_duration_us <= 0 || segment.source_in_us < 0) {
            return fail(result, "copy-only packet mux rejects invalid source video window");
        }
        if (segment.source_in_us >
            std::numeric_limits<int64_t>::max() - segment.source_duration_us) {
            return fail(result, "copy-only source video window overflows int64");
        }
        if (segment.normalized && segment.source_in_us != 0) {
            return fail(result, "copy-only normalized segment must start at source_in_us 0");
        }
        auto* session = sessions.resolve(segment.path, error);
        if (session == nullptr) return fail(result, error);
        auto& demuxer = session->demuxer();
        const int videoStreamIndex = demuxer.firstStream(AVMEDIA_TYPE_VIDEO);
        if (videoStreamIndex < 0) return fail(result, "video stream missing from " + segment.path.string());
        const AVStream* videoStream = demuxer.stream(videoStreamIndex);
        const MediaSignature signature = mediaSignatureFromStream(videoStream);
        if (!videoTarget) videoTarget = signature;
        const bool keyframeSafe = segment.normalized ||
            session->sourceWindowStartsOnKeyframe(videoStreamIndex, segment.source_in_us, error);
        if (!keyframeSafe) {
            return fail(result, "segment_execution_rejected: source window is not keyframe-safe for packet copy: " + error);
        }
        inputs.push_back(core::SegmentExecutionInput{
            index, segment.path, segment.source_in_us, segment.source_duration_us,
            signature, *videoTarget, segment.transform_required, keyframeSafe,
            segment.legacy_required});

        int audioStreamIndex = -1;
        if (segment.include_audio) {
            audioStreamIndex = demuxer.firstStream(AVMEDIA_TYPE_AUDIO);
            if (audioStreamIndex < 0) {
                return fail(result, "copy-only segment requests audio but the source has no audio stream");
            }
            const MediaSignature audioSignature = mediaSignatureFromStream(
                demuxer.stream(audioStreamIndex));
            if (!audioTarget) audioTarget = audioSignature;
            const std::size_t audioExecutionIndex = inputs.size();
            inputs.push_back(core::SegmentExecutionInput{
                index, segment.path, 0, segment.source_duration_us, audioSignature,
                *audioTarget, false, true, false});
            audios.push_back(AudioCandidate{session, segment.path, audioStreamIndex,
                                            audioExecutionIndex});
        }
        const int64_t sourceDuration = streamDurationUs(demuxer.raw(), videoStream);
        const bool extendTail = !segment.normalized && sourceDuration > 0 &&
            sourceDuration + 50000 < segment.source_in_us + segment.source_duration_us;
        videos.push_back(VideoCandidate{session, segment.path, videoStreamIndex,
                                        audioStreamIndex, segment.source_in_us,
                                        segment.source_duration_us, timeline,
                                        segment.include_audio, extendTail});
        if (timeline > std::numeric_limits<int64_t>::max() -
            segment.source_duration_us) {
            return fail(result, "copy-only packet mux timeline overflows int64");
        }
        timeline += segment.source_duration_us;
    }

    std::optional<AudioCandidate> finalAudio;
    if (request.audio) {
        const auto& audio = *request.audio;
        auto* session = sessions.resolve(audio.path, error);
        if (session == nullptr) return fail(result, error);
        auto& demuxer = session->demuxer();
        const int streamIndex = demuxer.firstStream(AVMEDIA_TYPE_AUDIO);
        if (streamIndex < 0) return fail(result, "audio stream missing from " + audio.path.string());
        const MediaSignature signature = mediaSignatureFromStream(demuxer.stream(streamIndex));
        const std::size_t executionIndex = inputs.size();
        inputs.push_back(core::SegmentExecutionInput{
            request.video_segments.size(), audio.path, 0, audio.duration_us,
            signature, signature, false, true, false});
        finalAudio = AudioCandidate{session, audio.path, streamIndex, executionIndex};
    }

    core::RuntimeExecutionPlanCompiler compiler;
    const auto executionPlan = compiler.compile(inputs, videoTarget.value_or(MediaSignature{}));
    for (const auto& executable : executionPlan.segments) {
        if (executable.execution.mode == SegmentExecutionMode::PacketCopy) continue;
        const std::string label = executable.index < request.video_segments.size()
            ? request.video_segments[executable.index].path.string() : "audio";
        return fail(result, "segment_execution_rejected: copy-only segment execution rejected at " +
            label + ": " + executable.execution.reason);
    }

    const AVStream* firstVideo = videos.front().session->demuxer().stream(
        videos.front().video_stream_index);
    if (!initializeOutputStream(output, firstVideo, plan.streams.video, error)) {
        return fail(result, error);
    }
    for (const auto& candidate : audios) {
        if (plan.streams.audio == nullptr && !initializeOutputStream(
                output, candidate.session->demuxer().stream(candidate.stream_index),
                plan.streams.audio, error)) return fail(result, error);
    }
    if (finalAudio && plan.streams.audio == nullptr && !initializeOutputStream(
            output, finalAudio->session->demuxer().stream(finalAudio->stream_index),
            plan.streams.audio, error)) return fail(result, error);

    for (const auto& candidate : videos) {
        plan.segments.push_back(PreparedVideoSegment{
            candidate.session, candidate.path, candidate.video_stream_index,
            candidate.audio_stream_index, candidate.source_in_us,
            candidate.source_duration_us, candidate.timeline_offset_us,
            candidate.include_audio, candidate.extend_video_tail});
    }
    plan.expected_duration_us = timeline;
    if (finalAudio) {
        const int64_t available = std::max<int64_t>(0, timeline - request.audio->start_offset_us);
        const int64_t duration = request.audio->duration_us > 0
            ? std::min(request.audio->duration_us, available) : available;
        const int64_t actual = streamDurationUs(
            finalAudio->session->demuxer().raw(),
            finalAudio->session->demuxer().stream(finalAudio->stream_index));
        if (duration <= 0 || (actual > 0 && actual + 50000 < duration)) {
            return fail(result, "copy-only audio is shorter than the video timeline");
        }
        plan.audio = PreparedAudioTrack{
            finalAudio->session, finalAudio->path, finalAudio->stream_index,
            request.audio->start_offset_us, duration};
    }
    return true;
}

bool writeStreamingOutput(UniqueOutputContext& output, const PreparedCopyMuxPlan& plan,
                          const fs::path& partial, const fs::path& target,
                          CopyOnlyMuxResult* result, std::string& error,
                          bool compute_sha256,
                          packet::WriteProgressCallback progressCallback = nullptr) {
    packet::PacketOutputSink sink;
    if ((output->oformat->flags & AVFMT_NOFILE) == 0) {
        sink.setComputeSHA256(compute_sha256);
        if (!sink.open(partial, error)) return fail(result, error);
        if (progressCallback) sink.setWriteProgressCallback(std::move(progressCallback));
        output->pb = sink.avio();
        output->flags |= AVFMT_FLAG_CUSTOM_IO;
    }
    if (avformat_write_header(output.get(), nullptr) < 0) {
        return fail(result, "avformat_write_header failed");
    }
    Writer writer{output.get(), plan.streams};
    std::vector<packet::CursorSegment> videoSegments;
    videoSegments.reserve(plan.segments.size());
    for (const auto& segment : plan.segments) {
        videoSegments.push_back({segment.session, segment.path.string(),
            segment.video_stream_index, plan.streams.video, segment.timeline_offset_us,
            segment.source_in_us, segment.source_duration_us, segment.extend_video_tail});
    }
    packet::TimestampState videoState;
    packet::VideoTimelineCursor video(std::move(videoSegments), videoState);
    std::vector<packet::CursorSegment> audioSegments;
    for (const auto& segment : plan.segments) {
        if (segment.include_audio) {
            audioSegments.push_back({segment.session, segment.path.string(),
                segment.audio_stream_index, plan.streams.audio, segment.timeline_offset_us,
                segment.source_in_us, segment.source_duration_us, false});
        }
    }
    if (plan.audio) {
        audioSegments.push_back({plan.audio->session, plan.audio->path.string(),
            plan.audio->stream_index, plan.streams.audio, 0, plan.audio->start_offset_us,
            plan.audio->duration_us, false});
    }
    packet::TimestampState audioState;
    packet::AudioTimelineCursor audio(std::move(audioSegments), audioState);
    if (!video.prime(error) || !audio.prime(error)) return fail(result, error);
    while (video.hasPacket() || audio.hasPacket()) {
        const bool takeVideo = !audio.hasPacket() ||
            (video.hasPacket() && video.current().sort_dts <= audio.current().sort_dts);
        if (takeVideo) {
            if (!consume(video.current(), &writer, error)) return fail(result, error);
            ++result->video_packets;
            if (!video.advance(error)) return fail(result, error);
        } else {
            if (!consume(audio.current(), &writer, error)) return fail(result, error);
            ++result->audio_packets;
            if (!audio.advance(error)) return fail(result, error);
        }
    }
    const bool needsAudio = plan.audio.has_value() || std::any_of(
        plan.segments.begin(), plan.segments.end(), [](const auto& segment) {
            return segment.include_audio;
        });
    if (result->video_packets == 0) {
        return fail(result, "copy-only packet mux found no video packets in the requested ranges");
    }
    if (needsAudio && result->audio_packets == 0) {
        return fail(result, "copy-only packet mux found no audio packets in the requested ranges");
    }
    if (av_write_trailer(output.get()) < 0) return fail(result, "av_write_trailer failed");
    const auto trailerDone = std::chrono::steady_clock::now();
    packet::PacketOutputSinkResult sinkResult;
    if (!sink.finalize(sinkResult, error)) return fail(result, error);
    result->sha256 = sinkResult.sha256;
    result->sha256_valid = sinkResult.sha256_valid;
    result->output_size_bytes = sinkResult.output_size_bytes;
    result->backward_seek_seen = sinkResult.backward_seek_seen;
    result->backward_seek_count = sinkResult.backward_seek_count;
    result->backward_seek_bytes = sinkResult.backward_seek_bytes;
    result->file_data_synced = sinkResult.file_data_synced;
    result->max_buffered_packets = 1;
    result->packet_heap_allocations = 0;
    result->global_sort_ms = 0;
    output->pb = nullptr;
    output.reset();
    if (!packet::validTimestamp(writer.video_end_us) ||
        writer.video_end_us + 80000 < plan.expected_duration_us) {
        return fail(result, "copy-only packet mux video stream ends before the requested timeline");
    }
    file::DurabilityEvidence evidence;
    evidence.file_data_synced = sinkResult.file_data_synced;
    bool durable = false;
    if (!file::publishAtomic(partial, target, evidence, &error, &durable)) {
        return fail(result, error);
    }
    result->trailer_to_publish_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - trailerDone).count();
    result->output_durable = durable;
    result->success = true;
    result->error.clear();
    return true;
}

} // namespace

bool runCopyOnlyMux(const CopyOnlyMuxRequest& request, CopyOnlyMuxResult* result) {
    CopyOnlyMuxResult local;
    if (result == nullptr) result = &local;
    *result = CopyOnlyMuxResult{};
    if (const auto validation = validateRequest(request); !validation.empty()) {
        return fail(result, validation);
    }
    if (request.audio && request.audio->duration_us > 0) {
        int64_t videoDuration = 0;
        for (const auto& segment : request.video_segments) {
            if (segment.source_duration_us > 0 && videoDuration <=
                std::numeric_limits<int64_t>::max() - segment.source_duration_us) {
                videoDuration += segment.source_duration_us;
            }
        }
        const int64_t available = std::max<int64_t>(
            0, videoDuration - request.audio->start_offset_us);
        if (request.audio->duration_us < available) {
            return fail(result, "copy-only audio is shorter than the video timeline");
        }
    }

    fs::path parent = request.output_path.parent_path();
    std::error_code ec;
    if (parent.empty()) parent = fs::current_path(ec);
    if (ec || parent.empty()) return fail(result, "copy-only packet mux cannot resolve output directory");
    fs::create_directories(parent, ec);
    if (ec) return fail(result, "copy-only packet mux cannot create output directory: " + ec.message());
    const fs::path partial = file::makePartialPath(request.output_path);
    const auto cleanup = [&]() {
        std::error_code cleanupError;
        fs::remove(partial, cleanupError);
    };

    AVFormatContext* raw = nullptr;
    const int allocation = avformat_alloc_output_context2(&raw, nullptr, "mp4", partial.c_str());
    if (allocation < 0 || raw == nullptr) {
        return fail(result, "avformat_alloc_output_context2: " + packet::ffmpegError(allocation));
    }
    UniqueOutputContext output(raw);
    packet::InputSessionRegistry sessions;
    PreparedCopyMuxPlan plan;
    std::string error;
    if (!preparePlan(request, sessions, output.get(), plan, result, error)) {
        cleanup();
        return false;
    }
    result->duration_us = plan.expected_duration_us;
    if (!writeStreamingOutput(output, plan, partial, request.output_path, result, error,
                              request.compute_sha256,
                              request.write_progress_callback)) {
        cleanup();
        return false;
    }
    return true;
}

} // namespace velox::media

#endif // VELOX_ENABLE_LIBAV
