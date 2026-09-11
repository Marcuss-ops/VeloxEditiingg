#ifdef VELOX_ENABLE_LIBAV

#include "media_packet_pipeline_internal.hpp"

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
    packet::InputSessionRegistry* registry{};
    fs::path path;
    int video_stream_index{-1};
    int audio_stream_index{-1};
    int64_t source_in_us{};
    int64_t source_duration_us{};
    int64_t timeline_offset_us{};
    bool include_audio{};
    bool extend_video_tail{};
    bool metadata_certified{};
    bool normalized{};
    bool transform_required{};
    bool legacy_required{};
    MediaSignature target_signature{};
    bool has_target_signature{};
};

struct PreparedAudioTrack {
    packet::InputSession* session{};
    packet::InputSessionRegistry* registry{};
    fs::path path;
    int stream_index{-1};
    int64_t start_offset_us{};
    int64_t duration_us{};
    bool metadata_certified{};
    MediaSignature target_signature{};
    bool has_target_signature{};
};

struct PreparedCopyMuxPlan {
    std::vector<PreparedVideoSegment> segments;
    std::optional<PreparedAudioTrack> audio;
    std::optional<MediaSignature> audio_target_signature;
    OutputStreams streams;
    int64_t expected_duration_us{};
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
    bool first_output_recorded{false};
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
    if (!writer.first_output_recorded) {
        writer.first_output_recorded = true;
        services::recordFirstOutputWrite();
    }
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
    std::optional<MediaSignature> videoTarget = request.target_video_signature;
    std::optional<MediaSignature> audioTarget;
    int64_t timeline = 0;
    if (request.video_segments.empty()) {
        return fail(result, "copy-only packet mux requires at least one video segment");
    }

    const auto& firstSegment = request.video_segments.front();
    if (firstSegment.source_duration_us <= 0 || firstSegment.source_in_us < 0) {
        return fail(result, "copy-only packet mux rejects invalid source video window");
    }
    if (firstSegment.source_in_us >
        std::numeric_limits<int64_t>::max() - firstSegment.source_duration_us) {
        return fail(result, "copy-only source video window overflows int64");
    }
    if (firstSegment.normalized && firstSegment.source_in_us != 0) {
        return fail(result, "copy-only normalized segment must start at source_in_us 0");
    }
    if (firstSegment.transform_required) {
        return fail(result, "segment_execution_rejected: media transform required for " +
            firstSegment.path.string());
    }
    if (firstSegment.legacy_required) {
        return fail(result, "segment_execution_rejected: legacy renderer required for " +
            firstSegment.path.string());
    }
    auto* firstSession = sessions.resolve(firstSegment.path, error);
    if (firstSession == nullptr) return fail(result, error);
    auto& firstDemuxer = firstSession->demuxer();
    const int firstVideoIndex = firstDemuxer.firstStream(AVMEDIA_TYPE_VIDEO);
    if (firstVideoIndex < 0) {
        return fail(result, "video stream missing from " + firstSegment.path.string());
    }
    const AVStream* firstVideo = firstDemuxer.stream(firstVideoIndex);
    const MediaSignature firstVideoSignature = mediaSignatureFromStream(firstVideo);
    if (!videoTarget) videoTarget = firstVideoSignature;
    std::string compatibilityReason;
    if (!mediaSignaturesCompatible(firstVideoSignature, *videoTarget, &compatibilityReason)) {
        return fail(result, "segment_execution_rejected: copy-only segment execution rejected at " +
            firstSegment.path.string() + ": " + compatibilityReason);
    }
    const bool firstKeyframeSafe = firstSegment.normalized ||
        firstSession->sourceWindowStartsOnKeyframe(
            firstVideoIndex, firstSegment.source_in_us, error);
    if (!firstKeyframeSafe) {
        return fail(result, "segment_execution_rejected: source window is not keyframe-safe for packet copy: " + error);
    }
    if (!initializeOutputStream(output, firstVideo, plan.streams.video, error)) {
        return fail(result, error);
    }

    int firstAudioIndex = -1;
    packet::InputSession* firstAudioSession = nullptr;
    if (firstSegment.include_audio) {
        firstAudioSession = firstSession;
        firstAudioIndex = firstDemuxer.firstStream(AVMEDIA_TYPE_AUDIO);
    } else {
        for (std::size_t index = 1; index < request.video_segments.size(); ++index) {
            if (!request.video_segments[index].include_audio) continue;
            firstAudioSession = sessions.resolve(request.video_segments[index].path, error);
            if (firstAudioSession != nullptr) {
                firstAudioIndex = firstAudioSession->demuxer().firstStream(AVMEDIA_TYPE_AUDIO);
            }
            break;
        }
    }
    if (firstAudioSession != nullptr) {
        if (firstAudioIndex < 0) {
            return fail(result, "copy-only segment requests audio but the source has no audio stream");
        }
        audioTarget = mediaSignatureFromStream(
            firstAudioSession->demuxer().stream(firstAudioIndex));
        plan.audio_target_signature = audioTarget;
        if (!initializeOutputStream(output,
                firstAudioSession->demuxer().stream(firstAudioIndex),
                plan.streams.audio, error)) return fail(result, error);
    }

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
        const bool first = index == 0;
        int videoIndex = first ? firstVideoIndex : -1;
        int audioIndex = first && segment.include_audio ? firstAudioIndex : -1;
        packet::InputSession* session = first ? firstSession : nullptr;
        const int64_t sourceDuration = first
            ? streamDurationUs(firstDemuxer.raw(), firstVideo)
            : 0;
        const bool extendTail = !segment.normalized &&
            (first ? sourceDuration > 0 && sourceDuration + 50000 <
                segment.source_in_us + segment.source_duration_us : true);
        plan.segments.push_back(PreparedVideoSegment{
            session, &sessions, segment.path, videoIndex, audioIndex,
            segment.source_in_us, segment.source_duration_us, timeline,
            segment.include_audio, extendTail, segment.metadata_certified,
            segment.normalized, segment.transform_required, segment.legacy_required,
            *videoTarget, true});
        if (timeline > std::numeric_limits<int64_t>::max() - segment.source_duration_us) {
            return fail(result, "copy-only packet mux timeline overflows int64");
        }
        timeline += segment.source_duration_us;
    }

    if (request.audio) {
        const auto& audio = *request.audio;
        auto* session = sessions.resolve(audio.path, error);
        if (session == nullptr) return fail(result, error);
        auto& demuxer = session->demuxer();
        const int streamIndex = demuxer.firstStream(AVMEDIA_TYPE_AUDIO);
        if (streamIndex < 0) {
            FinalAudioDecision decision;
            decision.reason = "audio_metadata_unverified";
            if (result != nullptr) result->final_audio_decision = decision;
            return fail(result, "copy_only final audio is not FINAL_AUDIO_COPY: " +
                decision.reason + " " + describeFinalAudioProbe(audio.path, decision.metadata));
        }
        const FinalAudioMetadata metadata = session->finalAudioMetadata(streamIndex);
        const FinalAudioDecision decision = resolveFinalAudioModePacket(
            metadata, true, static_cast<double>(timeline) / 1'000'000.0);
        if (result != nullptr) result->final_audio_decision = decision;
        if (decision.mode != FinalAudioMode::Copy) {
            return fail(result, "copy_only final audio is not FINAL_AUDIO_COPY: " +
                decision.reason + " " + describeFinalAudioProbe(audio.path, metadata));
        }
        const MediaSignature signature = mediaSignatureFromStream(demuxer.stream(streamIndex));
        if (audioTarget && !mediaSignaturesCompatible(signature, *audioTarget, &compatibilityReason)) {
            return fail(result, "segment_execution_rejected: copy-only final audio rejected: " + compatibilityReason);
        }
        if (!audioTarget) audioTarget = signature;
        plan.audio_target_signature = audioTarget;
        if (plan.streams.audio == nullptr && !initializeOutputStream(
                output, demuxer.stream(streamIndex), plan.streams.audio, error)) {
            return fail(result, error);
        }
        const int64_t available = std::max<int64_t>(0, timeline - request.audio->start_offset_us);
        const int64_t duration = request.audio->duration_us > 0
            ? std::min(request.audio->duration_us, available) : available;
        const int64_t actual = streamDurationUs(
            session->demuxer().raw(), session->demuxer().stream(streamIndex));
        if (duration <= 0 || (actual > 0 && actual + 50000 < duration)) {
            return fail(result, "copy-only audio is shorter than the video timeline");
        }
        plan.audio = PreparedAudioTrack{
            session, &sessions, audio.path, streamIndex,
            request.audio->start_offset_us, duration, audio.metadata_certified,
            *audioTarget, true};
    }
    if (plan.audio_target_signature == std::nullopt && plan.streams.audio != nullptr) {
        return fail(result, "copy-only packet mux could not establish an audio target signature");
    }
    plan.expected_duration_us = timeline;
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
            segment.source_in_us, segment.source_duration_us, segment.extend_video_tail,
            segment.registry, AVMEDIA_TYPE_VIDEO, segment.metadata_certified,
            segment.normalized, segment.transform_required, segment.legacy_required,
            segment.target_signature, segment.has_target_signature,
            segment.session != nullptr});
    }
    packet::TimestampState videoState;
    packet::VideoTimelineCursor video(std::move(videoSegments), videoState);
    std::vector<packet::CursorSegment> audioSegments;
    for (const auto& segment : plan.segments) {
        if (segment.include_audio) {
            audioSegments.push_back({segment.session, segment.path.string(),
                segment.audio_stream_index, plan.streams.audio, segment.timeline_offset_us,
                segment.source_in_us, segment.source_duration_us, false,
                segment.registry, AVMEDIA_TYPE_AUDIO, segment.metadata_certified,
                false, false, false,
                plan.audio_target_signature.value_or(MediaSignature{}),
                plan.audio_target_signature.has_value(), segment.session != nullptr});
        }
    }
    if (plan.audio) {
        audioSegments.push_back({plan.audio->session, plan.audio->path.string(),
            plan.audio->stream_index, plan.streams.audio, 0, plan.audio->start_offset_us,
            plan.audio->duration_us, false, plan.audio->registry, AVMEDIA_TYPE_AUDIO,
            plan.audio->metadata_certified, false, false, false,
            plan.audio->target_signature, plan.audio->has_target_signature, true});
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
