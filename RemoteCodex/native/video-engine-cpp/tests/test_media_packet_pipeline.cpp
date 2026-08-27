#include "velox/services/file_utils.hpp"
#include "velox/services/media_packet_pipeline.hpp"
#include "velox/services/media_probe.hpp"

extern "C" {
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
}

#include <chrono>
#include <cmath>
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>

namespace fs = std::filesystem;

namespace {

int failures = 0;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << "\n";
        ++failures;
    }
}

std::string uniqueStem() {
    return "velox_packet_pipeline_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

bool makeBFrameVideo(const fs::path& output) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote("testsrc=size=64x64:rate=25:duration=2")
            << " -an -c:v libx264 -preset ultrafast -pix_fmt yuv420p -bf 2 -g 25 "
            << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

bool makeVideo(const fs::path& output, const std::string& size) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote(
                "testsrc=size=" + size + ":rate=5:duration=1.2")
            << " -an -c:v libx264 -preset ultrafast -pix_fmt yuv420p -r 5 "
            << " -g 2 -keyint_min 2 -sc_threshold 0 "
            << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

bool makeAudio(const fs::path& output) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i "
            << velox::file::shellQuote("sine=frequency=440:sample_rate=48000")
            << " -t 2.0 -c:a aac -ar 48000 -ac 2 "
            << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

bool makeMuxedVideo(const fs::path& video, const fs::path& audio, const fs::path& output) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -i " << velox::file::shellQuote(video.string())
            << " -i " << velox::file::shellQuote(audio.string())
            << " -map 0:v:0 -map 1:a:0 -c:v copy -c:a copy -shortest "
            << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

struct PacketSummary {
    int video_packets{0};
    int audio_packets{0};
    bool video_dts_monotonic{true};
    bool audio_dts_monotonic{true};
    bool video_pts_monotonic{true};
    bool audio_pts_monotonic{true};
    int64_t first_audio_dts{AV_NOPTS_VALUE};
};

PacketSummary inspectPackets(const fs::path& path) {
    PacketSummary summary;
    AVFormatContext* context = nullptr;
    if (avformat_open_input(&context, path.c_str(), nullptr, nullptr) < 0 || context == nullptr) {
        expect(false, "output can be opened for packet inspection");
        if (context != nullptr) avformat_close_input(&context);
        return summary;
    }
    if (avformat_find_stream_info(context, nullptr) < 0) {
        expect(false, "output stream info can be read");
        avformat_close_input(&context);
        return summary;
    }

    int64_t lastVideoDts = AV_NOPTS_VALUE;
    int64_t lastAudioDts = AV_NOPTS_VALUE;
    int64_t lastVideoPts = AV_NOPTS_VALUE;
    int64_t lastAudioPts = AV_NOPTS_VALUE;
    AVPacket* packet = av_packet_alloc();
    while (packet != nullptr && av_read_frame(context, packet) >= 0) {
        if (packet->stream_index < 0 ||
            static_cast<unsigned int>(packet->stream_index) >= context->nb_streams) {
            av_packet_unref(packet);
            continue;
        }
        const auto* stream = context->streams[packet->stream_index];
        if (stream->codecpar->codec_type == AVMEDIA_TYPE_VIDEO) {
            ++summary.video_packets;
            if (packet->dts != AV_NOPTS_VALUE) {
                if (lastVideoDts != AV_NOPTS_VALUE && packet->dts <= lastVideoDts) {
                    summary.video_dts_monotonic = false;
                }
                lastVideoDts = packet->dts;
            }
            if (packet->pts != AV_NOPTS_VALUE) {
                if (lastVideoPts != AV_NOPTS_VALUE && packet->pts <= lastVideoPts) {
                    summary.video_pts_monotonic = false;
                }
                lastVideoPts = packet->pts;
            }
        } else if (stream->codecpar->codec_type == AVMEDIA_TYPE_AUDIO) {
            ++summary.audio_packets;
            if (packet->dts != AV_NOPTS_VALUE) {
                if (summary.first_audio_dts == AV_NOPTS_VALUE) {
                    summary.first_audio_dts = packet->dts;
                }
                if (lastAudioDts != AV_NOPTS_VALUE && packet->dts <= lastAudioDts) {
                    summary.audio_dts_monotonic = false;
                }
                lastAudioDts = packet->dts;
            }
            if (packet->pts != AV_NOPTS_VALUE) {
                if (lastAudioPts != AV_NOPTS_VALUE && packet->pts <= lastAudioPts) {
                    summary.audio_pts_monotonic = false;
                }
                lastAudioPts = packet->pts;
            }
        }
        av_packet_unref(packet);
    }
    av_packet_free(&packet);
    avformat_close_input(&context);
    return summary;
}

} // namespace

int main() {
    const fs::path root = fs::temp_directory_path() / uniqueStem();
    std::error_code ec;
    fs::create_directories(root, ec);
    expect(!ec, "temporary directory can be created");
    if (ec) return 1;

    struct Cleanup {
        fs::path root;
        ~Cleanup() {
            std::error_code ec;
            fs::remove_all(root, ec);
        }
    } cleanup{root};

    const fs::path video = root / "video.mp4";
    const fs::path incompatible = root / "incompatible.mp4";
    const fs::path normalized = root / "normalized.mp4";
    const fs::path audio = root / "audio.m4a";
    const fs::path videoWithAudio = root / "video-with-audio.mp4";
    const fs::path bframeVideo = root / "bframes.mp4";
    const fs::path manyOutput = root / "many-segments-output.mp4";
    const fs::path output = root / "packet-output.mp4";
    const fs::path failedOutput = root / "failed-output.mp4";
    expect(makeVideo(video, "64x64"), "video fixture can be created");
    expect(makeVideo(incompatible, "32x32"), "incompatible video fixture can be created");
    expect(makeVideo(normalized, "64x64"), "normalized fixture can be created");
    expect(makeAudio(audio), "audio fixture can be created");
    expect(makeMuxedVideo(video, audio, videoWithAudio),
           "video-with-audio fixture can be created");
    expect(makeBFrameVideo(bframeVideo), "B-frame fixture can be created");

    // The worker's verified cache contract can arrive either as the source
    // path (the normal resolver output) or as cache_key pointing directly at
    // the immutable file. Both forms must be handed to libavformat as-is.
    const fs::path cacheFile = root / "worker-cache" / "clip.mp4";
    fs::create_directories(cacheFile.parent_path(), ec);
    expect(!ec, "worker cache directory can be created");
    fs::copy_file(video, cacheFile, fs::copy_options::overwrite_existing, ec);
    expect(!ec, "worker cache fixture can be created");
    const auto sourceBound = velox::file::resolveLocalAssetPath(cacheFile.string(), {});
    expect(sourceBound == cacheFile,
           "resolved local source path is bound without staging");
    const auto cacheKeyBound = velox::file::resolveLocalAssetPath(
        "velox-asset://clip-1", cacheFile);
    expect(cacheKeyBound == cacheFile,
           "direct cache_key path is bound without staging");

    const fs::path atomicTarget = root / "atomic-target.mp4";
    const fs::path atomicPartial = velox::file::makePartialPath(atomicTarget);
    expect(velox::file::writeFile(atomicTarget, "old-output"),
           "existing output can be created for atomic replacement");
    expect(velox::file::writeFile(atomicPartial, "new-output"),
           "unique output partial can be written");
    std::string publishError;
    bool durable = false;
    expect(velox::file::publishAtomic(atomicPartial, atomicTarget, &publishError, &durable),
           "partial output is fsynced and atomically renamed");
    expect(durable, "atomic publication confirms parent-directory durability");
    expect(velox::file::readFile(atomicTarget) == "new-output",
           "atomic rename replaces the old output without a copy");
    expect(!fs::exists(atomicPartial),
           "successful atomic publication consumes the partial path");
    if (failures != 0) return 1;

    const fs::path emptyBin = root / "empty-bin";
    fs::create_directory(emptyBin, ec);
    expect(!ec, "sentinel PATH directory can be created");
    const fs::path ffmpegSentinel = root / "ffmpeg-invoked";
    const fs::path ffprobeSentinel = root / "ffprobe-invoked";
    expect(velox::file::writeFile(
        emptyBin / "ffmpeg",
        "#!/bin/sh\ntouch " + velox::file::shellQuote(ffmpegSentinel.string()) + "\nexit 1\n"),
        "ffmpeg sentinel can be written");
    expect(velox::file::writeFile(
        emptyBin / "ffprobe",
        "#!/bin/sh\ntouch " + velox::file::shellQuote(ffprobeSentinel.string()) + "\nexit 1\n"),
        "ffprobe sentinel can be written");
    fs::permissions(emptyBin / "ffmpeg",
                    fs::perms::owner_read | fs::perms::owner_write | fs::perms::owner_exec,
                    fs::perm_options::replace, ec);
    fs::permissions(emptyBin / "ffprobe",
                    fs::perms::owner_read | fs::perms::owner_write | fs::perms::owner_exec,
                    fs::perm_options::replace, ec);

    const char* previousPath = std::getenv("PATH");
    const bool hadPath = previousPath != nullptr;
    const std::string previousPathValue = hadPath ? previousPath : "";
    setenv("PATH", emptyBin.c_str(), 1);

    velox::media::CopyOnlyMuxRequest request;
    request.video_segments = {
        {video, 0, 800'000},
        {video, 0, 800'000},
    };
    request.audio = velox::media::CopyOnlyAudioTrack{audio, 0, 1'600'000};
    request.output_path = output;
    velox::media::CopyOnlyMuxResult muxResult;
    expect(velox::media::MediaPacketPipeline::muxCopyOnly(request, &muxResult),
           "MediaPacketPipeline facade concatenates video and muxes audio in-process");
    expect(muxResult.success, "successful packet mux reports success");
    expect(muxResult.output_durable, "packet mux reports durable atomic publication");
    expect(muxResult.video_packets > 0, "packet mux writes video packets");
    expect(muxResult.audio_packets > 0, "packet mux writes audio packets");
    expect(muxResult.max_buffered_packets >= 1 && muxResult.max_buffered_packets <= 2,
           "canonical mux reports bounded packet buffering");
    expect(muxResult.packet_heap_allocations == 0,
           "canonical mux reports no per-packet PacketHolder allocations");
    expect(muxResult.global_sort_ms == 0,
           "canonical mux reports no global packet sort");
    expect(fs::exists(output), "packet mux publishes the output atomically");
    expect(!fs::exists(fs::path(output.string() + ".partial")),
           "packet mux does not leave a fixed partial output");
    expect(!fs::exists(ffmpegSentinel), "packet pipeline never executes ffmpeg");
    expect(!fs::exists(ffprobeSentinel), "packet pipeline never executes ffprobe");

    const auto outputProbe = velox::media::probeMediaInProcess(output);
    expect(outputProbe.has_value(), "packet output can be probed in-process");
    if (outputProbe.has_value()) {
        expect(outputProbe->duration_verified, "packet output duration is verified");
        expect(outputProbe->duration_seconds > 1.4 && outputProbe->duration_seconds < 1.8,
               "packet output duration follows the two 0.8 second segments (actual=" +
                   std::to_string(outputProbe->duration_seconds) + ")");
        expect(outputProbe->streams.size() == 2, "packet output has exactly video and audio streams");
    }
    const auto packets = inspectPackets(output);
    expect(packets.video_packets > 0 && packets.audio_packets > 0,
           "packet output contains both media packet types");
    expect(packets.video_dts_monotonic && packets.video_pts_monotonic,
           "rewritten video timestamps are strictly monotonic");
    expect(packets.audio_dts_monotonic && packets.audio_pts_monotonic,
           "rewritten audio timestamps are strictly monotonic");

    // Determinism: identical requests must produce identical packet counts,
    // duration and monotonicity regardless of repeated session reuse.
    const fs::path deterministicOutput = root / "deterministic-output.mp4";
    auto deterministicRequest = request;
    deterministicRequest.output_path = deterministicOutput;
    velox::media::CopyOnlyMuxResult deterministicResult;
    expect(velox::media::muxCopyOnly(deterministicRequest, &deterministicResult),
           "repeated bounded mux request succeeds deterministically");
    const auto deterministicPackets = inspectPackets(deterministicOutput);
    expect(deterministicResult.max_buffered_packets == muxResult.max_buffered_packets &&
               deterministicResult.packet_heap_allocations == muxResult.packet_heap_allocations &&
               deterministicResult.global_sort_ms == muxResult.global_sort_ms &&
               deterministicResult.video_packets == muxResult.video_packets &&
               deterministicResult.audio_packets == muxResult.audio_packets &&
               deterministicResult.duration_us == muxResult.duration_us &&
               deterministicPackets.video_packets == packets.video_packets &&
               deterministicPackets.audio_packets == packets.audio_packets,
           "repeated mux produces identical packet counts and duration");

    // B-frame input must remain decodable and preserve the requested timeline.
    const fs::path bframeOutput = root / "bframe-output.mp4";
    velox::media::CopyOnlyMuxRequest bframeRequest;
    bframeRequest.video_segments = {{bframeVideo, 0, 1'600'000}};
    bframeRequest.output_path = bframeOutput;
    velox::media::CopyOnlyMuxResult bframeResult;
    expect(velox::media::muxCopyOnly(bframeRequest, &bframeResult),
           "bounded mux accepts B-frame video");
    const auto bframePackets = inspectPackets(bframeOutput);    expect(bframePackets.video_packets > 0 && bframePackets.video_dts_monotonic &&
           bframePackets.video_pts_monotonic,
           "B-frame output has monotonic timestamps");
    expect(bframeResult.video_packets == bframePackets.video_packets,
           "B-frame result packet count matches inspected output");

    // Bounded-mux stress case: many repeated segments must not change the
    // public contract and must produce deterministic packet accounting.
    velox::media::CopyOnlyMuxRequest manyRequest;
    manyRequest.video_segments.reserve(1000);
    for (int i = 0; i < 1000; ++i) {
        manyRequest.video_segments.push_back({video, 0, 800'000});
    }
    manyRequest.output_path = manyOutput;
    velox::media::CopyOnlyMuxResult manyResult;
    expect(velox::media::muxCopyOnly(manyRequest, &manyResult),
           "bounded mux accepts 1000 repeated segments");
    expect(manyResult.video_packets > 0 && manyResult.duration_us == 1000 * 800'000,
           "1000-segment mux reports the complete logical timeline");
    const auto manyPackets = inspectPackets(manyOutput);
    expect(manyPackets.video_packets == manyResult.video_packets &&
               manyPackets.video_dts_monotonic && manyPackets.video_pts_monotonic,
           "many-segment output has deterministic monotonic video packets");

    // A second 1000-segment run must preserve packet accounting and duration.
    const fs::path manyRepeatOutput = root / "many-segments-repeat-output.mp4";
    auto manyRepeatRequest = manyRequest;
    manyRepeatRequest.output_path = manyRepeatOutput;
    velox::media::CopyOnlyMuxResult manyRepeatResult;
    expect(velox::media::muxCopyOnly(manyRepeatRequest, &manyRepeatResult),
           "bounded mux repeats the 1000-segment request");
    const auto manyRepeatPackets = inspectPackets(manyRepeatOutput);
    expect(manyRepeatResult.video_packets == manyResult.video_packets &&
               manyRepeatResult.duration_us == manyResult.duration_us &&
               manyRepeatPackets.video_packets == manyPackets.video_packets &&
               manyRepeatPackets.video_dts_monotonic && manyRepeatPackets.video_pts_monotonic,
           "1000-segment repeated run is deterministic");


    const fs::path segmentAudioOutput = root / "segment-audio-output.mp4";
    velox::media::CopyOnlyMuxRequest segmentAudioRequest;
    segmentAudioRequest.video_segments = {
        {videoWithAudio, 0, 800'000, true},
        {videoWithAudio, 0, 800'000, true},
    };
    segmentAudioRequest.output_path = segmentAudioOutput;
    velox::media::CopyOnlyMuxResult segmentAudioResult;
    expect(velox::media::muxCopyOnly(segmentAudioRequest, &segmentAudioResult),
           "copy-only preserves compatible segment audio through packet mapping");
    const auto segmentAudioProbe = velox::media::probeMediaInProcess(segmentAudioOutput);
    expect(segmentAudioProbe.has_value() && segmentAudioProbe->streams.size() == 2,
           "segment audio packet mapping produces video and audio streams");

    const fs::path tailExtensionOutput = root / "tail-extension-output.mp4";
    velox::media::CopyOnlyMuxRequest tailExtensionRequest;
    tailExtensionRequest.video_segments = {{video, 0, 1'800'000}};
    tailExtensionRequest.output_path = tailExtensionOutput;
    velox::media::CopyOnlyMuxResult tailExtensionResult;
    expect(velox::media::muxCopyOnly(tailExtensionRequest, &tailExtensionResult),
           "copy-only freezes the last encoded video packet when the source is short");
    expect(tailExtensionResult.success && tailExtensionResult.video_packets > 6,
           "tail extension appends decode-free video packets");
    const auto tailExtensionProbe = velox::media::probeMediaInProcess(tailExtensionOutput);
    expect(tailExtensionProbe.has_value() && tailExtensionProbe->duration_verified &&
               tailExtensionProbe->duration_seconds > 1.55 &&
               tailExtensionProbe->duration_seconds < 1.95,
           "tail extension covers the requested duration (actual=" +
               (tailExtensionProbe.has_value()
                    ? std::to_string(tailExtensionProbe->duration_seconds)
                    : std::string("none")) +
               ")");

    velox::media::CopyOnlyMuxRequest incompatibleRequest;
    incompatibleRequest.video_segments = {{video, 0, 800'000}, {incompatible, 0, 800'000}};
    incompatibleRequest.output_path = failedOutput;
    velox::media::CopyOnlyMuxResult failureResult;
    expect(!velox::media::muxCopyOnly(incompatibleRequest, &failureResult),
           "incompatible stream parameters fail closed");
    expect(!failureResult.success && !failureResult.error.empty(),
           "incompatible stream failure includes an error");
    expect(failureResult.error.find("media signature mismatch") != std::string::npos,
           "incompatible stream failure comes from the canonical execution resolver");
    expect(!fs::exists(failedOutput), "failed packet mux does not publish output");

    // Mixed copy + normalized assembly: the packet mux is the final
    // assembler for a mixed render. A raw copy range and an independent
    // already-normalized transcode output (same canonical profile) are
    // concatenated through one mux, then a trailing raw copy range closes
    // the timeline.
    const fs::path mixedOutput = root / "mixed-output.mp4";
    velox::media::CopyOnlyMuxRequest mixedRequest;
    mixedRequest.video_segments = {
        {video, 0, 800'000, false, false},      // raw copy range
        {normalized, 0, 800'000, false, true},  // normalized transcode output
        {video, 0, 400'000, false, false},      // trailing raw copy range
    };
    mixedRequest.output_path = mixedOutput;
    velox::media::CopyOnlyMuxResult mixedResult;
    expect(velox::media::muxCopyOnly(mixedRequest, &mixedResult),
           "mixed packet mux concatenates raw copy and normalized segments");
    expect(mixedResult.success, "mixed packet mux reports success");
    const auto mixedProbe = velox::media::probeMediaInProcess(mixedOutput);
    expect(mixedProbe.has_value() && mixedProbe->duration_verified &&
               mixedProbe->duration_seconds > 1.8 && mixedProbe->duration_seconds < 2.2,
           "mixed mux output covers the copy + normalized + copy timeline (actual=" +
               (mixedProbe.has_value() ? std::to_string(mixedProbe->duration_seconds)
                                       : std::string("none")) +
               ")");

    // Fail-closed: a normalized segment whose profile does not match the
    // canonical stream must not be concatenated.
    const fs::path mixedFailOutput = root / "mixed-fail-output.mp4";
    velox::media::CopyOnlyMuxRequest mixedFailRequest;
    mixedFailRequest.video_segments = {
        {video, 0, 800'000, false, false},
        {incompatible, 0, 800'000, false, true},  // normalized but incompatible
    };
    mixedFailRequest.output_path = mixedFailOutput;
    velox::media::CopyOnlyMuxResult mixedFailResult;
    expect(!velox::media::muxCopyOnly(mixedFailRequest, &mixedFailResult),
           "incompatible normalized segment fails closed in a mixed mux");
    expect(mixedFailResult.error.find("media signature mismatch") != std::string::npos,
           "mixed normalized incompatibility comes from the canonical resolver");
    expect(!fs::exists(mixedFailOutput),
           "mixed incompatible normalized segment does not publish output");

    // A normalized segment starts at its own frame zero; a non-zero source_in
    // would silently trim the encoded segment and must be rejected.
    const fs::path normalizedOffsetOutput = root / "normalized-offset-output.mp4";
    velox::media::CopyOnlyMuxRequest normalizedOffsetRequest;
    normalizedOffsetRequest.video_segments = {
        {normalized, 100'000, 400'000, false, true}};
    normalizedOffsetRequest.output_path = normalizedOffsetOutput;
    velox::media::CopyOnlyMuxResult normalizedOffsetResult;
    expect(!velox::media::muxCopyOnly(normalizedOffsetRequest, &normalizedOffsetResult),
           "normalized segment with non-zero source_in fails closed");
    expect(normalizedOffsetResult.error.find("source_in_us 0") != std::string::npos,
           "normalized offset rejection explains the source_in_us 0 contract");
    expect(!fs::exists(normalizedOffsetOutput),
           "normalized offset rejection does not publish output");

    const fs::path shortAudioOutput = root / "short-audio-output.mp4";
    auto shortAudioRequest = request;
    shortAudioRequest.audio = velox::media::CopyOnlyAudioTrack{audio, 0, 400'000};
    shortAudioRequest.output_path = shortAudioOutput;
    velox::media::CopyOnlyMuxResult shortAudioResult;
    expect(!velox::media::muxCopyOnly(shortAudioRequest, &shortAudioResult),
           "audio shorter than the requested video timeline fails closed");
    expect(!fs::exists(shortAudioOutput),
           "short-audio failure does not publish a partial output");

    // Exact timebase coverage: the generated fixture uses a non-default
    // stream timebase and the output must remain decodable with monotonic DTS.
    expect(packets.video_dts_monotonic && packets.audio_dts_monotonic,
           "different input timebases remain monotonic after rescaling");

    const fs::path trimmedOutput = root / "non-zero-source-output.mp4";
    velox::media::CopyOnlyMuxRequest trimmedRequest;
    trimmedRequest.video_segments = {{video, 400'000, 400'000}};
    trimmedRequest.output_path = trimmedOutput;
    velox::media::CopyOnlyMuxResult trimmedResult;
    expect(velox::media::muxCopyOnly(trimmedRequest, &trimmedResult),
           "copy-only accepts an exact non-zero keyframe source window");
    const auto trimmedProbe = velox::media::probeMediaInProcess(trimmedOutput);
    expect(trimmedProbe.has_value() && trimmedProbe->duration_verified &&
               trimmedProbe->duration_seconds > 0.25 && trimmedProbe->duration_seconds < 0.55,
           "non-zero source window publishes only the requested duration");

    const fs::path nonKeyframeOutput = root / "non-keyframe-source-output.mp4";
    velox::media::CopyOnlyMuxRequest nonKeyframeRequest;
    nonKeyframeRequest.video_segments = {{video, 200'000, 400'000}};
    nonKeyframeRequest.output_path = nonKeyframeOutput;
    velox::media::CopyOnlyMuxResult nonKeyframeResult;
    expect(!velox::media::muxCopyOnly(nonKeyframeRequest, &nonKeyframeResult),
           "copy-only rejects a source window that does not start on a keyframe");
    expect(nonKeyframeResult.error.find("keyframe") != std::string::npos,
           "non-keyframe rejection explains the correctness requirement");
    expect(!fs::exists(nonKeyframeOutput),
           "non-keyframe rejection does not publish an output");

    if (hadPath) {
        setenv("PATH", previousPathValue.c_str(), 1);
    } else {
        unsetenv("PATH");
    }

    std::cerr << "summary: fail=" << failures << "\n";
    return failures == 0 ? 0 : 1;
}
