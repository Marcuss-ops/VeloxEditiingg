// test_streaming_packet_mux.cpp — certified bounded-mux test suite for the
// canonical packet-copy path (VideoTimelineCursor / AudioTimelineCursor /
// single interleaved writer) driven through the stable public boundary
// MediaPacketPipeline::muxCopyOnly.
//
// The streaming mux replaced the legacy collect-all-then-sort path. These
// tests pin the public contract AND the O(1)-memory invariants exposed on
// CopyOnlyMuxResult:
//
//   invariants:
//     max_buffered_packets <= 4      two reusable PendingPacket slots
//     packet_heap_allocations == 0   no per-packet new/delete/make_unique
//     global_sort_ms == 0            no global stable_sort pass
//     video_packets == inspected     mux accounting matches the output
//     audio_packets == inspected
//     PTS/DTS strictly monotonic     per stream, after timebase rescale
//     |probe duration - requested| <= 80ms   logical timeline preserved
//     output decodable               in-process probe, duration verified
//     sha consistency                backward_seek_seen <=> !sha256_valid
//     published size == sink size    no silent truncation on rename
//
//   coverage: video only, repeated source clip, video + source audio,
//   video + final voiceover, B-frames, different input timebases
//   (non-default track timescale, coexisting audio/video timebases,
//   incompatible video timebases fail closed), keyframe cuts,
//   non-keyframe reject, tail extension, 20+ segments, 1000+ segments,
//   determinism across repeated runs.

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
#include <optional>
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
    return "velox_streaming_mux_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

// 5 fps / GOP 2 -> a keyframe every 2 frames (400ms), sc_threshold 0 forces
// exact keyframe placement. `extraMuxerArgs` lets a scenario pin a non-default
// container track timescale on an otherwise identical stream.
bool makeVideo(const fs::path& output, const std::string& size, int fps,
               const std::string& extraMuxerArgs = "") {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote(
                "testsrc=size=" + size + ":rate=" + std::to_string(fps) + ":duration=1.2")
            << " -an -c:v libx264 -preset ultrafast -pix_fmt yuv420p -r " << fps
            << " -g 2 -keyint_min 2 -sc_threshold 0";
    if (!extraMuxerArgs.empty()) {
        command << " " << extraMuxerArgs;
    }
    command << " " << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

bool makeBFrameVideo(const fs::path& output) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote("testsrc=size=64x64:rate=25:duration=2")
            << " -an -c:v libx264 -preset ultrafast -pix_fmt yuv420p -bf 2 -g 25 "
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

struct StreamSummary {
    int video_packets{0};
    int audio_packets{0};
    bool video_dts_monotonic{true};
    bool audio_dts_monotonic{true};
    bool video_pts_monotonic{true};
    bool audio_pts_monotonic{true};
};

StreamSummary inspectStreams(const fs::path& path) {
    StreamSummary summary;
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

// Reads back the first `type` stream time base exactly the way the mux's
// InputSession does (open + find_stream_info). Used to prove the fixtures
// really exercise different input timebases.
AVRational streamTimeBase(const fs::path& path, AVMediaType type) {
    AVRational tb{-1, 1};
    AVFormatContext* context = nullptr;
    if (avformat_open_input(&context, path.c_str(), nullptr, nullptr) < 0 || context == nullptr) {
        if (context != nullptr) avformat_close_input(&context);
        return tb;
    }
    avformat_find_stream_info(context, nullptr);
    for (unsigned int i = 0; i < context->nb_streams; ++i) {
        if (context->streams[i]->codecpar->codec_type == type) {
            tb = context->streams[i]->time_base;
            break;
        }
    }
    avformat_close_input(&context);
    return tb;
}

bool sameRational(AVRational a, AVRational b) {
    return a.num == b.num && a.den == b.den;
}

std::string rationalString(AVRational r) {
    return std::to_string(r.num) + "/" + std::to_string(r.den);
}

// Publication + bounded-memory + SHA-consistency invariants for every
// successful streaming mux.
void checkOutput(const fs::path& output,
                 const velox::media::CopyOnlyMuxResult& result,
                 const std::string& label) {
    expect(result.success, label + ": mux succeeds");
    expect(result.error.empty(), label + ": success clears the error field");
    expect(result.output_durable, label + ": mux reports durable atomic publication");
    expect(fs::exists(output), label + ": mux publishes the output atomically");
    expect(!fs::exists(fs::path(output.string() + ".partial")),
           label + ": mux does not leave a partial output behind");
    expect(result.output_size_bytes > 0, label + ": mux reports a non-empty output");
    expect(fs::file_size(output) == result.output_size_bytes,
           label + ": published file size matches the sink size");
    expect(result.max_buffered_packets >= 1 && result.max_buffered_packets <= 4,
           label + ": bounded mux buffers at most 4 packets (actual=" +
               std::to_string(result.max_buffered_packets) + ")");
    expect(result.packet_heap_allocations == 0,
           label + ": bounded mux allocates no per-packet heap objects");
    expect(result.global_sort_ms == 0,
           label + ": bounded mux performs no global packet sort");
    expect(!result.backward_seek_seen || !result.sha256_valid,
           label + ": backward seek and valid incremental SHA are mutually exclusive");
    if (result.backward_seek_seen) {
        expect(result.sha256.empty(),
               label + ": backward seek disables the opportunistic SHA");
    } else if (result.sha256_valid) {
        expect(!result.sha256.empty(),
               label + ": valid opportunistic SHA is non-empty");
    }
}

// Result accounting vs the decoded output + per-stream timestamp monotonicity.
void checkStreams(const StreamSummary& inspected,
                  const velox::media::CopyOnlyMuxResult& result,
                  const std::string& label) {
    expect(inspected.video_packets == result.video_packets,
           label + ": inspected video packets match mux accounting (" +
               std::to_string(inspected.video_packets) + " vs " +
               std::to_string(result.video_packets) + ")");
    expect(inspected.audio_packets == result.audio_packets,
           label + ": inspected audio packets match mux accounting (" +
               std::to_string(inspected.audio_packets) + " vs " +
               std::to_string(result.audio_packets) + ")");
    expect(inspected.video_dts_monotonic && inspected.video_pts_monotonic,
           label + ": video timestamps are strictly monotonic after rescale");
    expect(inspected.audio_dts_monotonic && inspected.audio_pts_monotonic,
           label + ": audio timestamps are strictly monotonic after rescale");
}

// Logical timeline: result.duration_us is the exact requested sum and the
// probed output stays within tolerance_us of it (default 80ms).
void expectTimeline(const velox::media::CopyOnlyMuxResult& result,
                    const std::optional<velox::media::MediaProbeResult>& probe,
                    int64_t expected_us,
                    const std::string& label,
                    int64_t tolerance_us = 80'000) {
    expect(result.duration_us == expected_us,
           label + ": mux reports the exact requested timeline (" +
               std::to_string(result.duration_us) + " vs " + std::to_string(expected_us) + ")");
    expect(probe.has_value() && probe->duration_verified,
           label + ": output is decodable and its duration is verified");
    if (probe.has_value() && probe->duration_verified) {
        const int64_t probeUs = static_cast<int64_t>(probe->duration_seconds * 1'000'000.0);
        expect(std::llabs(probeUs - expected_us) <= tolerance_us,
               label + ": probe duration is within " + std::to_string(tolerance_us / 1000) +
                   "ms of the requested timeline (probe=" + std::to_string(probeUs) +
                   "us, expected=" + std::to_string(expected_us) + "us)");
    }
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
    const fs::path bframeVideo = root / "bframes.mp4";
    const fs::path audio = root / "audio.m4a";
    const fs::path videoWithAudio = root / "video-with-audio.mp4";
    const fs::path tbA = root / "timescale-90000.mp4";
    const fs::path tbB = root / "timescale-15360.mp4";
    expect(makeVideo(video, "64x64", 5), "video fixture can be created");
    expect(makeBFrameVideo(bframeVideo), "B-frame fixture can be created");
    expect(makeAudio(audio), "audio fixture can be created");
    expect(makeMuxedVideo(video, audio, videoWithAudio),
           "video-with-audio fixture can be created");
    // Same codec/size/pix_fmt/fps as `video`-family fixtures but with pinned,
    // mutually different MP4 track timescales: the only signature difference
    // between tbA and tbB is the stream time base.
    expect(makeVideo(tbA, "64x64", 25, "-video_track_timescale 90000"),
           "90000-timescale video fixture can be created");
    expect(makeVideo(tbB, "64x64", 25, "-video_track_timescale 15360"),
           "15360-timescale video fixture can be created");

    const AVRational tbATimeBase = streamTimeBase(tbA, AVMEDIA_TYPE_VIDEO);
    const AVRational tbBTimeBase = streamTimeBase(tbB, AVMEDIA_TYPE_VIDEO);
    expect(!sameRational(tbATimeBase, tbBTimeBase),
           "timebase fixtures really differ (" + rationalString(tbATimeBase) +
               " vs " + rationalString(tbBTimeBase) + ")");
    const AVRational audioTimeBase = streamTimeBase(audio, AVMEDIA_TYPE_AUDIO);
    const AVRational videoTimeBase = streamTimeBase(video, AVMEDIA_TYPE_VIDEO);
    expect(!sameRational(audioTimeBase, videoTimeBase),
           "audio and video fixtures use different input timebases (" +
               rationalString(audioTimeBase) + " vs " + rationalString(videoTimeBase) + ")");

    // --- video only (also covers the repeated-source-clip case) -----------
    {
        const fs::path output = root / "video-only.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{video, 0, 800'000}, {video, 0, 800'000}};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "video-only streaming mux succeeds");
        checkOutput(output, result, "video-only");
        const auto probe = velox::media::probeMediaInProcess(output);
        expectTimeline(result, probe, 1'600'000, "video-only");
        const auto inspected = inspectStreams(output);
        expect(inspected.video_packets > 0, "video-only mux writes video packets");
        expect(result.audio_packets == 0 && inspected.audio_packets == 0,
               "video-only mux writes no audio packets");
        checkStreams(inspected, result, "video-only");
    }

    // --- video + final voiceover ------------------------------------------
    {
        const fs::path output = root / "voiceover.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{video, 0, 800'000}, {video, 0, 800'000}};
        request.audio = velox::media::CopyOnlyAudioTrack{audio, 0, 1'600'000};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "voiceover streaming mux succeeds");
        checkOutput(output, result, "voiceover");
        const auto probe = velox::media::probeMediaInProcess(output);
        expectTimeline(result, probe, 1'600'000, "voiceover");
        const auto inspected = inspectStreams(output);
        expect(inspected.audio_packets > 0 && result.audio_packets > 0,
               "voiceover mux writes the final audio track");
        checkStreams(inspected, result, "voiceover");
    }

    // --- video + source audio ---------------------------------------------
    {
        const fs::path output = root / "source-audio.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {
            {videoWithAudio, 0, 800'000, true},
            {videoWithAudio, 0, 800'000, true},
        };
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "source-audio streaming mux succeeds");
        checkOutput(output, result, "source-audio");
        const auto probe = velox::media::probeMediaInProcess(output);
        expectTimeline(result, probe, 1'600'000, "source-audio");
        expect(probe.has_value() && probe->streams.size() == 2,
               "source-audio output has video and audio streams");
        const auto inspected = inspectStreams(output);
        expect(inspected.audio_packets > 0 && result.audio_packets > 0,
               "source-audio mux writes the per-segment audio");
        checkStreams(inspected, result, "source-audio");
    }

    // --- B-frames ----------------------------------------------------------
    {
        const fs::path output = root / "bframes.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{bframeVideo, 0, 1'600'000}};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "B-frame streaming mux succeeds");
        checkOutput(output, result, "B-frame");
        const auto probe = velox::media::probeMediaInProcess(output);
        expectTimeline(result, probe, 1'600'000, "B-frame");
        const auto inspected = inspectStreams(output);
        expect(inspected.video_packets > 0,
               "B-frame mux writes video packets");
        checkStreams(inspected, result, "B-frame");
    }

    // --- different input timebases ----------------------------------------
    // (a) a single non-default track timescale end-to-end, (b) coexisting
    // audio/video timebases already exercised by the voiceover + source-audio
    // cases above, (c) two video streams with different timebases must fail
    // closed through the compatibility predicate instead of guessing.
    {
        const fs::path output = root / "timescale-same.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{tbA, 0, 800'000}, {tbA, 0, 800'000}, {tbA, 0, 800'000}};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "non-default track timescale streaming mux succeeds");
        checkOutput(output, result, "timescale-same");
        const auto probe = velox::media::probeMediaInProcess(output);
        expectTimeline(result, probe, 2'400'000, "timescale-same");
        const auto inspected = inspectStreams(output);
        expect(inspected.video_packets > 0,
               "non-default timescale mux writes video packets");
        checkStreams(inspected, result, "timescale-same");
    }
    {
        const fs::path output = root / "timescale-mismatch.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{tbA, 0, 800'000}, {tbB, 0, 800'000}};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(!velox::media::muxCopyOnly(request, &result),
               "incompatible video timebases fail closed");
        expect(!result.success && !result.error.empty(),
               "incompatible timebase failure includes an error");
        expect(result.error.find("media signature mismatch: time_base") != std::string::npos,
               "incompatible timebase failure explains the exact mismatch (actual=" +
                   result.error + ")");
        expect(!fs::exists(output),
               "incompatible timebase failure does not publish output");
    }

    // --- keyframe cuts ------------------------------------------------------
    {
        const fs::path output = root / "keyframe-cut.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{video, 400'000, 400'000}};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "exact keyframe cut streaming mux succeeds");
        checkOutput(output, result, "keyframe-cut");
        const auto probe = velox::media::probeMediaInProcess(output);
        expectTimeline(result, probe, 400'000, "keyframe-cut");
        const auto inspected = inspectStreams(output);
        expect(inspected.video_packets > 0,
               "keyframe cut mux writes video packets");
        checkStreams(inspected, result, "keyframe-cut");
    }

    // --- non-keyframe reject -------------------------------------------------
    {
        const fs::path output = root / "non-keyframe.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{video, 200'000, 400'000}};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(!velox::media::muxCopyOnly(request, &result),
               "non-keyframe source window fails closed");
        expect(!result.success && !result.error.empty(),
               "non-keyframe rejection includes an error");
        expect(result.error.find("keyframe") != std::string::npos,
               "non-keyframe rejection explains the correctness requirement");
        expect(!fs::exists(output),
               "non-keyframe rejection does not publish an output");
    }

    // --- tail extension ------------------------------------------------------
    {
        const fs::path output = root / "tail-extension.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{video, 0, 1'800'000}};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "tail extension streaming mux succeeds");
        checkOutput(output, result, "tail-extension");
        expect(result.success && result.video_packets > 6,
               "tail extension appends decode-free video packets");
        const auto probe = velox::media::probeMediaInProcess(output);
        // The freeze ends on the source frame grid, so the calibrated window
        // is wider than the 80ms used for exact windows.
        expect(probe.has_value() && probe->duration_verified &&
                   probe->duration_seconds > 1.55 && probe->duration_seconds < 1.95,
               "tail extension covers the requested duration (actual=" +
                   (probe.has_value() ? std::to_string(probe->duration_seconds)
                                      : std::string("none")) +
                   ")");
        const auto inspected = inspectStreams(output);
        checkStreams(inspected, result, "tail-extension");
    }

    // --- 20+ segments + determinism -------------------------------------------
    {
        const fs::path output = root / "many-segments.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments.reserve(25);
        for (int i = 0; i < 25; ++i) {
            request.video_segments.push_back({video, 0, 400'000});
        }
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "25-segment streaming mux succeeds");
        checkOutput(output, result, "25-segment");
        const auto probe = velox::media::probeMediaInProcess(output);
        expectTimeline(result, probe, 10'000'000, "25-segment");
        const auto inspected = inspectStreams(output);
        checkStreams(inspected, result, "25-segment");

        const fs::path repeatOutput = root / "many-segments-repeat.mp4";
        auto repeatRequest = request;
        repeatRequest.output_path = repeatOutput;
        velox::media::CopyOnlyMuxResult repeatResult;
        expect(velox::media::muxCopyOnly(repeatRequest, &repeatResult),
               "25-segment streaming mux repeats deterministically");
        const auto repeatInspected = inspectStreams(repeatOutput);
        expect(repeatResult.video_packets == result.video_packets &&
                   repeatResult.audio_packets == result.audio_packets &&
                   repeatResult.duration_us == result.duration_us &&
                   repeatResult.max_buffered_packets == result.max_buffered_packets &&
                   repeatResult.packet_heap_allocations == result.packet_heap_allocations &&
                   repeatResult.global_sort_ms == result.global_sort_ms &&
                   repeatInspected.video_packets == inspected.video_packets &&
                   repeatInspected.audio_packets == inspected.audio_packets,
               "repeated 25-segment run is deterministic in accounting and invariants");
    }

    // --- 1000+ repeated segments ----------------------------------------------
    {
        const fs::path output = root / "thousand-segments.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments.reserve(1000);
        for (int i = 0; i < 1000; ++i) {
            request.video_segments.push_back({video, 0, 800'000});
        }
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "1000-segment streaming mux succeeds");
        checkOutput(output, result, "1000-segment");
        expect(result.duration_us == 1000 * 800'000,
               "1000-segment mux reports the complete logical timeline");
        const auto inspected = inspectStreams(output);
        checkStreams(inspected, result, "1000-segment");
    }

    std::cerr << "summary: fail=" << failures << "\n";
    return failures == 0 ? 0 : 1;
}
