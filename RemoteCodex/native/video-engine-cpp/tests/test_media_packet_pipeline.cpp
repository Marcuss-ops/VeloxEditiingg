#include "velox/services/file_utils.hpp"
#include "velox/services/media_packet_pipeline.hpp"
#include "velox/services/media_probe.hpp"

extern "C" {
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
}

#include <chrono>
#include <cmath>
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

bool makeVideo(const fs::path& output, const std::string& size) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote(
                "testsrc=size=" + size + ":rate=5:duration=1.2")
            << " -an -c:v libx264 -preset ultrafast -pix_fmt yuv420p -r 5 "
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
    const fs::path audio = root / "audio.m4a";
    const fs::path videoWithAudio = root / "video-with-audio.mp4";
    const fs::path output = root / "packet-output.mp4";
    const fs::path failedOutput = root / "failed-output.mp4";
    expect(makeVideo(video, "64x64"), "video fixture can be created");
    expect(makeVideo(incompatible, "32x32"), "incompatible video fixture can be created");
    expect(makeAudio(audio), "audio fixture can be created");
    expect(makeMuxedVideo(video, audio, videoWithAudio),
           "video-with-audio fixture can be created");
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
        {video, 800'000},
        {video, 800'000},
    };
    request.audio = velox::media::CopyOnlyAudioTrack{audio, 0, 1'600'000};
    request.output_path = output;
    velox::media::CopyOnlyMuxResult muxResult;
    expect(velox::media::muxCopyOnly(request, &muxResult),
           "packet pipeline concatenates video and muxes audio in-process");
    expect(muxResult.success, "successful packet mux reports success");
    expect(muxResult.video_packets > 0, "packet mux writes video packets");
    expect(muxResult.audio_packets > 0, "packet mux writes audio packets");
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

    const fs::path segmentAudioOutput = root / "segment-audio-output.mp4";
    velox::media::CopyOnlyMuxRequest segmentAudioRequest;
    segmentAudioRequest.video_segments = {
        {videoWithAudio, 800'000, true},
        {videoWithAudio, 800'000, true},
    };
    segmentAudioRequest.output_path = segmentAudioOutput;
    velox::media::CopyOnlyMuxResult segmentAudioResult;
    expect(velox::media::muxCopyOnly(segmentAudioRequest, &segmentAudioResult),
           "copy-only preserves compatible segment audio through packet mapping");
    const auto segmentAudioProbe = velox::media::probeMediaInProcess(segmentAudioOutput);
    expect(segmentAudioProbe.has_value() && segmentAudioProbe->streams.size() == 2,
           "segment audio packet mapping produces video and audio streams");

    velox::media::CopyOnlyMuxRequest incompatibleRequest;
    incompatibleRequest.video_segments = {{video, 800'000}, {incompatible, 800'000}};
    incompatibleRequest.output_path = failedOutput;
    velox::media::CopyOnlyMuxResult failureResult;
    expect(!velox::media::muxCopyOnly(incompatibleRequest, &failureResult),
           "incompatible stream parameters fail closed");
    expect(!failureResult.success && !failureResult.error.empty(),
           "incompatible stream failure includes an error");
    expect(!fs::exists(failedOutput), "failed packet mux does not publish output");

    const fs::path shortAudioOutput = root / "short-audio-output.mp4";
    auto shortAudioRequest = request;
    shortAudioRequest.audio = velox::media::CopyOnlyAudioTrack{audio, 0, 400'000};
    shortAudioRequest.output_path = shortAudioOutput;
    velox::media::CopyOnlyMuxResult shortAudioResult;
    expect(!velox::media::muxCopyOnly(shortAudioRequest, &shortAudioResult),
           "audio shorter than the requested video timeline fails closed");
    expect(!fs::exists(shortAudioOutput),
           "short-audio failure does not publish a partial output");

    if (hadPath) {
        setenv("PATH", previousPathValue.c_str(), 1);
    } else {
        unsetenv("PATH");
    }

    std::cerr << "summary: fail=" << failures << "\n";
    return failures == 0 ? 0 : 1;
}
