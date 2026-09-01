#pragma once

#include "velox/services/file_utils.hpp"
#include "velox/services/media_packet_pipeline.hpp"
#include "velox/services/media_probe.hpp"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
}

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

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

struct DecodedFrame {
    int64_t pts_us{0};
    uint32_t luma_checksum{0};
};

std::vector<DecodedFrame> decodeAllVideoFrames(const fs::path& path) {
    std::vector<DecodedFrame> frames;
    AVFormatContext* fmt = nullptr;
    if (avformat_open_input(&fmt, path.c_str(), nullptr, nullptr) < 0 || !fmt) {
        if (fmt) avformat_close_input(&fmt);
        return frames;
    }
    if (avformat_find_stream_info(fmt, nullptr) < 0) {
        avformat_close_input(&fmt);
        return frames;
    }

    int videoStream = -1;
    for (unsigned int i = 0; i < fmt->nb_streams; ++i) {
        if (fmt->streams[i]->codecpar->codec_type == AVMEDIA_TYPE_VIDEO) {
            videoStream = static_cast<int>(i);
            break;
        }
    }
    if (videoStream < 0) {
        avformat_close_input(&fmt);
        return frames;
    }

    const AVCodecParameters* par = fmt->streams[videoStream]->codecpar;
    const AVCodec* codec = avcodec_find_decoder(par->codec_id);
    if (!codec) {
        avformat_close_input(&fmt);
        return frames;
    }
    AVCodecContext* ctx = avcodec_alloc_context3(codec);
    if (!ctx) {
        avformat_close_input(&fmt);
        return frames;
    }
    avcodec_parameters_to_context(ctx, par);
    if (avcodec_open2(ctx, codec, nullptr) < 0) {
        avcodec_free_context(&ctx);
        avformat_close_input(&fmt);
        return frames;
    }

    const AVRational stream_tb = fmt->streams[videoStream]->time_base;
    AVPacket* pkt = av_packet_alloc();
    AVFrame* frame = av_frame_alloc();

    while (av_read_frame(fmt, pkt) >= 0) {
        if (pkt->stream_index != videoStream) {
            av_packet_unref(pkt);
            continue;
        }
        int rc = avcodec_send_packet(ctx, pkt);
        av_packet_unref(pkt);
        if (rc < 0) continue;
        while (avcodec_receive_frame(ctx, frame) == 0) {
            DecodedFrame df;
            if (frame->best_effort_timestamp != AV_NOPTS_VALUE) {
                df.pts_us = av_rescale_q(frame->best_effort_timestamp,
                                         stream_tb,
                                         AVRational{1, 1'000'000});
            }
            if (frame->format == AV_PIX_FMT_YUV420P ||
                frame->format == AV_PIX_FMT_YUV422P ||
                frame->format == AV_PIX_FMT_YUV444P) {
                const int w = frame->width;
                const int h = frame->height;
                const uint8_t* y = frame->data[0];
                const int ystride = frame->linesize[0];
                uint32_t sum = 0;
                for (int row = 0; row < h; ++row) {
                    for (int col = 0; col < w; ++col) {
                        sum += y[row * ystride + col];
                    }
                }
                df.luma_checksum = sum;
            }
            frames.push_back(df);
        }
    }

    avcodec_send_packet(ctx, nullptr);
    while (avcodec_receive_frame(ctx, frame) == 0) {
        DecodedFrame df;
        if (frame->best_effort_timestamp != AV_NOPTS_VALUE) {
            df.pts_us = av_rescale_q(frame->best_effort_timestamp,
                                     stream_tb,
                                     AVRational{1, 1'000'000});
        }
        if (frame->format == AV_PIX_FMT_YUV420P ||
            frame->format == AV_PIX_FMT_YUV422P ||
            frame->format == AV_PIX_FMT_YUV444P) {
            const int w = frame->width;
            const int h = frame->height;
            const uint8_t* y = frame->data[0];
            const int ystride = frame->linesize[0];
            uint32_t sum = 0;
            for (int row = 0; row < h; ++row) {
                for (int col = 0; col < w; ++col) {
                    sum += y[row * ystride + col];
                }
            }
            df.luma_checksum = sum;
        }
        frames.push_back(df);
    }

    av_frame_free(&frame);
    av_packet_free(&pkt);
    avcodec_free_context(&ctx);
    avformat_close_input(&fmt);
    return frames;
}

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
    expect((result.backward_seek_count == 0) == (result.backward_seek_bytes == 0),
           label + ": backward seek count and rewound bytes are consistent");
    if (result.backward_seek_seen) {
        expect(result.sha256.empty(),
               label + ": backward seek disables the opportunistic SHA");
    } else if (result.sha256_valid) {
        expect(!result.sha256.empty(),
               label + ": valid opportunistic SHA is non-empty");
    }
}

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
