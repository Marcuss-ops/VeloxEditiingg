#ifdef VELOX_ENABLE_LIBAV

#include "media_packet_pipeline_internal.hpp"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
}

#include <algorithm>
#include <filesystem>
#include <string>

namespace fs = std::filesystem;

namespace velox::media::packet {

bool streamAndRewrite(Demuxer& input,
                      const fs::path& path,
                      AVMediaType type,
                      int input_stream_index,
                      AVStream* output_stream,
                      int64_t timeline_offset,
                      int64_t source_in_us,
                      int64_t duration_us,
                      TimestampState& state,
                      PacketConsumer consumer,
                      void* consumer_context,
                      int64_t& packet_count,
                      std::string& error,
                      bool extend_video_tail) {
    if (consumer == nullptr || input_stream_index < 0 || input.raw() == nullptr ||
        static_cast<unsigned int>(input_stream_index) >= input.raw()->nb_streams) {
        error = "streaming packet consumer or stream index is invalid for " + path.string();
        return false;
    }
    if (!input.seekToTimestampUs(input_stream_index, source_in_us, error)) return false;
    const AVStream* input_stream = input.stream(input_stream_index);
    if (input_stream == nullptr || input_stream->codecpar == nullptr ||
        input_stream->codecpar->codec_type != type) {
        error = "requested stream is missing from " + path.string();
        return false;
    }
    const int64_t source_start = validTimestamp(input_stream->start_time) ? input_stream->start_time : 0;
    AVPacket scratch{};
    PendingPacket pending;
    AVPacket last_video_packet{};
    bool have_last_video_packet = false;
    bool eof = false;
    while (!eof) {
        std::string read_error;
        if (!input.readFrame(scratch, eof, read_error)) {
            error = "av_read_frame(" + path.string() + "): " + read_error;
            av_packet_unref(&scratch);
            return false;
        }
        if (eof) break;
        if (scratch.stream_index == input_stream_index) {
            int64_t sort_dts = AV_NOPTS_VALUE;
            if (rewritePacket(scratch, input_stream, output_stream, source_start,
                              source_in_us, timeline_offset, duration_us, state, sort_dts)) {
                if (extend_video_tail && type == AVMEDIA_TYPE_VIDEO) {
                    av_packet_unref(&last_video_packet);
                    if (av_packet_ref(&last_video_packet, &scratch) < 0) {
                        error = "av_packet_ref failed while retaining the last video packet";
                        av_packet_unref(&scratch);
                        return false;
                    }
                    have_last_video_packet = true;
                }
                av_packet_move_ref(&pending.packet, &scratch);
                pending.output_stream_index = output_stream->index;
                pending.sort_dts = sort_dts;
                pending.ready = true;
                ++packet_count;
                if (!consumer(pending, consumer_context, error)) {
                    pending.reset();
                    av_packet_unref(&last_video_packet);
                    return false;
                }
                pending.reset();
            }
        }
        av_packet_unref(&scratch);
    }

    if (extend_video_tail && type == AVMEDIA_TYPE_VIDEO && have_last_video_packet) {
        const int64_t timeline_end = timeline_offset + duration_us;
        AVRational frame_rate = input_stream->avg_frame_rate;
        if (frame_rate.num <= 0 || frame_rate.den <= 0) frame_rate = input_stream->r_frame_rate;
        const int64_t frame_duration_us = frame_rate.num > 0 && frame_rate.den > 0
            ? std::max<int64_t>(1, av_rescale_q(1, AVRational{frame_rate.den, frame_rate.num}, kMicrosecondTimeBase)) : 1;
        const int64_t packet_step_us = last_video_packet.duration > 0 ? last_video_packet.duration : frame_duration_us;
        int64_t next_pts = validTimestamp(last_video_packet.pts) ? last_video_packet.pts + packet_step_us : AV_NOPTS_VALUE;
        int64_t next_dts = validTimestamp(last_video_packet.dts) ? last_video_packet.dts + packet_step_us : AV_NOPTS_VALUE;
        while (validTimestamp(next_pts) || validTimestamp(next_dts)) {
            const int64_t next_timestamp = validTimestamp(next_pts) ? next_pts : next_dts;
            if (next_timestamp >= timeline_end) break;
            av_packet_ref(&pending.packet, &last_video_packet);
            pending.packet.pts = next_pts;
            pending.packet.dts = next_dts;
            pending.packet.duration = std::min<int64_t>(packet_step_us, timeline_end - next_timestamp);
            pending.packet.stream_index = output_stream->index;
            pending.output_stream_index = output_stream->index;
            pending.sort_dts = validTimestamp(next_dts) ? next_dts : next_pts;
            pending.ready = true;
            ++packet_count;
            if (!consumer(pending, consumer_context, error)) {
                pending.reset();
                av_packet_unref(&last_video_packet);
                return false;
            }
            pending.reset();
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

#endif // VELOX_ENABLE_LIBAV
