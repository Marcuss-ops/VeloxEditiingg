#ifdef VELOX_ENABLE_LIBAV

#include "media_packet_pipeline_internal.hpp"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
}

#include <filesystem>
#include <string>

namespace fs = std::filesystem;

namespace velox::media::packet {

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
    if (validTimestamp(packet.pts) && packet.pts < 0) packet.pts = 0;
    if (validTimestamp(packet.dts) && packet.dts < 0) packet.dts = 0;
    packet.duration = packet.duration > 0
        ? rescale(packet.duration, input_stream->time_base, kMicrosecondTimeBase)
        : 0;

    if (validTimestamp(packet.pts)) packet.pts += timeline_offset;
    if (validTimestamp(packet.dts)) packet.dts += timeline_offset;
    if (validTimestamp(packet.pts) && validTimestamp(packet.dts) && packet.pts < packet.dts) {
        packet.pts = packet.dts;
    }

    const int64_t end = timeline_offset + segment_duration;
    const int64_t packet_timestamp = validTimestamp(packet.pts) ? packet.pts : packet.dts;
    if (!validTimestamp(packet_timestamp) || packet_timestamp >= end) {
        return false;
    }
    if (packet.duration > 0 && packet_timestamp + packet.duration > end) {
        packet.duration = end - packet_timestamp;
    }

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

} // namespace velox::media::packet

#endif // VELOX_ENABLE_LIBAV
