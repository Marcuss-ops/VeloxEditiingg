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

PacketRewriteDecision rewritePacket(AVPacket& packet,
                                     const AVStream* input_stream,
                                     const AVStream* output_stream,
                                     int64_t source_start,
                                     int64_t source_in_us,
                                     int64_t timeline_offset,
                                     int64_t segment_duration,
                                     TimestampState& state,
                                     int64_t& sort_dts) {
    // Relative (source-window) references for both clocks. The decision is
    // classified on the pre-rewrite values so a rejected packet stays
    // untouched.
    const int64_t relative_pts = validTimestamp(packet.pts)
        ? relativeTimestamp(packet.pts, source_start, input_stream->time_base) - source_in_us
        : AV_NOPTS_VALUE;
    const int64_t relative_dts = validTimestamp(packet.dts)
        ? relativeTimestamp(packet.dts, source_start, input_stream->time_base) - source_in_us
        : AV_NOPTS_VALUE;

    const int64_t reference = validTimestamp(relative_pts) ? relative_pts : relative_dts;
    if (!validTimestamp(reference) ||
        (source_in_us > 0 && reference < 0) ||
        reference >= segment_duration) {
        // B-frame safe AfterWindow: only when BOTH clocks are past the
        // window end can no later packet present inside it. A packet with
        // pts past the end but dts inside is an anchor/B-frame interleave
        // that later packets still depend on, so it keeps the demux
        // scanning.
        if (validTimestamp(relative_pts) && validTimestamp(relative_dts) &&
            relative_pts >= segment_duration && relative_dts >= segment_duration) {
            return PacketRewriteDecision::AfterWindow;
        }
        return PacketRewriteDecision::BeforeWindow;
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
        if (validTimestamp(packet.pts) && validTimestamp(packet.dts) &&
            packet.pts >= end && packet.dts >= end) {
            return PacketRewriteDecision::AfterWindow;
        }
        return PacketRewriteDecision::BeforeWindow;
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
    return PacketRewriteDecision::Accepted;
}

} // namespace velox::media::packet

#endif // VELOX_ENABLE_LIBAV
