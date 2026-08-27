#ifdef VELOX_ENABLE_LIBAV

#include "velox/services/media_packet_cursors.hpp"

#include <algorithm>
#include <utility>

namespace velox::media::packet {
namespace {

bool fill(PendingPacket& pending, AVPacket& scratch, const CursorSegment& segment,
          TimestampState& state, AVPacket* lastVideo, bool* haveLast,
          PacketRewriteDecision* decision, std::string& error) {
    *decision = PacketRewriteDecision::BeforeWindow;
    pending.reset();
    const AVStream* input = segment.session->demuxer().stream(segment.stream_index);
    if (input == nullptr) {
        error = "cursor input stream is unavailable: " + segment.path;
        return false;
    }
    const int64_t sourceStart = validTimestamp(input->start_time) ? input->start_time : 0;
    int64_t sortDts = AV_NOPTS_VALUE;
    const PacketRewriteDecision rewrite_decision = rewritePacket(
        scratch, input, segment.output_stream, sourceStart,
        segment.source_in_us, segment.timeline_offset_us,
        segment.duration_us, state, sortDts);
    *decision = rewrite_decision;
    if (rewrite_decision != PacketRewriteDecision::Accepted) {
        return false;
    }
    if (lastVideo != nullptr && haveLast != nullptr) {
        av_packet_unref(lastVideo);
        if (av_packet_ref(lastVideo, &scratch) < 0) {
            error = "cursor failed to retain the last video packet";
            return false;
        }
        *haveLast = true;
    }
    av_packet_move_ref(&pending.packet, &scratch);
    pending.output_stream_index = segment.output_stream->index;
    pending.sort_dts = sortDts;
    pending.ready = true;
    return true;
}

}

VideoTimelineCursor::VideoTimelineCursor(std::vector<CursorSegment> segments, TimestampState& state)
    : segments_(std::move(segments)), state_(state) {}

bool VideoTimelineCursor::loadNextSegment(std::string& error) {
    if (segment_index_ >= segments_.size()) return false;
    auto& segment = segments_[segment_index_];
    if (!segment.session->demuxer().seekToTimestampUs(segment.stream_index, segment.source_in_us, error)) return false;
    return true;
}

bool VideoTimelineCursor::readCurrent(std::string& error) {
    while (segment_index_ < segments_.size()) {
        auto& segment = segments_[segment_index_];
        bool eof = false;
        bool after_window = false;
        while (!eof && !after_window) {
            if (!segment.session->demuxer().readFrame(scratch_, eof, error)) return false;
            if (eof) break;
            if (scratch_.stream_index != segment.stream_index) {
                av_packet_unref(&scratch_);
                continue;
            }
            PacketRewriteDecision decision = PacketRewriteDecision::BeforeWindow;
            if (fill(pending_, scratch_, segment, state_, &last_video_packet_, &have_last_video_packet_, &decision, error)) return true;
            av_packet_unref(&scratch_);
            // B-frame-safe early stop: both clocks past the window end means
            // no later packet can present inside it, so stop demuxing this
            // segment instead of scanning to EOF.
            if (decision == PacketRewriteDecision::AfterWindow) {
                after_window = true;
            }
        }
        ++segment_index_;
        if (segment_index_ < segments_.size() && !loadNextSegment(error)) return false;
    }
    return true;
}

bool VideoTimelineCursor::prime(std::string& error) {
    if (primed_) return pending_.ready;
    primed_ = true;
    if (!loadNextSegment(error)) return segments_.empty();
    return readCurrent(error);
}

bool VideoTimelineCursor::advance(std::string& error) {
    pending_.reset();
    return readCurrent(error);
}

AudioTimelineCursor::AudioTimelineCursor(std::vector<CursorSegment> segments, TimestampState& state)
    : segments_(std::move(segments)), state_(state) {}

bool AudioTimelineCursor::loadNextSegment(std::string& error) {
    if (segment_index_ >= segments_.size()) return false;
    auto& segment = segments_[segment_index_];
    return segment.session->demuxer().seekToTimestampUs(segment.stream_index, segment.source_in_us, error);
}

bool AudioTimelineCursor::readCurrent(std::string& error) {
    while (segment_index_ < segments_.size()) {
        auto& segment = segments_[segment_index_];
        bool eof = false;
        bool after_window = false;
        while (!eof && !after_window) {
            if (!segment.session->demuxer().readFrame(scratch_, eof, error)) return false;
            if (eof) break;
            if (scratch_.stream_index != segment.stream_index) {
                av_packet_unref(&scratch_);
                continue;
            }
            PacketRewriteDecision decision = PacketRewriteDecision::BeforeWindow;
            if (fill(pending_, scratch_, segment, state_, nullptr, nullptr, &decision, error)) return true;
            av_packet_unref(&scratch_);
            // Audio has no decode/presentation reorder (pts == dts), so the
            // first packet past the window on both clocks ends the segment.
            if (decision == PacketRewriteDecision::AfterWindow) {
                after_window = true;
            }
        }
        ++segment_index_;
        if (segment_index_ < segments_.size() && !loadNextSegment(error)) return false;
    }
    return true;
}

bool AudioTimelineCursor::prime(std::string& error) {
    if (primed_) return pending_.ready;
    primed_ = true;
    if (!loadNextSegment(error)) return segments_.empty();
    return readCurrent(error);
}

bool AudioTimelineCursor::advance(std::string& error) {
    pending_.reset();
    return readCurrent(error);
}

} // namespace velox::media::packet

#endif
