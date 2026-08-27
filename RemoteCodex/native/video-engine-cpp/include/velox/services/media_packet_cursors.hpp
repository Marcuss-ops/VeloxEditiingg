#pragma once

#include "velox/services/media_packet_components.hpp"

#include <cstddef>
#include <string>
#include <vector>

namespace velox::media::packet {

struct CursorSegment {
    InputSession* session{nullptr};
    std::string path;
    int stream_index{-1};
    AVStream* output_stream{nullptr};
    int64_t timeline_offset_us{0};
    int64_t source_in_us{0};
    int64_t duration_us{0};
    bool extend_video_tail{false};
};

class VideoTimelineCursor {
public:
    VideoTimelineCursor(std::vector<CursorSegment> segments, TimestampState& state);
    bool prime(std::string& error);
    bool advance(std::string& error);
    bool hasPacket() const { return pending_.ready; }
    PendingPacket& current() { return pending_; }

private:
    bool readCurrent(std::string& error);
    bool loadNextSegment(std::string& error);

    std::vector<CursorSegment> segments_;
    TimestampState& state_;
    std::size_t segment_index_{0};
    AVPacket scratch_{};
    AVPacket last_video_packet_{};
    bool have_last_video_packet_{false};
    bool primed_{false};
    PendingPacket pending_;
};

class AudioTimelineCursor {
public:
    AudioTimelineCursor(std::vector<CursorSegment> segments, TimestampState& state);
    bool prime(std::string& error);
    bool advance(std::string& error);
    bool hasPacket() const { return pending_.ready; }
    PendingPacket& current() { return pending_; }

private:
    bool readCurrent(std::string& error);
    bool loadNextSegment(std::string& error);

    std::vector<CursorSegment> segments_;
    TimestampState& state_;
    std::size_t segment_index_{0};
    AVPacket scratch_{};
    bool primed_{false};
    PendingPacket pending_;
};

} // namespace velox::media::packet
