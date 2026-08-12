#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace velox::media {

struct CopyOnlyVideoSegment {
    std::filesystem::path path;
    int64_t duration_us{0};
    bool include_audio{false};
};

struct CopyOnlyAudioTrack {
    std::filesystem::path path;
    int64_t start_offset_us{0};
    int64_t duration_us{0};
};

struct CopyOnlyMuxRequest {
    std::vector<CopyOnlyVideoSegment> video_segments;
    std::optional<CopyOnlyAudioTrack> audio;
    std::filesystem::path output_path;
};

struct CopyOnlyMuxResult {
    bool success{false};
    // True only when the output rename committed and the parent directory
    // fsync completed. A successful rename with unavailable directory fsync
    // is published but explicitly not durable.
    bool output_durable{false};
    std::string error;
    int64_t video_packets{0};
    int64_t audio_packets{0};
    int64_t duration_us{0};
};

// Concatenate compatible local stream-copy inputs and optionally add one
// already-prepared audio track without decoding or encoding. Every input is
// demuxed in-process, trimmed to its declared duration, rewritten to a common
// microsecond timeline, and written through one MP4 muxer. The output is
// published atomically only after the trailer has been written and the
// temporary file has been synced.
bool muxCopyOnly(const CopyOnlyMuxRequest& request, CopyOnlyMuxResult* result = nullptr);

} // namespace velox::media
