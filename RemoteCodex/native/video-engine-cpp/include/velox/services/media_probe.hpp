#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace velox::media {

// Probe data intentionally contains value types only. LibAV ownership and
// ABI-specific structs stay private to media_probe.cpp.
struct MediaProbeStream {
    int index{0};
    bool is_video{false};
    bool is_audio{false};
    int codec_id{0};
    int pixel_format{0};
    int width{0};
    int height{0};
    double average_frame_rate{0.0};
    int sample_rate{0};
    int channels{0};
    std::string channel_layout;
    double duration_seconds{0.0};
    bool duration_verified{false};
    double start_time_seconds{0.0};
    bool start_time_verified{false};
    bool extradata_present{false};
};

struct MediaProbeResult {
    double duration_seconds{0.0};
    bool duration_verified{false};
    std::string format_name;
    std::vector<MediaProbeStream> streams;
};

// Opens a local media path and reads its format/stream metadata in-process
// through libavformat/libavcodec/libavutil. No ffprobe or shell is invoked.
//
// Defined only when the engine is built with VELOX_ENABLE_LIBAV=ON (see
// media_probe.cpp). Non-LibAV builds compile media_probe.cpp to an empty
// translation unit and route every probe through the ffprobe CLI fallback
// in media_utils_probe.cpp, so this symbol has no callers and is intentionally
// left undefined — calling it from a non-LibAV build fails at link time.
std::optional<MediaProbeResult> probeMediaInProcess(const std::filesystem::path& mediaPath);

} // namespace velox::media
