#pragma once
#include <string>
#include <vector>
#include <variant>
#include <optional>

namespace velox::plan {

struct ImageSource {
    std::string url;
    std::string cache_key;
};

struct VideoSource {
    std::string url;
    std::string cache_key;
};

struct ColorSource {
    std::string color_hex;
};

using MediaSource = std::variant<ImageSource, VideoSource, ColorSource>;

struct TransformSpec {
    std::string scale_mode{"cover"}; // cover, contain, stretch
    bool slow_zoom{true};
};

struct TimelineItem {
    MediaSource source;
    double duration_seconds{0.0};
    TransformSpec transform;
};

struct AudioTrack {
    std::string source_url;
    double volume{1.0};
    double start_time_offset{0.0};
};

struct CanvasSpec {
    int width{1920};
    int height{1080};
    int fps{30};
};

struct RenderPlan {
    int version{1};
    std::string job_id;
    CanvasSpec canvas;
    std::vector<TimelineItem> timeline;
    std::vector<AudioTrack> audio_tracks;
    std::string output_path;
};

} // namespace velox::plan
