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
    // V1 legacy timing in floating seconds (RenderPlan V1 wire contract).
    double duration_seconds{0.0};
    bool include_audio{false};
    TransformSpec transform;
    std::string scene_id;

    // CompiledRenderPlanV2 integer timing (plan_version: 2). Frames and
    // microseconds are the source of truth; float seconds are never used
    // when these are present. 0 means "not provided by this plan version".
    int64_t duration_us{0};
    int64_t source_in_us{0};
    int64_t source_duration_us{0};
    int64_t timeline_start_frame{0};
    int64_t frame_count{0};
};

struct AudioTrack {
    std::string source_url;
    double volume{1.0};
    // V1 legacy timing in floating seconds.
    double start_time_offset{0.0};
    double duration_seconds{0.0};
    std::string role;
    bool loop{false};

    // CompiledRenderPlanV2 integer timing in microseconds.
    int64_t start_offset_us{0};
    int64_t duration_us{0};
};

// Indicates the plan arrived as a CompiledRenderPlanV2 document
// (plan_version: 2) carrying integer frames/microseconds instead of the V1
// floating-second timeline.
constexpr int kRenderPlanVersionV2 = 2;
constexpr int kRenderPlanVersionV1 = 1;

struct SubtitleTrack {
    std::string source;
    std::string preset;
    std::string font;
};

struct CanvasSpec {
    int width{1920};
    int height{1080};
    int fps{30};
    // CompiledRenderPlanV2 rational frame rate (fps = fps_num / fps_den).
    int fps_num{0};
    int fps_den{0};
};

struct RenderPlan {
    int version{1};
    std::string job_id;
    CanvasSpec canvas;
    bool copy_only{false};
    std::vector<TimelineItem> timeline;
    std::vector<AudioTrack> audio_tracks;
    std::vector<SubtitleTrack> subtitle_tracks;
    std::string output_path;
};

} // namespace velox::plan
