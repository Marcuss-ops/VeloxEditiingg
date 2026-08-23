#pragma once
#include <string>
#include <vector>
#include <filesystem>

#include "velox/services/file_utils.hpp"

namespace velox::media {

enum class FinalAudioMode {
    Encode,
    Copy,
};

struct FinalAudioMetadata {
    bool metadata_verified{false};
    std::string codec;
    int sample_rate{0};
    int channels{0};
    std::string channel_layout;
    double duration_seconds{0.0};
    double start_time_seconds{0.0};
    bool duration_verified{false};
    bool start_time_verified{false};
    // FINAL_AUDIO_COPY transport guards. Raw AAC/ADTS/LATM without MP4
    // AudioSpecificConfig must fall back to AAC encoding.
    std::string format_name;
    bool extradata_verified{false};
    bool container_verified{false};
};

struct FinalAudioDecision {
    FinalAudioMode mode{FinalAudioMode::Encode};
    std::string reason{"encode_default"};
    FinalAudioMetadata metadata;
};

FinalAudioMetadata probeFinalAudioMetadata(const std::filesystem::path& audioPath);

FinalAudioDecision resolveFinalAudioMode(
    const FinalAudioMetadata& metadata,
    bool isFinalMix,
    double expectedDurationSeconds,
    double volume,
    double startOffset);

// Packet-mode FINAL_AUDIO_COPY decision for the in-process copy-only mux
// (muxCopyOnly). Unlike the legacy FFmpeg resolver above, the packet path
// never re-encodes: audio packets are copied into the same MP4 mux as the
// video packets, the muxer trims trailing audio to the video timeline and
// neutralizes a non-zero source start_time during the rewrite, and a
// positive start offset is a pure packet timestamp shift. The prepared
// upstream audio therefore only needs to be a verified MP4-AAC track whose
// duration COVERS the video timeline (longer is trimmed, shorter fails).
//
// Volume is intentionally not part of this signature: any volume != 1.0
// would require a filter/re-encode and is rejected by the copy-only plan
// validation before this resolver runs.
FinalAudioDecision resolveFinalAudioModePacket(
    const FinalAudioMetadata& metadata,
    bool isFinalMix,
    double expectedDurationSeconds);

const char* finalAudioModeName(FinalAudioMode mode);

struct SceneSegmentParams {
    int width{1920};
    int height{1080};
    int fps{24};
    bool copy_only{false};
    bool slow_zoom{true};
    std::string scale_mode{"cover"}; // cover, contain, stretch
    std::string color_hex{""};
};

double probeMediaDurationSeconds(const std::filesystem::path& mediaPath);

// Returns true when the media file contains at least one audio stream.
// Video clips are allowed to be silent; callers mixing several tracks must
// omit those files from the audio filter graph rather than referencing a
// nonexistent :a stream.
bool hasAudioStream(const std::filesystem::path& mediaPath);

// ─── F5: args-only builders ─────────────────────────────────────────────
//
// These return the FFmpeg ARGUMENTS string only — NO `ffmpeg` prefix,
// NO leading global flags like `-y` / `-hide_banner` / `-loglevel`.
//
// Callers prepend their own global flags. This lets
// `render_engine.cpp` inject `-progress pipe:1 -nostats` cleanly.
std::string buildSceneSegmentArgs(
    const std::filesystem::path& imagePath,
    const std::filesystem::path& segmentPath,
    double duration,
    const SceneSegmentParams& params);

std::string buildVideoSegmentArgs(
    const std::filesystem::path& clipPath,
    const std::filesystem::path& segmentPath,
    double duration,
    const SceneSegmentParams& params,
    bool includeAudio = false);

std::string buildColorSegmentArgs(
    const std::filesystem::path& segmentPath,
    double duration,
    const SceneSegmentParams& params,
    const std::string& color_hex);

// ─── Existing execution wrappers (shell-prepend "ffmpeg" + runCommand) ──
//
// These preserve the legacy surface used by `cmd_full_video.cpp`. They
// internally delegate to the *Args builders and add the canonical
// global flags.
bool buildSceneSegment(const std::filesystem::path& imagePath,
                       const std::filesystem::path& segmentPath,
                       double duration,
                       const SceneSegmentParams& params = {});

bool buildVideoSegment(const std::filesystem::path& clipPath,
                       const std::filesystem::path& segmentPath,
                       double duration,
                       const SceneSegmentParams& params = {});

bool concatSegments(const std::vector<std::filesystem::path>& segments,
                    const std::filesystem::path& outputPath,
                    const std::filesystem::path& workDir);

bool muxAudio(const std::filesystem::path& videoPath,
              const std::filesystem::path& audioPath,
              const std::filesystem::path& outputPath,
              double volume = 1.0,
              double startOffset = 0.0,
              file::CommandResult* profile = nullptr,
              bool isFinalMix = false,
              double expectedDurationSeconds = 0.0,
              FinalAudioDecision* decision = nullptr);

} // namespace velox::media
