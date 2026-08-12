#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace velox::media {

enum class MediaKind {
    Video,
    Audio,
};

// A value-only media identity used by every execution decision. Keeping this
// independent from LibAV makes the resolver testable without a media fixture
// and prevents callers from inventing a second compatibility predicate.
struct MediaSignature {
    MediaKind kind{MediaKind::Video};
    int codec_id{-1};
    int profile{-1};
    int level{-1};
    int width{0};
    int height{0};
    int pixel_format{-1};
    int frame_rate_num{0};
    int frame_rate_den{1};
    int sample_rate{0};
    int channels{0};
    std::string channel_layout;
    std::vector<std::uint8_t> extradata;
};

bool mediaSignaturesCompatible(const MediaSignature& source,
                               const MediaSignature& target,
                               std::string* reason = nullptr);

enum class SegmentExecutionMode {
    PacketCopy,
    NativeTranscode,
    LegacyFallback,
};

const char* segmentExecutionModeName(SegmentExecutionMode mode);

struct SegmentExecutionRequest {
    MediaSignature source;
    MediaSignature target;
    bool transform_required{false};
    bool source_window_keyframe_safe{false};
    bool legacy_required{false};
};

struct SegmentExecutionDecision {
    SegmentExecutionMode mode{SegmentExecutionMode::LegacyFallback};
    std::string reason;
    bool keyframe_safe{false};
};

// Resolves a segment exactly once. Packet copy is selected only when the
// source already matches the canonical target and its requested trim starts
// on a keyframe. Transform and incompatible-profile work is explicitly
// NativeTranscode; only features not yet supported by the native renderer
// are LegacyFallback.
SegmentExecutionDecision resolveSegmentExecution(const SegmentExecutionRequest& request);

} // namespace velox::media
