#pragma once

#include <cstdint>
#include <string>
#include <utility>
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
    int time_base_num{0};
    int time_base_den{1};
    int sample_rate{0};
    int channels{0};
    std::string channel_layout;
    std::vector<std::uint8_t> extradata;
};

// The single packet-copy compatibility predicate. Callers MUST NOT invent a
// second, private comparison: every "can this stream be stream-copied into
// the target" question goes through here.
//
// profile/level (and extradata) are pin-optional on the TARGET: a field left
// at its default means the target does not constrain that field ("don't
// care"), so only fields the target actually pins are compared. The canonical
// output profile pins profile/level explicitly, so its strictness is
// unchanged; a target that only wants to pin codec/pix_fmt/dims/fps leaves
// profile/level at their defaults.
bool mediaSignaturesCompatible(const MediaSignature& source,
                               const MediaSignature& target,
                               std::string* reason = nullptr);

// The single packet-copy execution mode. Velox's video assembly / mixed /
// concat path is COPY-ONLY: a segment is either stream-copied into the
// packet mux (PacketCopy) or rejected (Reject). NativeTranscode and
// LegacyFallback were removed on purpose — the assembly path must never
// re-encode a segment, and a binary enum makes it impossible for a future
// change to silently re-introduce transcoding. Transcoding still exists for
// the legacy render loop (images/color/legacy rendering), but that decision
// is made locally there, never through this enum.
enum class SegmentExecutionMode {
    PacketCopy,
    Reject,
};

const char* segmentExecutionModeName(SegmentExecutionMode mode);

struct SegmentExecutionRequest {
    MediaSignature source;
    MediaSignature target;
    bool transform_required{false};
    bool source_window_keyframe_safe{false};
    bool legacy_required{false};

    SegmentExecutionRequest() = default;

    // Keep the short form unambiguous for packet-mux callers: its third
    // argument is the keyframe-safety result, never a transform request.
    SegmentExecutionRequest(MediaSignature source_signature,
                            MediaSignature target_signature,
                            bool keyframe_safe)
        : source(std::move(source_signature)),
          target(std::move(target_signature)),
          source_window_keyframe_safe(keyframe_safe) {}

    SegmentExecutionRequest(MediaSignature source_signature,
                            MediaSignature target_signature,
                            bool transform,
                            bool keyframe_safe,
                            bool legacy)
        : source(std::move(source_signature)),
          target(std::move(target_signature)),
          transform_required(transform),
          source_window_keyframe_safe(keyframe_safe),
          legacy_required(legacy) {}
};

struct SegmentExecutionDecision {
    // Fail-closed default: an unset/unknown decision is a reject, never a
    // silent fallback to transcoding.
    SegmentExecutionMode mode{SegmentExecutionMode::Reject};
    std::string reason;
    bool keyframe_safe{false};
};

// Resolves a segment exactly once, fail-closed. Packet copy is selected only
// when the source already matches the canonical target AND its requested trim
// starts on a keyframe AND no transform/legacy feature is requested. Every
// other condition (legacy feature, media transform, non-keyframe-safe trim,
// incompatible media signature) resolves to Reject with an exact reason —
// never to a transcode. The assembly path is copy-only; jobs that need any
// re-encoding must be prepared upstream (Chronon/RenderingGen).
SegmentExecutionDecision resolveSegmentExecution(const SegmentExecutionRequest& request);

} // namespace velox::media
