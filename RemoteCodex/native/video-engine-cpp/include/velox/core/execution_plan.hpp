#pragma once

#include <cstdint>
#include <filesystem>
#include <vector>

#include "velox/services/segment_execution.hpp"

namespace velox::core {

// A segment whose asset has already been resolved. The compiled render plan
// decides WHICH clip, WHERE it sits on the timeline and WHEN it starts; this
// descriptor only carries the inputs the execution resolver needs to decide
// HOW to run it. Signatures and keyframe safety come from the LibAV probe,
// never from the plan document.
struct SegmentExecutionInput {
    std::size_t index{0};
    std::filesystem::path input_path;
    int64_t source_in_us{0};
    int64_t source_duration_us{0};
    media::MediaSignature source_signature;
    // Target for non-video segments (audio). Video targets are always the
    // plan's canonical video profile, enforced by the compiler.
    media::MediaSignature target_signature;
    bool transform_required{false};
    bool source_window_keyframe_safe{false};
    bool legacy_required{false};
};

// One segment after resolution. The execution decision is the single
// runtime source of truth for this segment: packet mux, frame pipeline and
// legacy fallback all read this decision instead of re-resolving.
struct ExecutableSegment {
    std::size_t index{0};
    std::filesystem::path input_path;
    int64_t source_in_us{0};
    int64_t source_duration_us{0};
    media::SegmentExecutionDecision execution;
    media::MediaSignature source_signature;
    media::MediaSignature target_signature;
};

// The compiled runtime execution plan. It does not place clips on a
// timeline; it records exactly how each already-placed segment must be
// executed against the canonical output profile.
struct RuntimeExecutionPlan {
    std::vector<ExecutableSegment> segments;
    media::MediaSignature canonical_video_profile;
};

// Produces exactly one execution decision per segment. This is the SSOT of
// runtime execution: every downstream consumer reads the resolved decision
// produced here rather than calling resolveSegmentExecution() again, so the
// PACKET_COPY / NATIVE_TRANSCODE / LEGACY_FALLBACK contract is decided in
// one place for the whole render.
class RuntimeExecutionPlanCompiler {
public:
    RuntimeExecutionPlan compile(
        const std::vector<SegmentExecutionInput>& inputs,
        const media::MediaSignature& canonical_video_profile) const;
};

} // namespace velox::core
