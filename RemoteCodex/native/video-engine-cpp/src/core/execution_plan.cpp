#include "velox/core/execution_plan.hpp"

#include <utility>

namespace velox::core {

RuntimeExecutionPlan RuntimeExecutionPlanCompiler::compile(
    const std::vector<SegmentExecutionInput>& inputs,
    const media::MediaSignature& canonical_video_profile) const {
    RuntimeExecutionPlan plan;
    plan.canonical_video_profile = canonical_video_profile;
    plan.segments.reserve(inputs.size());

    for (const auto& input : inputs) {
        media::SegmentExecutionRequest request;
        request.source = input.source_signature;
        // Every video segment targets the canonical output profile; only
        // non-video (audio) segments keep their own target signature. This
        // guarantees the mixed renderer produces exactly one compatible
        // video form instead of a per-segment target.
        request.target = input.source_signature.kind == media::MediaKind::Video
            ? canonical_video_profile
            : input.target_signature;
        request.transform_required = input.transform_required;
        request.source_window_keyframe_safe = input.source_window_keyframe_safe;
        request.legacy_required = input.legacy_required;

        ExecutableSegment segment;
        segment.index = input.index;
        segment.input_path = input.input_path;
        segment.source_in_us = input.source_in_us;
        segment.source_duration_us = input.source_duration_us;
        segment.source_signature = input.source_signature;
        segment.target_signature = request.target;
        segment.execution = media::resolveSegmentExecution(request);

        plan.segments.push_back(std::move(segment));
    }

    return plan;
}

} // namespace velox::core
