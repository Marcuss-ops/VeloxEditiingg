#include "velox/core/execution_plan.hpp"

#include <iostream>
#include <string>
#include <vector>

namespace {

int failures = 0;
constexpr int h264CodecId = 27;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << "\n";
        ++failures;
    }
}

velox::media::MediaSignature videoSignature() {
    velox::media::MediaSignature signature;
    signature.kind = velox::media::MediaKind::Video;
    signature.codec_id = h264CodecId;
    signature.profile = 100;
    signature.level = 40;
    signature.width = 1920;
    signature.height = 1080;
    signature.pixel_format = 0;
    signature.frame_rate_num = 30;
    signature.frame_rate_den = 1;
    signature.extradata = {1, 2, 3};
    return signature;
}

velox::core::SegmentExecutionInput segmentInput(
    std::size_t index,
    velox::media::MediaSignature source,
    bool keyframe_safe = true,
    bool transform_required = false,
    bool legacy_required = false) {
    velox::core::SegmentExecutionInput input;
    input.index = index;
    input.input_path = "/tmp/segment_" + std::to_string(index) + ".mp4";
    input.source_in_us = 0;
    input.source_duration_us = 1'000'000;
    input.source_signature = std::move(source);
    input.source_window_keyframe_safe = keyframe_safe;
    input.transform_required = transform_required;
    input.legacy_required = legacy_required;
    return input;
}

} // namespace

int main() {
    using velox::media::SegmentExecutionMode;

    const auto canonical = videoSignature();

    // Three matching, keyframe-safe video segments: all resolve to
    // PACKET_COPY against the canonical profile.
    std::vector<velox::core::SegmentExecutionInput> inputs;
    inputs.push_back(segmentInput(0, canonical));
    inputs.push_back(segmentInput(1, canonical));
    inputs.push_back(segmentInput(2, canonical));

    velox::core::RuntimeExecutionPlanCompiler compiler;
    auto plan = compiler.compile(inputs, canonical);

    expect(plan.canonical_video_profile.profile == canonical.profile,
           "compiler preserves the canonical video profile");
    expect(plan.segments.size() == 3,
           "compiler returns one executable segment per input");
    for (std::size_t i = 0; i < plan.segments.size(); ++i) {
        expect(plan.segments[i].index == i,
               "executable segment keeps its input index");
        expect(plan.segments[i].execution.mode == SegmentExecutionMode::PacketCopy,
               "matching keyframe-safe segment uses packet copy");
        expect(plan.segments[i].target_signature.codec_id == canonical.codec_id,
               "video segment targets the canonical profile");
    }

    // A mixed plan: one incompatible source resolves to REJECT and the
    // remaining compatible sources stay PACKET_COPY — a single decision per
    // segment in one compile pass. The copy-only contract means the
    // incompatible segment is rejected, never transcoded.
    inputs.clear();
    inputs.push_back(segmentInput(0, canonical));
    auto incompatible = canonical;
    incompatible.width = 1280;
    inputs.push_back(segmentInput(1, incompatible));
    inputs.push_back(segmentInput(2, canonical));

    plan = compiler.compile(inputs, canonical);
    expect(plan.segments.size() == 3, "mixed plan keeps one decision per segment");
    expect(plan.segments[0].execution.mode == SegmentExecutionMode::PacketCopy,
           "compatible segment stays packet copy in a mixed plan");
    expect(plan.segments[1].execution.mode == SegmentExecutionMode::Reject,
           "incompatible segment is rejected (copy-only)");
    expect(plan.segments[1].execution.reason == "media signature mismatch: width",
           "mixed plan preserves the resolver's deterministic reason");
    expect(plan.segments[2].execution.mode == SegmentExecutionMode::PacketCopy,
           "trailing compatible segment stays packet copy");

    // Transform and legacy-required segments resolve to REJECT without
    // disturbing the neighboring packet-copy decision.
    inputs.clear();
    inputs.push_back(segmentInput(0, canonical));
    inputs.push_back(segmentInput(1, canonical, true, true, false));   // transform
    inputs.push_back(segmentInput(2, canonical, true, false, true));   // legacy-only feature
    plan = compiler.compile(inputs, canonical);
    expect(plan.segments[0].execution.mode == SegmentExecutionMode::PacketCopy,
           "transform segment does not disturb the leading packet copy");
    expect(plan.segments[1].execution.mode == SegmentExecutionMode::Reject,
           "transform-required segment is rejected (copy-only)");
    expect(plan.segments[2].execution.mode == SegmentExecutionMode::Reject,
           "legacy-required segment is rejected (copy-only)");

    // Non-keyframe-safe trim must not use packet copy.
    inputs.clear();
    inputs.push_back(segmentInput(0, canonical, false));
    plan = compiler.compile(inputs, canonical);
    expect(plan.segments[0].execution.mode == SegmentExecutionMode::Reject,
           "non-keyframe trim is rejected (copy-only)");

    // Audio segments keep their own target signature instead of the video
    // canonical profile.
    auto audio_source = velox::media::MediaSignature{};
    audio_source.kind = velox::media::MediaKind::Audio;
    audio_source.codec_id = 86018;
    audio_source.pixel_format = 8;
    audio_source.sample_rate = 48000;
    audio_source.channels = 2;
    audio_source.channel_layout = "stereo";
    auto audio_target = audio_source;

    inputs.clear();
    auto audio_input = segmentInput(0, audio_source);
    audio_input.target_signature = audio_target;
    inputs.push_back(audio_input);
    plan = compiler.compile(inputs, canonical);
    expect(plan.segments[0].target_signature.channel_layout == "stereo",
           "audio segment keeps its own target instead of the video profile");
    expect(plan.segments[0].execution.mode == SegmentExecutionMode::PacketCopy,
           "matching audio segment resolves to packet copy");

    return failures == 0 ? 0 : 1;
}
