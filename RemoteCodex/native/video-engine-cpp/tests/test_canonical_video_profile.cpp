#include "velox/core/canonical_video_profile.hpp"

#include <iostream>
#include <string>

namespace {

int failures = 0;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << "\n";
        ++failures;
    }
}

} // namespace

int main() {
    using velox::core::canonicalVideoProfileV1;
    using velox::core::mediaSignatureFromCanonicalProfile;
    using velox::media::SegmentExecutionMode;

    const auto& profile = canonicalVideoProfileV1();
    expect(profile.codec == "libx264", "V1 profile pins the libx264 encoder");
    expect(profile.codec_id == 27, "V1 profile uses the H.264 stream codec id");
    expect(profile.width == 1920 && profile.height == 1080,
           "V1 profile is 1920x1080");
    expect(profile.fps_num == 24 && profile.fps_den == 1,
           "V1 profile is 24 fps");
    expect(profile.pixel_format == 0, "V1 profile is yuv420p");
    expect(profile.profile == 100, "V1 profile is H.264 High");
    expect(profile.level == 40, "V1 profile is H.264 level 4.0");
    expect(profile.gop_size == 48, "V1 profile pins GOP 48");
    expect(profile.max_b_frames == 0, "V1 profile disables B-frames");
    expect(profile.preset == "medium", "V1 profile uses the medium preset");
    expect(profile.crf == 23, "V1 profile pins CRF 23");
    expect(profile.version == 1, "V1 profile carries version 1");

    const auto signature = mediaSignatureFromCanonicalProfile(profile);
    expect(signature.kind == velox::media::MediaKind::Video,
           "canonical profile derives a video signature");
    expect(signature.codec_id == profile.codec_id,
           "derived signature carries the codec id");
    expect(signature.profile == profile.profile && signature.level == profile.level,
           "derived signature carries profile and level");
    expect(signature.width == profile.width && signature.height == profile.height,
           "derived signature carries the canvas geometry");
    expect(signature.pixel_format == profile.pixel_format,
           "derived signature carries the pixel format");
    expect(signature.frame_rate_num == profile.fps_num &&
               signature.frame_rate_den == profile.fps_den,
           "derived signature carries the rational frame rate");
    expect(signature.extradata.empty(),
           "derived signature does not pin bitstream extradata");

    // The derived signature must be the canonical packet-copy target: a
    // source that already matches the canonical profile resolves to
    // PACKET_COPY, while a mismatched source resolves to REJECT (copy-only).
    velox::media::SegmentExecutionRequest request;
    request.source = signature;
    request.target = signature;
    request.transform_required = false;
    request.source_window_keyframe_safe = true;
    request.legacy_required = false;

    auto decision = velox::media::resolveSegmentExecution(request);
    expect(decision.mode == SegmentExecutionMode::PacketCopy,
           "canonical-profile source resolves to packet copy");

    request.source.width = 1280;
    decision = velox::media::resolveSegmentExecution(request);
    expect(decision.mode == SegmentExecutionMode::Reject,
           "off-profile source is rejected against the canonical target (copy-only)");

    // A real on-profile source carries its own stream extradata (SPS/PPS).
    // The canonical target does not pin extradata, so a matching source must
    // still resolve to PACKET_COPY — otherwise the mixed renderer could never
    // stream-copy a segment.
    auto real_source = signature;
    real_source.extradata = {0x01, 0x64, 0x00, 0x1f};
    expect(velox::media::mediaSignaturesCompatible(real_source, signature),
           "canonical target is extradata-agnostic for matching sources");

    request.source = real_source;
    request.target = signature;
    decision = velox::media::resolveSegmentExecution(request);
    expect(decision.mode == SegmentExecutionMode::PacketCopy,
           "on-profile source with its own extradata still resolves to packet copy");

    return failures == 0 ? 0 : 1;
}
