#include "velox/services/segment_execution.hpp"

#include <iostream>
#include <string>

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
    signature.codec_id =  h264CodecId;
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

} // namespace

int main() {
    using velox::media::SegmentExecutionMode;

    const auto target = videoSignature();
    auto request = velox::media::SegmentExecutionRequest{
        .source = target,
        .target = target,
        .transform_required = false,
        .source_window_keyframe_safe = true,
        .legacy_required = false,
    };

    auto decision = velox::media::resolveSegmentExecution(request);
    expect(decision.mode == SegmentExecutionMode::PacketCopy,
           "matching keyframe-safe segment uses packet copy");
    expect(decision.reason == "source matches canonical output profile",
           "packet-copy reason is deterministic");

    request.source.width = 1280;
    decision = velox::media::resolveSegmentExecution(request);
    expect(decision.mode == SegmentExecutionMode::Reject,
           "profile mismatch is rejected (copy-only)");
    expect(decision.reason == "media signature mismatch: width",
           "profile mismatch identifies the first mismatched field");

    // Codec, resolution, pixel format, frame rate, profile and level
    // mismatches each resolve to Reject with the exact first-mismatch field.
    // This pins the copy-only contract per field: any single incompatible
    // dimension rejects the segment, never a re-encode.
    request.source = target;
    request.source.codec_id = 173; // H.265
    decision = velox::media::resolveSegmentExecution(request);
    expect(decision.mode == SegmentExecutionMode::Reject,
           "codec mismatch is rejected (copy-only)");
    expect(decision.reason == "media signature mismatch: codec_id",
           "codec mismatch identifies codec_id");

    request.source = target;
    request.source.height = 720;
    decision = velox::media::resolveSegmentExecution(request);
    expect(decision.mode == SegmentExecutionMode::Reject,
           "resolution mismatch is rejected (copy-only)");
    expect(decision.reason == "media signature mismatch: height",
           "resolution mismatch identifies height");

    request.source = target;
    request.source.pixel_format = 1;
    decision = velox::media::resolveSegmentExecution(request);
    expect(decision.mode == SegmentExecutionMode::Reject,
           "pixel format mismatch is rejected (copy-only)");
    expect(decision.reason == "media signature mismatch: pixel_format",
           "pixel format mismatch identifies pixel_format");

    request.source = target;
    request.source.frame_rate_num = 25;
    decision = velox::media::resolveSegmentExecution(request);
    expect(decision.mode == SegmentExecutionMode::Reject,
           "frame rate mismatch is rejected (copy-only)");
    expect(decision.reason == "media signature mismatch: frame_rate",
           "frame rate mismatch identifies frame_rate");

    request.source = target;
    request.source.profile = 66; // FF_PROFILE_H264_BASELINE
    decision = velox::media::resolveSegmentExecution(request);
    expect(decision.mode == SegmentExecutionMode::Reject,
           "profile mismatch is rejected (copy-only)");
    expect(decision.reason == "media signature mismatch: profile",
           "profile mismatch identifies profile");

    request.source = target;
    request.source.level = 31;
    decision = velox::media::resolveSegmentExecution(request);
    expect(decision.mode == SegmentExecutionMode::Reject,
           "level mismatch is rejected (copy-only)");
    expect(decision.reason == "media signature mismatch: level",
           "level mismatch identifies level");

    request.source = target;
    request.transform_required = true;
    decision = velox::media::resolveSegmentExecution(request);
    expect(decision.mode == SegmentExecutionMode::Reject,
           "transform is rejected (copy-only)");
    expect(decision.reason == "segment requires a media transform",
           "transform rejection keeps the exact reason");

    request.transform_required = false;
    request.source_window_keyframe_safe = false;
    decision = velox::media::resolveSegmentExecution(request);
    expect(decision.mode == SegmentExecutionMode::Reject,
           "non-keyframe trim is rejected (copy-only)");
    expect(decision.reason == "source window is not keyframe-safe for packet copy",
           "non-keyframe rejection keeps the exact reason");

    request.source_window_keyframe_safe = true;
    request.legacy_required = true;
    decision = velox::media::resolveSegmentExecution(request);
    expect(decision.mode == SegmentExecutionMode::Reject,
           "legacy-required feature is rejected (copy-only)");

    expect(std::string(velox::media::segmentExecutionModeName(SegmentExecutionMode::PacketCopy)) ==
               "packet_copy",
           "packet-copy mode has stable wire name");
    expect(std::string(velox::media::segmentExecutionModeName(SegmentExecutionMode::Reject)) ==
               "reject",
           "reject mode has stable wire name");

    auto audio_source = velox::media::MediaSignature{};
    audio_source.kind = velox::media::MediaKind::Audio;
    audio_source.codec_id = 86018;
    audio_source.pixel_format = 8;
    audio_source.sample_rate = 48000;
    audio_source.channels = 2;
    audio_source.channel_layout = "stereo";
    auto audio_target = audio_source;
    expect(velox::media::mediaSignaturesCompatible(audio_source, audio_target),
           "matching audio signatures are compatible");
    audio_target.pixel_format = 9;
    expect(!velox::media::mediaSignaturesCompatible(audio_source, audio_target),
           "audio sample format is part of the canonical signature");

    // profile/level are pin-optional on the target: a target that leaves them
    // at their defaults accepts any source profile/level, so the legacy
    // copy-only guard can pin only codec/pix_fmt/dims/fps.
    auto unprofiled_target = videoSignature();
    unprofiled_target.profile = -1;
    unprofiled_target.level = -1;
    auto high_profile_source = videoSignature();
    high_profile_source.profile = 100;
    high_profile_source.level = 40;
    expect(velox::media::mediaSignaturesCompatible(high_profile_source, unprofiled_target),
           "unprofiled target accepts any source profile (pin-optional)");

    // A target that pins profile/level stays strict.
    auto pinned_target = videoSignature();
    pinned_target.profile = 100;
    pinned_target.level = 40;
    auto baseline_source = videoSignature();
    baseline_source.profile = 66; // FF_PROFILE_H264_BASELINE
    expect(!velox::media::mediaSignaturesCompatible(baseline_source, pinned_target),
           "pinned target rejects a mismatched source profile");

    return failures == 0 ? 0 : 1;
}
