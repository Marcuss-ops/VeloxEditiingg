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
    expect(decision.mode == SegmentExecutionMode::NativeTranscode,
           "profile mismatch uses native transcode");
    expect(decision.reason == "media signature mismatch: width",
           "profile mismatch identifies the first mismatched field");

    request.source = target;
    request.transform_required = true;
    decision = velox::media::resolveSegmentExecution(request);
    expect(decision.mode == SegmentExecutionMode::NativeTranscode,
           "transform uses native transcode");

    request.transform_required = false;
    request.source_window_keyframe_safe = false;
    decision = velox::media::resolveSegmentExecution(request);
    expect(decision.mode == SegmentExecutionMode::NativeTranscode,
           "non-keyframe trim cannot use packet copy");

    request.source_window_keyframe_safe = true;
    request.legacy_required = true;
    decision = velox::media::resolveSegmentExecution(request);
    expect(decision.mode == SegmentExecutionMode::LegacyFallback,
           "unsupported feature uses explicit legacy fallback");

    expect(std::string(velox::media::segmentExecutionModeName(SegmentExecutionMode::PacketCopy)) ==
               "packet_copy",
           "packet-copy mode has stable wire name");

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

    return failures == 0 ? 0 : 1;
}
