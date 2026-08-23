#include "velox/services/media_utils.hpp"

#include <cmath>

namespace velox::media {
namespace {

std::string finalAudioTransportReason(const FinalAudioMetadata& metadata) {
    if (!metadata.metadata_verified || !metadata.duration_verified ||
        !metadata.start_time_verified) {
        return "audio_metadata_unverified";
    }
    if (!metadata.extradata_verified || !metadata.container_verified) {
        return "audio_transport_unverified";
    }
    if (metadata.codec != "aac") return "audio_codec_not_aac";
    return {};
}

} // namespace

FinalAudioDecision resolveFinalAudioMode(
    const FinalAudioMetadata& metadata,
    bool is_final_mix,
    double expected_duration_seconds,
    double volume,
    double start_offset) {
    FinalAudioDecision decision;
    decision.metadata = metadata;
    if (!is_final_mix) {
        decision.reason = "not_final_mix";
        return decision;
    }
    if (volume != 1.0 || start_offset != 0.0) {
        decision.reason = "final_audio_filter_required";
        return decision;
    }
    const std::string transport_reason = finalAudioTransportReason(metadata);
    if (!transport_reason.empty()) {
        decision.reason = transport_reason;
        return decision;
    }
    if (expected_duration_seconds <= 0.0 ||
        std::abs(metadata.duration_seconds - expected_duration_seconds) > 0.25) {
        decision.reason = "audio_duration_mismatch";
        return decision;
    }
    if (std::abs(metadata.start_time_seconds) > 0.05) {
        decision.reason = "audio_start_time_mismatch";
        return decision;
    }
    decision.mode = FinalAudioMode::Copy;
    decision.reason = "verified_final_mix";
    return decision;
}

FinalAudioDecision resolveFinalAudioModePacket(
    const FinalAudioMetadata& metadata,
    bool is_final_mix,
    double expected_duration_seconds) {
    FinalAudioDecision decision;
    decision.metadata = metadata;
    if (!is_final_mix) {
        decision.reason = "not_final_mix";
        return decision;
    }
    const std::string transport_reason = finalAudioTransportReason(metadata);
    if (!transport_reason.empty()) {
        decision.reason = transport_reason;
        return decision;
    }
    if (expected_duration_seconds <= 0.0 ||
        metadata.duration_seconds + 0.05 < expected_duration_seconds) {
        decision.reason = "audio_duration_mismatch";
        return decision;
    }
    decision.mode = FinalAudioMode::Copy;
    decision.reason = "verified_final_mix";
    return decision;
}

const char* finalAudioModeName(FinalAudioMode mode) {
    return mode == FinalAudioMode::Copy ? "COPY" : "ENCODE";
}

} // namespace velox::media
