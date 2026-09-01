#include "velox/services/media_utils.hpp"

#include <cmath>
#include <sstream>
#include <system_error>

namespace velox::media {
namespace {

std::string quoteDiagnosticValue(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 2);
    for (const char c : value) {
        if (c == '"' || c == '\\') out.push_back('\\');
        out.push_back(c);
    }
    return out;
}

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

std::string describeFinalAudioProbe(
    const std::filesystem::path& audioPath,
    const FinalAudioMetadata& metadata) {
    std::error_code ec;
    const bool exists = !audioPath.empty() && std::filesystem::exists(audioPath, ec);
    const auto size = exists ? std::filesystem::file_size(audioPath, ec) : 0;
    std::string reason;
    if (audioPath.empty()) {
        reason = "binding_missing";
    } else if (!exists) {
        reason = "file_not_found";
    } else if (metadata.codec.empty()) {
        reason = "no_audio_stream_or_probe_failed";
    } else if (!metadata.metadata_verified) {
        reason = "metadata_unverified";
    } else if (!metadata.extradata_verified || !metadata.container_verified) {
        reason = "audio_transport_unverified";
    } else if (metadata.codec != "aac") {
        reason = "unsupported_codec";
    } else {
        reason = "valid";
    }

    std::ostringstream out;
    out << "reason=" << reason
        << " path=\"" << quoteDiagnosticValue(audioPath.string()) << "\""
        << " exists=" << (exists ? "true" : "false")
        << " size_bytes=" << size
        << " format=\"" << quoteDiagnosticValue(metadata.format_name) << "\""
        << " codec=\"" << quoteDiagnosticValue(metadata.codec) << "\""
        << " sample_rate=" << metadata.sample_rate
        << " channels=" << metadata.channels
        << " channel_layout=\"" << quoteDiagnosticValue(metadata.channel_layout) << "\""
        << " duration_seconds=" << metadata.duration_seconds
        << " duration_verified=" << (metadata.duration_verified ? "true" : "false")
        << " start_time_seconds=" << metadata.start_time_seconds
        << " start_time_verified=" << (metadata.start_time_verified ? "true" : "false")
        << " extradata_verified=" << (metadata.extradata_verified ? "true" : "false")
        << " container_verified=" << (metadata.container_verified ? "true" : "false");
    return out.str();
}

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
