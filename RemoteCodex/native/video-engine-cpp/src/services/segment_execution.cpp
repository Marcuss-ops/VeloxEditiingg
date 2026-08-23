#include "velox/services/segment_execution.hpp"

#include <sstream>

namespace velox::media {
namespace {

void mismatch(std::string* reason, const char* field) {
    if (reason != nullptr) {
        *reason = std::string("media signature mismatch: ") + field;
    }
}

} // namespace

bool mediaSignaturesCompatible(const MediaSignature& source,
                               const MediaSignature& target,
                               std::string* reason) {
    if (source.kind != target.kind) {
        mismatch(reason, "kind");
        return false;
    }
    if (source.codec_id != target.codec_id) {
        mismatch(reason, "codec_id");
        return false;
    }
    // profile/level are pin-optional on the TARGET: a target that leaves a
    // field at its default (< 0) means "any source profile/level is
    // acceptable". The canonical profile pins both explicitly, so its
    // strictness is unchanged. (Same don't-care pattern as extradata below.)
    if (target.profile >= 0 && source.profile != target.profile) {
        mismatch(reason, "profile");
        return false;
    }
    if (target.level >= 0 && source.level != target.level) {
        mismatch(reason, "level");
        return false;
    }
    // extradata is a stream-level detail (H.264 SPS/PPS), not part of the
    // canonical output identity. A canonical-profile target carries an empty
    // extradata to mean "don't care"; only compare when BOTH sides actually
    // pin it (the packet mux compares two concrete streams, so its stricter
    // behavior is unchanged).
    if (!source.extradata.empty() && !target.extradata.empty() &&
        source.extradata != target.extradata) {
        mismatch(reason, "extradata");
        return false;
    }
    if (source.kind == MediaKind::Video) {
        if (source.width != target.width) {
            mismatch(reason, "width");
            return false;
        }
        if (source.height != target.height) {
            mismatch(reason, "height");
            return false;
        }
        if (source.pixel_format != target.pixel_format) {
            mismatch(reason, "pixel_format");
            return false;
        }
        if (source.frame_rate_num != target.frame_rate_num ||
            source.frame_rate_den != target.frame_rate_den) {
            mismatch(reason, "frame_rate");
            return false;
        }
        if (target.time_base_num > 0 &&
            (source.time_base_num != target.time_base_num ||
             source.time_base_den != target.time_base_den)) {
            mismatch(reason, "time_base");
            return false;
        }
    } else {
        if (source.pixel_format != target.pixel_format) {
            mismatch(reason, "sample_format");
            return false;
        }
        if (source.sample_rate != target.sample_rate) {
            mismatch(reason, "sample_rate");
            return false;
        }
        if (source.channels != target.channels) {
            mismatch(reason, "channels");
            return false;
        }
        if (source.channel_layout != target.channel_layout) {
            mismatch(reason, "channel_layout");
            return false;
        }
        if (target.time_base_num > 0 &&
            (source.time_base_num != target.time_base_num ||
             source.time_base_den != target.time_base_den)) {
            mismatch(reason, "time_base");
            return false;
        }
    }
    return true;
}

const char* segmentExecutionModeName(SegmentExecutionMode mode) {
    switch (mode) {
    case SegmentExecutionMode::PacketCopy:
        return "packet_copy";
    case SegmentExecutionMode::Reject:
        return "reject";
    }
    return "reject";
}

SegmentExecutionDecision resolveSegmentExecution(const SegmentExecutionRequest& request) {
    SegmentExecutionDecision decision;
    decision.keyframe_safe = request.source_window_keyframe_safe;

    // Fail-closed: the assembly path is copy-only. ANY condition that would
    // need re-encoding (legacy feature, media transform, non-keyframe-safe
    // trim, incompatible media signature) resolves to Reject with an exact
    // reason, never to a transcode. Upstream (Chronon/RenderingGen) must
    // produce segments that are already canonical and keyframe-safe.
    if (request.legacy_required) {
        decision.mode = SegmentExecutionMode::Reject;
        decision.reason = "feature requires legacy renderer";
        return decision;
    }
    if (request.transform_required) {
        decision.mode = SegmentExecutionMode::Reject;
        decision.reason = "segment requires a media transform";
        return decision;
    }
    if (!request.source_window_keyframe_safe) {
        decision.mode = SegmentExecutionMode::Reject;
        decision.reason = "source window is not keyframe-safe for packet copy";
        return decision;
    }

    std::string compatibility_reason;
    if (!mediaSignaturesCompatible(request.source, request.target, &compatibility_reason)) {
        decision.mode = SegmentExecutionMode::Reject;
        decision.reason = compatibility_reason;
        return decision;
    }

    decision.mode = SegmentExecutionMode::PacketCopy;
    decision.reason = "source matches canonical output profile";
    return decision;
}

} // namespace velox::media
