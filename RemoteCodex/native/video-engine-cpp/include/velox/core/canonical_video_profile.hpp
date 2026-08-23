#pragma once

#include <cstdint>
#include <string>

#include "velox/services/segment_execution.hpp"

namespace velox::core {

// The canonical output identity of the mixed renderer. A segment transcoded
// by the native FramePipeline must be re-encoded in exactly this form so the
// packet mux can concatenate it with packet-copied ranges. This is stricter
// than MediaSignature: on top of the stream-identity fields (codec, dims,
// fps, pixel format, profile/level) it pins the encoder knobs (GOP,
// B-frames, CRF, preset) plus a version, so two "H264 1080p30" outputs
// cannot silently diverge inside one render.
struct CanonicalVideoProfile {
    // Encoder name (human/debug); "libx264" is the canonical encoder.
    std::string codec{"libx264"};
    // Stream codec identity (AVCodecID). 27 == AV_CODEC_ID_H264.
    int codec_id{27};

    int width{1920};
    int height{1080};

    int fps_num{24};
    int fps_den{1};

    // LibAV pixel format. 0 == AV_PIX_FMT_YUV420P.
    int pixel_format{0};
    // LibAV codec profile. 100 == FF_PROFILE_H264_HIGH.
    int profile{100};
    // LibAV codec level. 40 == H.264 level 4.0.
    int level{40};

    int gop_size{48};
    int max_b_frames{0};

    std::string preset{"medium"};
    int crf{23};

    // Canonical-profile version. Any change to the fields above that alters
    // the produced bitstream must bump this so derived/cache keys change.
    int version{1};
};

// The stream-level view of a canonical profile: the value-only MediaSignature
// that mediaSignaturesCompatible() compares. extradata is intentionally left
// empty — the canonical profile pins the stream identity, not the
// bitstream-level SPS/PPS that the encoder emits at transcode time.
media::MediaSignature mediaSignatureFromCanonicalProfile(
    const CanonicalVideoProfile& profile);

// The canonical V1 mixed-renderer profile: H.264, 1920x1080, 24 fps,
// yuv420p, High, GOP 48, B-frames 0, CRF 23.
const CanonicalVideoProfile& canonicalVideoProfileV1();

} // namespace velox::core
