#include "velox/core/canonical_video_profile.hpp"

namespace velox::core {

media::MediaSignature mediaSignatureFromCanonicalProfile(
    const CanonicalVideoProfile& profile) {
    media::MediaSignature signature;
    signature.kind = media::MediaKind::Video;
    signature.codec_id = profile.codec_id;
    signature.profile = profile.profile;
    signature.level = profile.level;
    signature.width = profile.width;
    signature.height = profile.height;
    signature.pixel_format = profile.pixel_format;
    signature.frame_rate_num = profile.fps_num;
    signature.frame_rate_den = profile.fps_den;
    // extradata stays empty by design (see header): the canonical identity
    // does not pin bitstream-level extradata.
    return signature;
}

const CanonicalVideoProfile& canonicalVideoProfileV1() {
    static const CanonicalVideoProfile profile{
        .codec = "libx264",
        .codec_id = 27,
        .width = 1920,
        .height = 1080,
        .fps_num = 24,
        .fps_den = 1,
        .pixel_format = 0,
        .profile = 100,
        .level = 40,
        .gop_size = 48,
        .max_b_frames = 0,
        .preset = "medium",
        .crf = 23,
        .version = 1,
    };
    return profile;
}

} // namespace velox::core
