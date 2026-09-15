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
        .profile_id = "VELOX_ASSEMBLY_READY_V1",
        .stream_profile_id = "VELOX_ASSEMBLY_READY_V1",
        .layout = Mp4Layout::Progressive,
        .container = "mp4",
        .stream_codec = "h264",
        .pixel_format_name = "yuv420p",
        .codec_profile_name = "high",
        .codec_level_name = "4.0",
        .time_base_num = 1,
        .time_base_den = 90000,
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

const CanonicalVideoProfile& canonicalVideoProfileFmp4StreamV1() {
    static const CanonicalVideoProfile profile{
        .profile_id = "velox-h264-fmp4-stream-v1",
        .stream_profile_id = "VELOX_ASSEMBLY_READY_V1",
        .layout = Mp4Layout::Fragmented,
        .container = "mp4",
        .stream_codec = "h264",
        .pixel_format_name = "yuv420p",
        .codec_profile_name = "high",
        .codec_level_name = "4.0",
        .time_base_num = 1,
        .time_base_den = 90000,
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

std::optional<CanonicalVideoProfile> resolveCanonicalVideoProfile(
    const std::string& profile_id, std::string& error) {
    if (profile_id == canonicalVideoProfileV1().profile_id) {
        return canonicalVideoProfileV1();
    }
    if (profile_id == canonicalVideoProfileFmp4StreamV1().profile_id) {
        return canonicalVideoProfileFmp4StreamV1();
    }
    error = "canonical video profile \"" + profile_id + "\" is not registered";
    return std::nullopt;
}

} // namespace velox::core
