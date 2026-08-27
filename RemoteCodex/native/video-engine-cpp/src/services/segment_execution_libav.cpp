#include "velox/services/segment_execution_libav.hpp"

// The in-process LibAV bridge is built only when VELOX_ENABLE_LIBAV is ON.
// Without the flag this translation unit is empty: the header above errors
// out when included, so the empty TU simply keeps the libav-only symbol
// available to nothing in a non-LibAV build (the packet pipeline compiles
// to its fail-closed stub and never references mediaSignatureFromStream).
#ifdef VELOX_ENABLE_LIBAV

// InputSession/Demuxer (media_packet_components.hpp) provide the in-process
// open/seek/keyframe probe; their definitions live in
// media_packet_muxer.cpp, which is linked alongside this TU everywhere.
#include "velox/services/media_packet_components.hpp"

extern "C" {
#include <libavutil/channel_layout.h>
#include <libavutil/version.h>
}

namespace fs = std::filesystem;

namespace velox::media {

MediaSignature mediaSignatureFromStream(const AVStream* stream) {
    MediaSignature signature;
    if (stream == nullptr || stream->codecpar == nullptr) {
        return signature;
    }

    const AVCodecParameters* parameters = stream->codecpar;
    signature.kind = parameters->codec_type == AVMEDIA_TYPE_AUDIO
        ? MediaKind::Audio
        : MediaKind::Video;
    signature.codec_id = parameters->codec_id;
    signature.profile = parameters->profile;
    signature.level = parameters->level;
    if (parameters->extradata != nullptr && parameters->extradata_size > 0) {
        signature.extradata.assign(
            parameters->extradata,
            parameters->extradata + parameters->extradata_size);
    }

    if (signature.kind == MediaKind::Video) {
        signature.width = parameters->width;
        signature.height = parameters->height;
        signature.pixel_format = parameters->format;
        // Matroska and some MP4 producers leave avg_frame_rate unset while
        // still publishing the authoritative stream rate in r_frame_rate.
        // Never turn that valid source into 0/1: packet-copy compatibility
        // must use the same fallback as the media probe path.
        AVRational frame_rate = stream->avg_frame_rate;
        if (frame_rate.num <= 0 || frame_rate.den <= 0) {
            frame_rate = stream->r_frame_rate;
        }
        signature.frame_rate_num = frame_rate.num;
        signature.frame_rate_den = frame_rate.den;
    } else {
        signature.sample_rate = parameters->sample_rate;
        signature.pixel_format = parameters->format;
#if LIBAVUTIL_VERSION_MAJOR >= 57
        signature.channels = parameters->ch_layout.nb_channels;
        char layout[256]{};
        if (av_channel_layout_describe(&parameters->ch_layout, layout, sizeof(layout)) >= 0) {
            signature.channel_layout = layout;
        }
#else
        signature.channels = parameters->channels;
        signature.channel_layout = std::to_string(parameters->channel_layout);
#endif
    }
    signature.time_base_num = stream->time_base.num;
    signature.time_base_den = stream->time_base.den;
    return signature;
}

bool probeSegmentForExecution(const fs::path& path,
                              int64_t source_in_us,
                              MediaKind kind,
                              SegmentProbe* out,
                              std::string* error) {
    if (out == nullptr) {
        return false;
    }
    *out = SegmentProbe{};
    std::string local_error;
    std::string& err = error != nullptr ? *error : local_error;

    packet::InputSession session;
    if (!session.open(path, err)) {
        return false;
    }
    packet::Demuxer& demuxer = session.demuxer();
    const int stream_index = demuxer.firstStream(
        kind == MediaKind::Audio ? AVMEDIA_TYPE_AUDIO : AVMEDIA_TYPE_VIDEO);
    if (stream_index < 0) {
        err = "media stream missing from " + path.string();
        return false;
    }
    const AVStream* stream = demuxer.stream(stream_index);
    out->signature = mediaSignatureFromStream(stream);

    if (kind == MediaKind::Video) {
        std::string keyframe_error;
        // A non-keyframe trim is a valid, recoverable outcome (route to
        // transcode), not a probe failure, so the keyframe error is
        // deliberately not propagated.
        out->source_window_keyframe_safe =
            session.sourceWindowStartsOnKeyframe(stream_index, source_in_us, keyframe_error);
    }
    return true;
}

} // namespace velox::media

#endif // VELOX_ENABLE_LIBAV
