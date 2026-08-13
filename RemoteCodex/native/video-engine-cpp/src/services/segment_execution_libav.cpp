#include "velox/services/segment_execution_libav.hpp"

// The in-process LibAV bridge is built only when VELOX_ENABLE_LIBAV is ON.
// Without the flag this translation unit is empty: the header above errors
// out when included, so the empty TU simply keeps the libav-only symbol
// available to nothing in a non-LibAV build (the packet pipeline compiles
// to its fail-closed stub and never references mediaSignatureFromStream).
#ifdef VELOX_ENABLE_LIBAV

extern "C" {
#include <libavutil/channel_layout.h>
#include <libavutil/version.h>
}

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
        signature.frame_rate_num = stream->avg_frame_rate.num;
        signature.frame_rate_den = stream->avg_frame_rate.den;
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
    return signature;
}

} // namespace velox::media

#endif // VELOX_ENABLE_LIBAV
