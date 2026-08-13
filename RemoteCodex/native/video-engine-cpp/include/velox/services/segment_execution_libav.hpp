#pragma once
// segment_execution_libav.hpp — the single LibAV bridge that translates an
// AVStream into the value-only MediaSignature used by every execution
// decision. This is the ONLY place where AVCodecParameters / AVStream are
// mapped to MediaSignature; callers that need a compatibility check must go
// through mediaSignaturesCompatible() (segment_execution.hpp) rather than
// inventing a second, private predicate.
//
// This header is LibAV-aware by design: it is included only by
// segment_execution_libav.cpp and the LibAV-aware pipeline sources, both of
// which compile exclusively when VELOX_ENABLE_LIBAV=ON.
#ifndef VELOX_ENABLE_LIBAV
#error "segment_execution_libav.hpp requires -DVELOX_ENABLE_LIBAV=ON"
#endif

// The LibAV public headers must be included under C linkage (same wrapper
// convention as media_packet_components.hpp / media_probe.cpp).
extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
}

#include "velox/services/segment_execution.hpp"

namespace velox::media {

// Translates one AVStream into its canonical MediaSignature. A null stream
// (or one without codecpar) returns a default-constructed signature with
// kind Video and no fields populated. Video fields (width/height/pixel
// format/frame rate) and audio fields (sample rate/channels/layout) are
// filled per the stream's media kind; profile/level/extradata are captured
// for both.
MediaSignature mediaSignatureFromStream(const AVStream* stream);

} // namespace velox::media
