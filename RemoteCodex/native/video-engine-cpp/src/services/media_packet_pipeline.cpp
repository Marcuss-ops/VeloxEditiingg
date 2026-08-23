#include "velox/services/media_packet_pipeline.hpp"

#ifdef VELOX_ENABLE_LIBAV

#include "media_packet_pipeline_internal.hpp"

namespace velox::media {

bool MediaPacketPipeline::muxCopyOnly(
    const CopyOnlyMuxRequest& request,
    CopyOnlyMuxResult* result) {
    return runCopyOnlyMux(request, result);
}

bool muxCopyOnly(const CopyOnlyMuxRequest& request, CopyOnlyMuxResult* result) {
    return MediaPacketPipeline::muxCopyOnly(request, result);
}

} // namespace velox::media

#else

namespace velox::media {

bool MediaPacketPipeline::muxCopyOnly(
    const CopyOnlyMuxRequest& request,
    CopyOnlyMuxResult* result) {
    (void)request;
    if (result != nullptr) {
        *result = CopyOnlyMuxResult{};
        result->error =
            "VELOX_ENABLE_LIBAV=OFF: in-process packet mux requires "
            "libavformat/libavcodec/libavutil; rebuild with "
            "-DVELOX_ENABLE_LIBAV=ON";
    }
    return false;
}

bool muxCopyOnly(const CopyOnlyMuxRequest& request, CopyOnlyMuxResult* result) {
    return MediaPacketPipeline::muxCopyOnly(request, result);
}

} // namespace velox::media

#endif // VELOX_ENABLE_LIBAV
