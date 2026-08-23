#include "media_utils_internal.hpp"

namespace velox::media::detail {

void canvasDims(const SceneSegmentParams& params, int& width, int& height, int& fps) {
    width = params.width > 0 ? params.width : 1920;
    height = params.height > 0 ? params.height : 1080;
    fps = params.fps > 0 ? params.fps : 24;
}

std::string scaleFilterString(
    const std::string& scale_mode,
    const std::string& size,
    const std::string& resolution) {
    std::string filter;
    if (scale_mode == "contain") {
        filter = "scale=" + size + ":force_original_aspect_ratio=decrease,pad=" +
            size + ":(ow-iw)/2:(oh-ih)/2,format=yuv420p";
    } else if (scale_mode == "stretch") {
        filter = "scale=" + size + ",format=yuv420p";
    } else {
        filter = "scale=" + size + ":force_original_aspect_ratio=increase,crop=" +
            size + ",format=yuv420p";
    }
    (void)resolution;
    return withDecodeTelemetry(filter);
}

} // namespace velox::media::detail
